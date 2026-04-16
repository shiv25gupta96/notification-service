package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"notification-service/internal/ingestion"
	kafkapkg "notification-service/internal/platform/kafka"
	redispkg "notification-service/internal/platform/redis"
	"notification-service/pkg/telemetry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is the real entry point. Separating it from main lets us return errors
// cleanly and makes the startup sequence unit-testable.
func run() error {
	logger, err := buildLogger()
	if err != nil {
		return fmt.Errorf("logger init: %w", err)
	}
	defer logger.Sync() //nolint:errcheck

	// ── Infrastructure ────────────────────────────────────────────────────────

	redisCfg, err := redispkg.FromEnv()
	if err != nil {
		return fmt.Errorf("redis config: %w", err)
	}
	redisClient, err := redispkg.NewClient(redisCfg)
	if err != nil {
		return fmt.Errorf("redis client: %w", err)
	}
	defer redisClient.Close()

	kafkaCfg, err := kafkapkg.ProducerFromEnv()
	if err != nil {
		return fmt.Errorf("kafka config: %w", err)
	}
	kafkaWriter, err := kafkapkg.NewWriter(kafkaCfg)
	if err != nil {
		return fmt.Errorf("kafka writer: %w", err)
	}
	defer kafkaWriter.Close()

	// ── Domain wiring ─────────────────────────────────────────────────────────

	idempotencyStore := ingestion.NewRedisIdempotencyStore(redisClient, 0)
	templateResolver := ingestion.NewRedisTemplateResolver(redisClient)
	validator := ingestion.NewValidator(templateResolver)
	publisher := ingestion.NewPublisher(kafkaWriter)
	handler := ingestion.NewHandler(idempotencyStore, validator, publisher, logger)

	// ── HTTP routing ──────────────────────────────────────────────────────────

	mux := http.NewServeMux()

	// POST /v1/notifications — ingestion endpoint.
	notifHandler := ingestion.Chain(
		ingestion.MethodGate(http.MethodPost, handler),
		ingestion.RequestID,
		ingestion.AccessLog(logger),
		ingestion.Timeout(90*time.Millisecond), // 100 ms SLA with 10 ms headroom
	)
	mux.Handle("/v1/notifications", notifHandler)

	// GET /metrics — Prometheus scrape endpoint.
	mux.Handle("/metrics", telemetry.Handler())

	// GET /healthz — liveness probe (returns 200 as long as the process runs).
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// ── HTTP server ───────────────────────────────────────────────────────────

	addr := envOr("HTTP_ADDR", ":8080")
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in background.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("ingestion service starting", zap.String("addr", addr))
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	case err := <-errCh:
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}

	logger.Info("ingestion service stopped")
	return nil
}

// buildLogger returns a production zap.Logger. The LOG_LEVEL environment
// variable ("debug", "info", "warn", "error") overrides the default level.
func buildLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		if err := cfg.Level.UnmarshalText([]byte(level)); err != nil {
			return nil, fmt.Errorf("invalid LOG_LEVEL %q: %w", level, err)
		}
	}
	return cfg.Build()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
