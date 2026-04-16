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

	kafkapkg "notification-service/internal/platform/kafka"
	postgrespkg "notification-service/internal/platform/postgres"
	redispkg "notification-service/internal/platform/redis"
	s3pkg "notification-service/internal/platform/s3"
	"notification-service/internal/template"
	"notification-service/pkg/telemetry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	logger, err := buildLogger()
	if err != nil {
		return fmt.Errorf("logger: %w", err)
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

	pgCfg, err := postgrespkg.FromEnv()
	if err != nil {
		return fmt.Errorf("postgres config: %w", err)
	}
	pgPool, err := postgrespkg.NewPool(pgCfg)
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	defer pgPool.Close()

	s3Cfg, err := s3pkg.FromEnv()
	if err != nil {
		return fmt.Errorf("s3 config: %w", err)
	}
	s3Client, err := s3pkg.NewClient(s3Cfg)
	if err != nil {
		return fmt.Errorf("s3 client: %w", err)
	}

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

	repo := template.NewPostgresRepository(pgPool)
	cache := template.NewRedisCache(redisClient)
	bodyStore := template.NewS3BodyStore(s3Client, s3Cfg.Bucket)
	eventPub := template.NewKafkaEventPublisher(kafkaWriter)
	svc := template.NewService(repo, cache, bodyStore, eventPub, logger)

	// ── Startup cache warm (must complete before accepting traffic) ───────────
	// This is the gate that ensures the ingestion service never reads a stale
	// or missing cache entry on startup. The context has a generous timeout to
	// accommodate large template catalogs.

	warmCtx, warmCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := svc.WarmCache(warmCtx); err != nil {
		warmCancel()
		return fmt.Errorf("cache warm: %w", err)
	}
	warmCancel()
	logger.Info("startup cache warm complete")

	// ── HTTP routing ──────────────────────────────────────────────────────────

	mux := http.NewServeMux()

	h := template.NewHandler(svc, logger)
	h.RegisterRoutes(mux)

	mux.Handle("/metrics", telemetry.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// ── HTTP server ───────────────────────────────────────────────────────────

	addr := envOr("HTTP_ADDR", ":8081")
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("template service starting", zap.String("addr", addr))
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
		return fmt.Errorf("shutdown: %w", err)
	}

	logger.Info("template service stopped")
	return nil
}

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
