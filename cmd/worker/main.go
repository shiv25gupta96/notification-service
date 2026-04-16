package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.uber.org/zap"

	kafkapkg "notification-service/internal/platform/kafka"
	postgrespkg "notification-service/internal/platform/postgres"
	redispkg "notification-service/internal/platform/redis"
	"notification-service/internal/worker"
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

	// ── Domain wiring ─────────────────────────────────────────────────────────

	templateSvcURL := envOr("TEMPLATE_SERVICE_URL", "http://localhost:8081")
	templateEngine := worker.NewHTTPTemplateEngine(templateSvcURL)

	emailProvider := worker.NewDummyEmailProvider(logger)
	auditLog := worker.NewPostgresAuditLog(pgPool)

	cache := worker.NewRedisWorkerCache(redisClient)

	steps := []worker.Step{
		worker.NewIdempotencyStep(cache),
		worker.NewPreferenceStep(logger),
		worker.NewTemplateStep(templateEngine),
		worker.NewDeliveryStep(emailProvider, cache),
	}
	chain := worker.NewChain(steps, auditLog, logger)

	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaReader := worker.NewKafkaReader(brokers)
	defer kafkaReader.Close()

	// We also need a Kafka writer for the DLQ (future use); wire the producer
	// config now so it's available when DLQ support is added.
	kafkaCfg, err := kafkapkg.ProducerFromEnv()
	if err != nil {
		return fmt.Errorf("kafka producer config: %w", err)
	}
	kafkaWriter, err := kafkapkg.NewWriter(kafkaCfg)
	if err != nil {
		return fmt.Errorf("kafka writer: %w", err)
	}
	defer kafkaWriter.Close()

	consumer := worker.NewConsumer(kafkaReader, chain, logger)

	// ── Graceful shutdown ─────────────────────────────────────────────────────

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		if err := consumer.Run(ctx); err != nil {
			errCh <- err
		}
	}()

	var runErr error
	select {
	case sig := <-quit:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	case err := <-errCh:
		logger.Error("consumer error — initiating shutdown", zap.Error(err))
		runErr = err
	}

	cancel()
	logger.Info("worker stopped")
	return runErr
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
