package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"go.uber.org/zap"

	kafkapkg "notification-service/internal/platform/kafka"
	postgrespkg "notification-service/internal/platform/postgres"
	redispkg "notification-service/internal/platform/redis"
	"notification-service/internal/scheduler"
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

	// PostgreSQL is retained for future use (e.g., delivery rule storage)
	// even though the scheduler no longer owns cron rules.
	pgCfg, err := postgrespkg.FromEnv()
	if err != nil {
		return fmt.Errorf("postgres config: %w", err)
	}
	pgPool, err := postgrespkg.NewPool(pgCfg)
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	defer pgPool.Close()

	kafkaCfg, err := kafkapkg.ProducerFromEnv()
	if err != nil {
		return fmt.Errorf("kafka producer config: %w", err)
	}
	kafkaWriter, err := kafkapkg.NewWriter(kafkaCfg)
	if err != nil {
		return fmt.Errorf("kafka writer: %w", err)
	}
	defer kafkaWriter.Close()

	// ── Domain wiring ─────────────────────────────────────────────────────────

	scheduleStore := scheduler.NewRedisScheduleStore(redisClient)

	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaReader := scheduler.NewKafkaReader(brokers)
	defer kafkaReader.Close()

	consumer := scheduler.NewConsumer(kafkaReader, scheduleStore, logger)
	poller := scheduler.NewPoller(scheduleStore, kafkaWriter, logger)

	// ── Concurrent component execution ────────────────────────────────────────

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx); err != nil {
			errCh <- fmt.Errorf("consumer: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := poller.Run(ctx); err != nil {
			errCh <- fmt.Errorf("poller: %w", err)
		}
	}()

	var runErr error
	select {
	case sig := <-quit:
		logger.Info("shutdown signal received", zap.String("signal", sig.String()))
	case err := <-errCh:
		logger.Error("component error — initiating shutdown", zap.Error(err))
		runErr = err
	}

	cancel()
	wg.Wait()

	logger.Info("scheduler service stopped")
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
