package redis

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every Redis connection parameter the application needs.
// Values are loaded from environment variables so that the binary is
// configuration-free at build time (twelve-factor app principle).
type Config struct {
	// Addrs is the list of Redis node addresses (host:port).
	// For a cluster supply all known nodes; for standalone supply one.
	Addrs []string

	Password string
	DB       int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// PoolSize is the maximum number of connections per node.
	PoolSize int
	// MinIdleConns keeps this many connections open even when idle,
	// amortising connection-establishment cost on burst traffic.
	MinIdleConns int

	TLSEnabled bool
}

// FromEnv constructs a Config by reading the following environment variables:
//
//	REDIS_ADDRS          comma-separated host:port list (required)
//	REDIS_PASSWORD       optional
//	REDIS_DB             integer, default 0
//	REDIS_DIAL_TIMEOUT   duration string, default "5s"
//	REDIS_READ_TIMEOUT   duration string, default "3s"
//	REDIS_WRITE_TIMEOUT  duration string, default "3s"
//	REDIS_POOL_SIZE      integer, default 100
//	REDIS_MIN_IDLE_CONNS integer, default 10
//	REDIS_TLS_ENABLED    "true" | "false", default "false"
func FromEnv() (Config, error) {
	raw := os.Getenv("REDIS_ADDRS")
	if raw == "" {
		return Config{}, fmt.Errorf("redis config: REDIS_ADDRS must not be empty")
	}

	cfg := Config{
		Addrs:        strings.Split(raw, ","),
		Password:     os.Getenv("REDIS_PASSWORD"),
		DialTimeout:  parseDurationEnv("REDIS_DIAL_TIMEOUT", 5*time.Second),
		ReadTimeout:  parseDurationEnv("REDIS_READ_TIMEOUT", 3*time.Second),
		WriteTimeout: parseDurationEnv("REDIS_WRITE_TIMEOUT", 3*time.Second),
		PoolSize:     parseIntEnv("REDIS_POOL_SIZE", 100),
		MinIdleConns: parseIntEnv("REDIS_MIN_IDLE_CONNS", 10),
		TLSEnabled:   os.Getenv("REDIS_TLS_ENABLED") == "true",
	}

	if db := os.Getenv("REDIS_DB"); db != "" {
		n, err := strconv.Atoi(db)
		if err != nil {
			return Config{}, fmt.Errorf("redis config: REDIS_DB must be an integer: %w", err)
		}
		cfg.DB = n
	}

	return cfg, nil
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func parseIntEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
