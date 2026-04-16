package postgres

import (
	"fmt"
	"os"
)

// Config holds all parameters needed to open a PostgreSQL connection pool.
type Config struct {
	// DSN is the full connection string, e.g.:
	// "postgres://user:pass@host:5432/dbname?sslmode=require"
	// It is the authoritative connection parameter; individual fields are
	// provided for convenience and assembled into a DSN by DSNString().
	DSN string

	// Individual fields (used when DSN is not provided).
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string

	// Pool configuration.
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime string // duration string, e.g. "1h"
	MaxConnIdleTime string // duration string, e.g. "30m"
}

// FromEnv constructs a Config by reading the following environment variables:
//
//	POSTGRES_DSN               full DSN (takes precedence over individual fields)
//	POSTGRES_HOST              default "localhost"
//	POSTGRES_PORT              default "5432"
//	POSTGRES_USER              required when DSN is not set
//	POSTGRES_PASSWORD          required when DSN is not set
//	POSTGRES_DB                required when DSN is not set
//	POSTGRES_SSL_MODE          default "require"
//	POSTGRES_MAX_CONNS         int32, default 20
//	POSTGRES_MIN_CONNS         int32, default 2
//	POSTGRES_MAX_CONN_LIFETIME duration string, default "1h"
//	POSTGRES_MAX_CONN_IDLE     duration string, default "30m"
func FromEnv() (Config, error) {
	cfg := Config{
		DSN:             os.Getenv("POSTGRES_DSN"),
		Host:            envOr("POSTGRES_HOST", "localhost"),
		Port:            envOr("POSTGRES_PORT", "5432"),
		User:            os.Getenv("POSTGRES_USER"),
		Password:        os.Getenv("POSTGRES_PASSWORD"),
		Database:        os.Getenv("POSTGRES_DB"),
		SSLMode:         envOr("POSTGRES_SSL_MODE", "require"),
		MaxConns:        int32(parseIntEnv("POSTGRES_MAX_CONNS", 20)),
		MinConns:        int32(parseIntEnv("POSTGRES_MIN_CONNS", 2)),
		MaxConnLifetime: envOr("POSTGRES_MAX_CONN_LIFETIME", "1h"),
		MaxConnIdleTime: envOr("POSTGRES_MAX_CONN_IDLE", "30m"),
	}

	// Validate: either DSN or individual fields must be present.
	if cfg.DSN == "" {
		if cfg.User == "" || cfg.Password == "" || cfg.Database == "" {
			return Config{}, fmt.Errorf("postgres config: set POSTGRES_DSN or POSTGRES_USER/PASSWORD/DB")
		}
	}
	return cfg, nil
}

// DSNString assembles the individual fields into a DSN when cfg.DSN is empty.
func (c Config) DSNString() string {
	if c.DSN != "" {
		return c.DSN
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode,
	)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseIntEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}
