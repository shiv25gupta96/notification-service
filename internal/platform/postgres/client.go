package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates a pgxpool.Pool from cfg and pings the database to verify
// connectivity. The caller is responsible for calling pool.Close() on shutdown.
func NewPool(cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSNString())
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns

	if cfg.MaxConnLifetime != "" {
		d, err := time.ParseDuration(cfg.MaxConnLifetime)
		if err != nil {
			return nil, fmt.Errorf("postgres: parse MaxConnLifetime %q: %w", cfg.MaxConnLifetime, err)
		}
		poolCfg.MaxConnLifetime = d
	}

	if cfg.MaxConnIdleTime != "" {
		d, err := time.ParseDuration(cfg.MaxConnIdleTime)
		if err != nil {
			return nil, fmt.Errorf("postgres: parse MaxConnIdleTime %q: %w", cfg.MaxConnIdleTime, err)
		}
		poolCfg.MaxConnIdleTime = d
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: open pool: %w", err)
	}

	// Ping exercises the pool by acquiring and releasing a connection.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping failed: %w", err)
	}
	return pool, nil
}
