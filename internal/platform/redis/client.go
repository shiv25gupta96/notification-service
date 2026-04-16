package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// NewClient builds a redis.UniversalClient from cfg.
//
// redis.NewUniversalClient selects the correct driver automatically:
//   - len(Addrs) == 1 → standalone Client
//   - len(Addrs) > 1  → ClusterClient
//
// A Ping is issued before returning to surface misconfiguration at startup
// rather than at first request.
func NewClient(cfg Config) (goredis.UniversalClient, error) {
	opts := &goredis.UniversalOptions{
		Addrs:        cfg.Addrs,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
	}
	if cfg.TLSEnabled {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	}

	client := goredis.NewUniversalClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping %v: %w", cfg.Addrs, err)
	}
	return client, nil
}
