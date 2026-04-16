package template

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const cacheKeyPrefix = "template:"

// Cache is the contract for reading and writing template metadata in Redis.
// The Template Service is the sole writer of these keys system-wide.
type Cache interface {
	// Set writes entry under "template:{entry.ID}", overwriting any existing
	// value. There is intentionally no TTL: expiry is controlled exclusively
	// by this service via Delete.
	Set(ctx context.Context, entry *CacheEntry) error

	// Delete removes the key for templateID. Called on deactivation.
	Delete(ctx context.Context, templateID string) error

	// SetBatch writes all entries in a single pipelined request. Used during
	// startup cache warming to minimise round-trips.
	SetBatch(ctx context.Context, entries []*CacheEntry) error
}

// RedisCache implements Cache using a Redis UniversalClient.
type RedisCache struct {
	client redis.UniversalClient
}

// NewRedisCache creates a cache backed by client.
func NewRedisCache(client redis.UniversalClient) *RedisCache {
	return &RedisCache{client: client}
}

// Set marshals entry to JSON and writes it to Redis with no expiry.
// An error here means the ingestion service will return 400 for this template
// until the next startup warm; callers must treat it as a hard failure.
func (c *RedisCache) Set(ctx context.Context, entry *CacheEntry) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("cache: marshal entry %q: %w", entry.ID, err)
	}
	if err := c.client.Set(ctx, cacheKeyPrefix+entry.ID, raw, 0).Err(); err != nil {
		return fmt.Errorf("cache: redis SET %q: %w", entry.ID, err)
	}
	return nil
}

// Delete removes the template cache entry for templateID.
func (c *RedisCache) Delete(ctx context.Context, templateID string) error {
	if err := c.client.Del(ctx, cacheKeyPrefix+templateID).Err(); err != nil {
		return fmt.Errorf("cache: redis DEL %q: %w", templateID, err)
	}
	return nil
}

// SetBatch uses a Redis pipeline to write all entries in a single round-trip.
// On pipeline execution error, partial writes may have occurred; the caller
// should log and continue — the next startup warm will reconcile the cache.
func (c *RedisCache) SetBatch(ctx context.Context, entries []*CacheEntry) error {
	if len(entries) == 0 {
		return nil
	}

	pipe := c.client.Pipeline()
	for _, entry := range entries {
		raw, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("cache: marshal entry %q in batch: %w", entry.ID, err)
		}
		pipe.Set(ctx, cacheKeyPrefix+entry.ID, raw, 0)
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		// Collect and surface the first non-nil individual command error for
		// diagnostics; the overall error is already wrapped above.
		for _, cmd := range cmds {
			if cmd.Err() != nil && !errors.Is(cmd.Err(), redis.Nil) {
				return fmt.Errorf("cache: batch pipeline exec (%d entries): first error on %q: %w",
					len(entries), cmd.(*redis.StatusCmd).Args()[1], cmd.Err())
			}
		}
		return fmt.Errorf("cache: batch pipeline exec (%d entries): %w", len(entries), err)
	}
	return nil
}
