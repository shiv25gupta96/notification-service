package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	idempotencyKeyPrefix = "idempotency:"
	defaultIdempotencyTTL = 24 * time.Hour
)

// IdempotencyStore is the contract for checking and committing idempotency
// keys. It is defined here (at point of use) per Go interface conventions.
type IdempotencyStore interface {
	// Check returns the cached NotificationResponse for key, or nil when
	// the key has not been seen before. An error is returned only on
	// infrastructure failure — the caller must treat that as a hard 500.
	Check(ctx context.Context, key string) (*NotificationResponse, error)

	// Commit persists resp so future Check calls return it. The call is
	// best-effort: if it fails the caller logs a warning and continues,
	// accepting a narrow window for a duplicate publish that downstream
	// workers will deduplicate by notification_id.
	Commit(ctx context.Context, key string, resp *NotificationResponse) error
}

// RedisIdempotencyStore implements IdempotencyStore using a Redis
// UniversalClient (works with both standalone and cluster topologies).
type RedisIdempotencyStore struct {
	client redis.UniversalClient
	ttl    time.Duration
}

// NewRedisIdempotencyStore creates a store backed by client. Pass ttl = 0
// to use the default 24-hour retention window.
func NewRedisIdempotencyStore(client redis.UniversalClient, ttl time.Duration) *RedisIdempotencyStore {
	if ttl == 0 {
		ttl = defaultIdempotencyTTL
	}
	return &RedisIdempotencyStore{client: client, ttl: ttl}
}

// Check returns a previously committed response or nil on a miss.
// JSON corruption is treated as a miss so the request re-processes rather
// than hard-failing — a deliberate trade-off in favour of liveness.
func (s *RedisIdempotencyStore) Check(ctx context.Context, key string) (*NotificationResponse, error) {
	raw, err := s.client.Get(ctx, idempotencyKeyPrefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("idempotency check: redis GET %q: %w", key, err)
	}

	var resp NotificationResponse
	if jsonErr := json.Unmarshal(raw, &resp); jsonErr != nil {
		// Corrupted entry. Treat as miss; the key will be overwritten on
		// Commit with the fresh response.
		return nil, nil
	}
	return &resp, nil
}

// Commit persists resp under key using SET NX semantics so that a concurrent
// winner's value is never overwritten. A failure to write is not fatal — see
// interface doc for the rationale.
func (s *RedisIdempotencyStore) Commit(ctx context.Context, key string, resp *NotificationResponse) error {
	raw, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("idempotency commit: marshal response: %w", err)
	}

	// SetNX: write only if the key does not already exist. This protects
	// against a concurrent duplicate that also reached Commit.
	if err := s.client.SetNX(ctx, idempotencyKeyPrefix+key, raw, s.ttl).Err(); err != nil {
		return fmt.Errorf("idempotency commit: redis SETNX %q: %w", key, err)
	}
	return nil
}
