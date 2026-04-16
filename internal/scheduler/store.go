package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ScheduleStore is the persistence contract for the deferred-delivery queue.
// The scheduler service is the sole reader and writer of this store.
type ScheduleStore interface {
	// Enqueue adds msg to the sorted set with score = sendAt.Unix().
	// Redis ZADD semantics: duplicate members (identical JSON) update the score
	// in-place, making Enqueue naturally idempotent for retry scenarios.
	Enqueue(ctx context.Context, msg ScheduledMessage, sendAt time.Time) error

	// Poll returns all messages with score <= until.Unix(), ordered by
	// ascending score (earliest-due first). Corrupted members are silently
	// skipped so a single bad entry cannot stall the entire poll loop.
	Poll(ctx context.Context, until time.Time) ([]ScheduledMessage, error)

	// Remove deletes msgs from the sorted set after successful publication to
	// their channel topics. Callers must pass the exact ScheduledMessage values
	// returned by Poll; re-serialisation produces byte-for-byte identical members
	// because encoding/json sorts map keys deterministically.
	Remove(ctx context.Context, msgs []ScheduledMessage) error
}

// RedisScheduleStore implements ScheduleStore using a Redis sorted set.
type RedisScheduleStore struct {
	client redis.UniversalClient
}

// NewRedisScheduleStore creates a store backed by client.
func NewRedisScheduleStore(client redis.UniversalClient) *RedisScheduleStore {
	return &RedisScheduleStore{client: client}
}

// Enqueue serialises msg to JSON and adds it to the sorted set.
func (s *RedisScheduleStore) Enqueue(ctx context.Context, msg ScheduledMessage, sendAt time.Time) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("schedule store: marshal: %w", err)
	}
	z := redis.Z{
		Score:  float64(sendAt.Unix()),
		Member: string(raw),
	}
	if err := s.client.ZAdd(ctx, scheduleQueueKey, z).Err(); err != nil {
		return fmt.Errorf("schedule store: ZADD: %w", err)
	}
	return nil
}

// Poll fetches all members with score in the range (-inf, until].
func (s *RedisScheduleStore) Poll(ctx context.Context, until time.Time) ([]ScheduledMessage, error) {
	members, err := s.client.ZRangeByScore(ctx, scheduleQueueKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", until.Unix()),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("schedule store: ZRANGEBYSCORE: %w", err)
	}

	msgs := make([]ScheduledMessage, 0, len(members))
	for _, m := range members {
		var msg ScheduledMessage
		if err := json.Unmarshal([]byte(m), &msg); err != nil {
			// Corrupted entry: skip rather than stalling the poll loop.
			// The poller logs corruption at the call site via the returned slice length.
			continue
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// Remove deletes each message from the sorted set using a pipelined ZREM.
// Re-serialising each ScheduledMessage produces the same bytes that Enqueue
// wrote because encoding/json sorts map keys alphabetically, and all other
// fields use canonical Go JSON formatting (RFC 3339 for time.Time, etc.).
func (s *RedisScheduleStore) Remove(ctx context.Context, msgs []ScheduledMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	raws := make([]string, len(msgs))
	for i, msg := range msgs {
		raw, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("schedule store: marshal for removal [%d]: %w", i, err)
		}
		raws[i] = string(raw)
		pipe.ZRem(ctx, scheduleQueueKey, raws[i])
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule store: ZREM pipeline: %w", err)
	}
	return nil
}
