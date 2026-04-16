package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"notification-service/pkg/transport"
)

// ── memScheduleStore ─────────────────────────────────────────────────────────

// entry pairs a score with its serialised member for the in-memory sorted set.
type entry struct {
	score  float64
	member string
}

// memScheduleStore is a thread-safe in-memory ScheduleStore for tests.
type memScheduleStore struct {
	mu      sync.Mutex
	entries []entry
}

func newMemScheduleStore() *memScheduleStore { return &memScheduleStore{} }

func (s *memScheduleStore) Enqueue(_ context.Context, msg ScheduledMessage, sendAt time.Time) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Upsert: replace existing member or append.
	member := string(raw)
	score := float64(sendAt.Unix())
	for i, e := range s.entries {
		if e.member == member {
			s.entries[i].score = score
			return nil
		}
	}
	s.entries = append(s.entries, entry{score: score, member: member})
	return nil
}

func (s *memScheduleStore) Poll(_ context.Context, until time.Time) ([]ScheduledMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := float64(until.Unix())
	var due []entry
	for _, e := range s.entries {
		if e.score <= limit {
			due = append(due, e)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].score < due[j].score })

	msgs := make([]ScheduledMessage, 0, len(due))
	for _, e := range due {
		var m ScheduledMessage
		if err := json.Unmarshal([]byte(e.member), &m); err != nil {
			continue
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func (s *memScheduleStore) Remove(_ context.Context, msgs []ScheduledMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	toRemove := make(map[string]struct{}, len(msgs))
	for _, msg := range msgs {
		raw, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		toRemove[string(raw)] = struct{}{}
	}

	filtered := s.entries[:0]
	for _, e := range s.entries {
		if _, remove := toRemove[e.member]; !remove {
			filtered = append(filtered, e)
		}
	}
	s.entries = filtered
	return nil
}

func (s *memScheduleStore) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// ── captureWriter ─────────────────────────────────────────────────────────────

type captureWriter struct {
	mu        sync.Mutex
	calls     [][]transport.Envelope
	returnErr error
}

func (c *captureWriter) Write(_ context.Context, envelopes []transport.Envelope) error {
	if c.returnErr != nil {
		return c.returnErr
	}
	cp := make([]transport.Envelope, len(envelopes))
	copy(cp, envelopes)
	c.mu.Lock()
	c.calls = append(c.calls, cp)
	c.mu.Unlock()
	return nil
}

func (c *captureWriter) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func (c *captureWriter) firstBatch() []transport.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) == 0 {
		return nil
	}
	return c.calls[0]
}

// ── stubReader (Kafka) ────────────────────────────────────────────────────────

// stubReader feeds pre-loaded kafka.Messages and then blocks until the context
// is cancelled, simulating a real Kafka reader that waits for new messages.
type stubReader struct {
	msgs    []kafka.Message
	pos     int
	commits []kafka.Message
}

func (r *stubReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if r.pos < len(r.msgs) {
		m := r.msgs[r.pos]
		r.pos++
		return m, nil
	}
	// Block until ctx is done, mimicking a real reader waiting for more messages.
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *stubReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.commits = append(r.commits, msgs...)
	return nil
}

func (r *stubReader) Close() error { return nil }

// ── error stubs ───────────────────────────────────────────────────────────────

var errStoreFailure = errors.New("store: simulated failure")

type failingStore struct{}

func (failingStore) Enqueue(_ context.Context, _ ScheduledMessage, _ time.Time) error {
	return errStoreFailure
}
func (failingStore) Poll(_ context.Context, _ time.Time) ([]ScheduledMessage, error) {
	return nil, errStoreFailure
}
func (failingStore) Remove(_ context.Context, _ []ScheduledMessage) error { return errStoreFailure }
