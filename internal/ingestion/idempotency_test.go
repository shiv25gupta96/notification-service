package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// ── In-memory stub ────────────────────────────────────────────────────────────

// memStore is an in-memory IdempotencyStore used exclusively in tests.
// It surfaces the exact same contract as RedisIdempotencyStore without
// requiring a live Redis instance.
type memStore struct {
	data    map[string][]byte
	checkErr error // if non-nil, Check returns this error
	commitErr error // if non-nil, Commit returns this error
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (m *memStore) Check(_ context.Context, key string) (*NotificationResponse, error) {
	if m.checkErr != nil {
		return nil, m.checkErr
	}
	raw, ok := m.data[key]
	if !ok {
		return nil, nil
	}
	var resp NotificationResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, nil // treat as miss, matching RedisIdempotencyStore behaviour
	}
	return &resp, nil
}

func (m *memStore) Commit(_ context.Context, key string, resp *NotificationResponse) error {
	if m.commitErr != nil {
		return m.commitErr
	}
	if _, exists := m.data[key]; exists {
		return nil // SET NX semantics: first writer wins
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	m.data[key] = raw
	return nil
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestIdempotencyStore_CheckMissAndCommit(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	ctx := context.Background()
	key := "order-123-confirm"

	// Miss: key not yet written.
	got, err := store.Check(ctx, key)
	if err != nil {
		t.Fatalf("Check error on miss: %v", err)
	}
	if got != nil {
		t.Fatalf("want nil on miss, got %+v", got)
	}

	// Commit a response.
	resp := &NotificationResponse{
		ParentNotificationID: "parent-uuid",
		Channels: []ChannelResponse{
			{Channel: ChannelEmail, NotificationID: "child-uuid", Status: StatusQueued},
		},
	}
	if err := store.Commit(ctx, key, resp); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	// Hit: must return the previously committed value.
	got, err = store.Check(ctx, key)
	if err != nil {
		t.Fatalf("Check error on hit: %v", err)
	}
	if got == nil {
		t.Fatal("want non-nil on hit, got nil")
	}
	if got.ParentNotificationID != resp.ParentNotificationID {
		t.Errorf("want parent_id %q, got %q", resp.ParentNotificationID, got.ParentNotificationID)
	}
}

func TestIdempotencyStore_SetNXSemantics(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	ctx := context.Background()
	key := "idem-race"

	first := &NotificationResponse{ParentNotificationID: "first-winner"}
	second := &NotificationResponse{ParentNotificationID: "second-loser"}

	if err := store.Commit(ctx, key, first); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if err := store.Commit(ctx, key, second); err != nil {
		t.Fatalf("second Commit: %v", err)
	}

	got, _ := store.Check(ctx, key)
	if got.ParentNotificationID != "first-winner" {
		t.Errorf("SET NX violated: want first-winner, got %q", got.ParentNotificationID)
	}
}

func TestIdempotencyStore_CheckError(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	store.checkErr = errors.New("redis: connection refused")

	_, err := store.Check(context.Background(), "any")
	if err == nil {
		t.Fatal("want error when redis is down, got nil")
	}
}

func TestIdempotencyStore_CommitError(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	store.commitErr = errors.New("redis: write timeout")

	err := store.Commit(context.Background(), "any", &NotificationResponse{})
	if err == nil {
		t.Fatal("want error when redis write fails, got nil")
	}
}

func TestIdempotencyStore_CorruptedEntryTreatedAsMiss(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	// Manually inject corrupt bytes so Check hits the unmarshal error path.
	store.data["bad-key"] = []byte("not-json{{{")

	got, err := store.Check(context.Background(), "bad-key")
	if err != nil {
		t.Fatalf("unexpected error for corrupted entry: %v", err)
	}
	if got != nil {
		t.Errorf("want nil for corrupted entry (treated as miss), got %+v", got)
	}
}

// ── TTL smoke-test using the real Redis implementation contract ───────────────
// This verifies the TTL constant is set to a sane value without requiring Redis.

func TestDefaultIdempotencyTTL(t *testing.T) {
	t.Parallel()
	if defaultIdempotencyTTL < 1*time.Hour {
		t.Errorf("defaultIdempotencyTTL %v is suspiciously short — should be ≥ 1 hour", defaultIdempotencyTTL)
	}
	if defaultIdempotencyTTL > 7*24*time.Hour {
		t.Errorf("defaultIdempotencyTTL %v is suspiciously long — should be ≤ 7 days", defaultIdempotencyTTL)
	}
}
