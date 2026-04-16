package template

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
)

// ── In-memory stubs ───────────────────────────────────────────────────────────

type memRepo struct {
	rows      []*Template
	createErr error
	listErr   error
}

func (r *memRepo) Create(_ context.Context, t *Template) (*Template, error) {
	if r.createErr != nil {
		return nil, r.createErr
	}
	t.CreatedAt = time.Now().UTC()
	cp := *t
	r.rows = append(r.rows, &cp)
	return &cp, nil
}

func (r *memRepo) GetLatestActive(_ context.Context, templateID string) (*Template, error) {
	var best *Template
	for _, row := range r.rows {
		if row.TemplateID == templateID && row.IsActive {
			if best == nil || row.Version > best.Version {
				best = row
			}
		}
	}
	if best == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, templateID)
	}
	cp := *best
	return &cp, nil
}

func (r *memRepo) GetByVersion(_ context.Context, templateID string, version int) (*Template, error) {
	for _, row := range r.rows {
		if row.TemplateID == templateID && row.Version == version {
			cp := *row
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("%w: %s v%d", ErrNotFound, templateID, version)
}

func (r *memRepo) ListActive(_ context.Context) ([]*Template, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	// Return highest active version per template_id.
	latest := make(map[string]*Template)
	for _, row := range r.rows {
		if !row.IsActive {
			continue
		}
		if cur, ok := latest[row.TemplateID]; !ok || row.Version > cur.Version {
			cp := *row
			latest[row.TemplateID] = &cp
		}
	}
	out := make([]*Template, 0, len(latest))
	for _, t := range latest {
		out = append(out, t)
	}
	return out, nil
}

func (r *memRepo) DeactivateAll(_ context.Context, templateID string) error {
	for i, row := range r.rows {
		if row.TemplateID == templateID {
			r.rows[i].IsActive = false
		}
	}
	return nil
}

func (r *memRepo) MaxVersion(_ context.Context, templateID string) (int, error) {
	max := 0
	for _, row := range r.rows {
		if row.TemplateID == templateID && row.Version > max {
			max = row.Version
		}
	}
	return max, nil
}

// ─────────────────────────────────────────────────────────────────────────────

type memCache struct {
	entries  map[string]*CacheEntry
	setErr   error
	delErr   error
	batchErr error
}

func newMemCache() *memCache {
	return &memCache{entries: make(map[string]*CacheEntry)}
}

func (c *memCache) Set(_ context.Context, e *CacheEntry) error {
	if c.setErr != nil {
		return c.setErr
	}
	c.entries[e.ID] = e
	return nil
}

func (c *memCache) Delete(_ context.Context, templateID string) error {
	if c.delErr != nil {
		return c.delErr
	}
	delete(c.entries, templateID)
	return nil
}

func (c *memCache) SetBatch(_ context.Context, entries []*CacheEntry) error {
	if c.batchErr != nil {
		return c.batchErr
	}
	for _, e := range entries {
		c.entries[e.ID] = e
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────

type memBodyStore struct {
	objects map[string][]byte
	putErr  error
	getErr  error
}

func newMemBodyStore() *memBodyStore {
	return &memBodyStore{objects: make(map[string][]byte)}
}

func (b *memBodyStore) Put(_ context.Context, key string, body []byte, _ string) error {
	if b.putErr != nil {
		return b.putErr
	}
	cp := make([]byte, len(body))
	copy(cp, body)
	b.objects[key] = cp
	return nil
}

func (b *memBodyStore) Get(_ context.Context, key string) ([]byte, error) {
	if b.getErr != nil {
		return nil, b.getErr
	}
	data, ok := b.objects[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return data, nil
}

// ─────────────────────────────────────────────────────────────────────────────

type capturePublisher struct {
	events []TemplateEvent
	err    error
}

func (p *capturePublisher) Publish(_ context.Context, event TemplateEvent) error {
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, event)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────

func newTestService(t *testing.T) (*Service, *memRepo, *memCache, *memBodyStore, *capturePublisher) {
	t.Helper()
	repo := &memRepo{}
	cache := newMemCache()
	store := newMemBodyStore()
	pub := &capturePublisher{}
	svc := NewService(repo, cache, store, pub, noopLogger(t))
	return svc, repo, cache, store, pub
}

// noopLogger returns a zap no-op logger. The *testing.T parameter is kept
// for symmetry with the handler_test.go helper convention.
func noopLogger(_ *testing.T) *zap.Logger { return zap.NewNop() }

// ── Service tests ─────────────────────────────────────────────────────────────

func TestService_Create_Success(t *testing.T) {
	t.Parallel()
	svc, _, cache, store, pub := newTestService(t)

	resp, err := svc.Create(context.Background(), CreateRequest{
		TemplateID:        "order_confirmation_email",
		Channel:           ChannelEmail,
		Name:              "Order Confirmation",
		BodyHTML:          "<h1>Thanks {{customer_name}}</h1>",
		RequiredVariables: []string{"customer_name"},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Version != 1 {
		t.Errorf("want version 1, got %d", resp.Version)
	}
	if !resp.IsActive {
		t.Error("want is_active=true")
	}

	// Cache must be warmed.
	entry, ok := cache.entries["order_confirmation_email"]
	if !ok {
		t.Fatal("cache not set after create")
	}
	if entry.Version != 1 {
		t.Errorf("cache version: want 1, got %d", entry.Version)
	}

	// S3 body must be uploaded.
	key := s3Key("order_confirmation_email", 1)
	if _, ok := store.objects[key]; !ok {
		t.Errorf("s3 body not uploaded at key %q", key)
	}

	// Kafka event must be emitted.
	if len(pub.events) != 1 || pub.events[0].EventType != EventCreated {
		t.Errorf("want 1 TEMPLATE_CREATED event, got %v", pub.events)
	}
}

func TestService_Create_DuplicateTemplateID(t *testing.T) {
	t.Parallel()
	svc, _, _, _, _ := newTestService(t)

	req := CreateRequest{
		TemplateID: "dup",
		Channel:    ChannelEmail,
		Name:       "Dup",
		BodyHTML:   "<p>body</p>",
	}
	if _, err := svc.Create(context.Background(), req); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := svc.Create(context.Background(), req)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("want ErrAlreadyExists, got %v", err)
	}
}

func TestService_Create_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  CreateRequest
	}{
		{"empty template_id", CreateRequest{Channel: ChannelEmail, Name: "N", BodyHTML: "b"}},
		{"unknown channel", CreateRequest{TemplateID: "x", Channel: "sms", Name: "N", BodyHTML: "b"}},
		{"empty name", CreateRequest{TemplateID: "x", Channel: ChannelEmail, BodyHTML: "b"}},
		{"empty body_html", CreateRequest{TemplateID: "x", Channel: ChannelEmail, Name: "N"}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc, _, _, _, _ := newTestService(t)
			_, err := svc.Create(context.Background(), tc.req)
			if err == nil {
				t.Fatal("want validation error, got nil")
			}
		})
	}
}

func TestService_CreateVersion_IncrementsVersion(t *testing.T) {
	t.Parallel()
	svc, _, cache, _, pub := newTestService(t)

	// Create v1.
	if _, err := svc.Create(context.Background(), CreateRequest{
		TemplateID: "tmpl-a",
		Channel:    ChannelEmail,
		Name:       "A",
		BodyHTML:   "<p>v1</p>",
	}); err != nil {
		t.Fatalf("create v1: %v", err)
	}

	// Create v2.
	resp, err := svc.CreateVersion(context.Background(), "tmpl-a", CreateVersionRequest{
		BodyHTML:          "<p>v2</p>",
		RequiredVariables: []string{"x"},
	})
	if err != nil {
		t.Fatalf("create version: %v", err)
	}

	if resp.Version != 2 {
		t.Errorf("want version 2, got %d", resp.Version)
	}
	if !resp.IsActive {
		t.Error("new version must be active")
	}

	// Cache must reflect the new version.
	if cache.entries["tmpl-a"].Version != 2 {
		t.Errorf("cache: want version 2, got %d", cache.entries["tmpl-a"].Version)
	}

	// Exactly two events: CREATED + VERSION_CREATED.
	if len(pub.events) != 2 {
		t.Fatalf("want 2 events, got %d", len(pub.events))
	}
	if pub.events[1].EventType != EventVersioned {
		t.Errorf("want VERSION_CREATED, got %s", pub.events[1].EventType)
	}
}

func TestService_CreateVersion_TemplateNotFound(t *testing.T) {
	t.Parallel()
	svc, _, _, _, _ := newTestService(t)

	_, err := svc.CreateVersion(context.Background(), "ghost", CreateVersionRequest{BodyHTML: "<p>x</p>"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", err)
	}
}

func TestService_Deactivate(t *testing.T) {
	t.Parallel()
	svc, repo, cache, _, pub := newTestService(t)

	if _, err := svc.Create(context.Background(), CreateRequest{
		TemplateID: "to-deactivate",
		Channel:    ChannelEmail,
		Name:       "D",
		BodyHTML:   "<p>bye</p>",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := svc.Deactivate(context.Background(), "to-deactivate"); err != nil {
		t.Fatalf("deactivate: %v", err)
	}

	// All DB rows must be inactive.
	for _, row := range repo.rows {
		if row.TemplateID == "to-deactivate" && row.IsActive {
			t.Error("expected is_active=false after deactivation")
		}
	}

	// Cache entry must be removed.
	if _, ok := cache.entries["to-deactivate"]; ok {
		t.Error("cache entry should be deleted after deactivation")
	}

	// Kafka event.
	last := pub.events[len(pub.events)-1]
	if last.EventType != EventDeactivated {
		t.Errorf("want TEMPLATE_DEACTIVATED event, got %s", last.EventType)
	}
}

func TestService_WarmCache(t *testing.T) {
	t.Parallel()
	svc, _, cache, _, _ := newTestService(t)

	// Manually seed the repo with two active templates at different versions.
	svc.repo.(*memRepo).rows = []*Template{
		{ID: "1", TemplateID: "t1", Version: 1, Channel: ChannelEmail, IsActive: false},
		{ID: "2", TemplateID: "t1", Version: 2, Channel: ChannelEmail, IsActive: true},
		{ID: "3", TemplateID: "t2", Version: 1, Channel: ChannelInApp, IsActive: true},
	}

	if err := svc.WarmCache(context.Background()); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	if len(cache.entries) != 2 {
		t.Fatalf("want 2 cache entries, got %d", len(cache.entries))
	}
	if cache.entries["t1"].Version != 2 {
		t.Errorf("t1: want version 2 in cache, got %d", cache.entries["t1"].Version)
	}
}

func TestService_S3UploadFailure_ReturnsError(t *testing.T) {
	t.Parallel()
	svc, _, _, store, _ := newTestService(t)
	store.putErr = errors.New("s3: request timeout")

	_, err := svc.Create(context.Background(), CreateRequest{
		TemplateID: "s3-fail",
		Channel:    ChannelEmail,
		Name:       "S3 Fail",
		BodyHTML:   "<p>body</p>",
	})
	if err == nil {
		t.Fatal("want error when S3 upload fails, got nil")
	}
}

func TestService_CacheSetFailure_ReturnsError(t *testing.T) {
	t.Parallel()
	svc, _, cache, _, _ := newTestService(t)
	cache.setErr = errors.New("redis: connection refused")

	_, err := svc.Create(context.Background(), CreateRequest{
		TemplateID: "cache-fail",
		Channel:    ChannelEmail,
		Name:       "Cache Fail",
		BodyHTML:   "<p>body</p>",
	})
	if err == nil {
		t.Fatal("want hard error when Redis Set fails (Redis is mandatory)")
	}
}

func TestService_KafkaFailure_NonFatal(t *testing.T) {
	t.Parallel()
	svc, _, _, _, pub := newTestService(t)
	pub.err = errors.New("kafka: broker unavailable")

	// Kafka publish failure must NOT propagate — create should still succeed.
	_, err := svc.Create(context.Background(), CreateRequest{
		TemplateID: "kafka-fail",
		Channel:    ChannelEmail,
		Name:       "K",
		BodyHTML:   "<p>body</p>",
	})
	if err != nil {
		t.Fatalf("kafka failure should be non-fatal, got error: %v", err)
	}
}

// Satisfy the time import used in memRepo.Create.
var _ = time.Now
