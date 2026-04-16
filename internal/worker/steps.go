package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	workerIdempotencyPrefix = "worker:done:"
	workerIdempotencyTTL    = 48 * time.Hour
)

// workerCache is the minimal Redis surface used by worker steps.
// Defined at point of use so tests inject a simple in-memory implementation
// without satisfying the full redis.UniversalClient interface.
type workerCache interface {
	Get(ctx context.Context, key string) (string, bool)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
}

// RedisWorkerCache adapts redis.UniversalClient to workerCache.
type RedisWorkerCache struct{ c redis.UniversalClient }

// NewRedisWorkerCache wraps client.
func NewRedisWorkerCache(c redis.UniversalClient) *RedisWorkerCache {
	return &RedisWorkerCache{c: c}
}

func (r *RedisWorkerCache) Get(ctx context.Context, key string) (string, bool) {
	val, err := r.c.Get(ctx, key).Result()
	if err != nil {
		return "", false // treat miss and error identically: allow through
	}
	return val, true
}

func (r *RedisWorkerCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return r.c.Set(ctx, key, value, ttl).Err()
}

// ── IdempotencyStep ───────────────────────────────────────────────────────────

// IdempotencyStep prevents re-delivery of a notification that was already
// processed successfully. It uses a distinct key prefix from the ingestion
// layer so the two idempotency namespaces stay independent.
//
// The done key is written by DeliveryStep after a confirmed send, never here,
// so we never mark a notification DONE before it is actually delivered.
type IdempotencyStep struct {
	cache workerCache
}

// NewIdempotencyStep creates an IdempotencyStep backed by cache.
func NewIdempotencyStep(cache workerCache) *IdempotencyStep {
	return &IdempotencyStep{cache: cache}
}

func (s *IdempotencyStep) Name() string { return "idempotency" }

// Execute checks whether this notification was already delivered.
// Returns ErrSuppressed when the done key is present, halting the chain.
func (s *IdempotencyStep) Execute(ctx context.Context, item *WorkItem) error {
	_, found := s.cache.Get(ctx, workerIdempotencyPrefix+item.NotificationID)
	if found {
		return ErrSuppressed
	}
	return nil
}

// ── PreferenceStep ────────────────────────────────────────────────────────────

// PreferenceStep enforces user notification preferences.
// This is a placeholder implementation that always allows delivery.
// It will be replaced when the Preference Service is wired in.
type PreferenceStep struct {
	logger *zap.Logger
}

// NewPreferenceStep creates a PreferenceStep.
func NewPreferenceStep(logger *zap.Logger) *PreferenceStep {
	return &PreferenceStep{logger: logger}
}

func (s *PreferenceStep) Name() string { return "preference" }

// Execute always returns nil. The real implementation will call the Preference
// Service API and return ErrSuppressed when the user has opted out.
func (s *PreferenceStep) Execute(_ context.Context, _ *WorkItem) error {
	return nil
}

// ── TemplateStep ──────────────────────────────────────────────────────────────

// TemplateStep fetches the raw template from the Template Service and renders
// it locally using Go's template engine. It populates item.RenderedSubject and
// item.RenderedBody for the subsequent DeliveryStep.
type TemplateStep struct {
	engine TemplateEngine
}

// NewTemplateStep creates a TemplateStep backed by engine.
func NewTemplateStep(engine TemplateEngine) *TemplateStep {
	return &TemplateStep{engine: engine}
}

func (s *TemplateStep) Name() string { return "template" }

// Execute fetches the template body and substitutes variables.
func (s *TemplateStep) Execute(ctx context.Context, item *WorkItem) error {
	fetch, err := s.engine.Fetch(ctx, item.TemplateID)
	if err != nil {
		return fmt.Errorf("template step: fetch %s: %w", item.TemplateID, err)
	}

	subject, err := renderSubject(fetch.Subject, item.Variables)
	if err != nil {
		return fmt.Errorf("template step: render subject: %w", err)
	}

	body, err := renderBody(fetch.BodyHTML, item.Variables)
	if err != nil {
		return fmt.Errorf("template step: render body: %w", err)
	}

	item.RenderedSubject = subject
	item.RenderedBody = body
	return nil
}

// ── DeliveryStep ──────────────────────────────────────────────────────────────

// DeliveryStep sends the rendered email via the EmailProvider and, on success,
// writes the idempotency key to the cache so re-deliveries are suppressed.
//
// User address resolution is deliberately simplified: in production the
// user_id would be resolved to an email address via a User Service call.
// For now, the user_id stands in as the recipient so the worker can be tested
// end-to-end without a User Service dependency.
type DeliveryStep struct {
	provider EmailProvider
	cache    workerCache
}

// NewDeliveryStep creates a DeliveryStep.
func NewDeliveryStep(provider EmailProvider, cache workerCache) *DeliveryStep {
	return &DeliveryStep{provider: provider, cache: cache}
}

func (s *DeliveryStep) Name() string { return "delivery" }

// Execute sends the email and commits the idempotency key on success.
func (s *DeliveryStep) Execute(ctx context.Context, item *WorkItem) error {
	msg := EmailMessage{
		To:      item.UserID,
		Subject: item.RenderedSubject,
		Body:    item.RenderedBody,
	}
	if err := s.provider.Send(ctx, msg); err != nil {
		return fmt.Errorf("delivery step: send to %s: %w", item.UserID, err)
	}
	// Best-effort: a Set failure here means the notification may be
	// re-delivered on the next Kafka retry. That is acceptable — idempotent
	// email providers handle duplicates at the send layer.
	_ = s.cache.Set(ctx, workerIdempotencyPrefix+item.NotificationID, "1", workerIdempotencyTTL)
	return nil
}
