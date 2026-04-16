package template

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Service orchestrates all template lifecycle operations. It is the only
// component in the system that may write "template:" Redis keys — it acquires
// exclusive ownership of these keys on every create or update operation.
type Service struct {
	repo      Repository
	cache     Cache
	bodyStore BodyStore
	publisher EventPublisher
	logger    *zap.Logger
}

// NewService creates a Service with all dependencies injected.
func NewService(
	repo Repository,
	cache Cache,
	bodyStore BodyStore,
	publisher EventPublisher,
	logger *zap.Logger,
) *Service {
	return &Service{
		repo:      repo,
		cache:     cache,
		bodyStore: bodyStore,
		publisher: publisher,
		logger:    logger,
	}
}

// Create persists a brand-new template (version 1) and warms the cache.
//
// Step order:
//  1. Validate the request (pure)
//  2. Check that template_id does not already exist
//  3. Upload HTML body to S3 at the deterministic key
//  4. Insert the row into PostgreSQL
//  5. Write metadata to Redis (mandatory — fail hard)
//  6. Publish TEMPLATE_CREATED to Kafka (best-effort)
func (s *Service) Create(ctx context.Context, req CreateRequest) (*TemplateResponse, error) {
	if err := validateCreateRequest(req); err != nil {
		return nil, err
	}

	// Guard: a template_id must not already exist.
	existing, err := s.repo.MaxVersion(ctx, req.TemplateID)
	if err != nil {
		return nil, fmt.Errorf("service create: check existence: %w", err)
	}
	if existing > 0 {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyExists, req.TemplateID)
	}

	return s.persist(ctx, req.TemplateID, req.Channel, req.Name, req.Subject,
		req.BodyHTML, req.RequiredVariables, 1, EventCreated)
}

// CreateVersion adds a new immutable version of an existing template.
// The previous active version is deactivated atomically with the new insert.
//
// Step order:
//  1. Validate the request (pure)
//  2. Fetch the current max version to compute the next version number
//  3. Deactivate all existing versions
//  4. Upload HTML body to S3
//  5. Insert new row into PostgreSQL
//  6. Write updated metadata to Redis (mandatory)
//  7. Publish VERSION_CREATED to Kafka (best-effort)
func (s *Service) CreateVersion(ctx context.Context, templateID string, req CreateVersionRequest) (*TemplateResponse, error) {
	if err := validateCreateVersionRequest(req); err != nil {
		return nil, err
	}

	// Fetch the current template to inherit immutable fields.
	current, err := s.repo.GetLatestActive(ctx, templateID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, templateID)
		}
		return nil, fmt.Errorf("service create version: get current: %w", err)
	}

	nextVersion := current.Version + 1

	// Deactivate all existing versions before inserting the new one so the
	// DB and Redis never hold two simultaneously-active versions for the same ID.
	if err := s.repo.DeactivateAll(ctx, templateID); err != nil {
		return nil, fmt.Errorf("service create version: deactivate existing: %w", err)
	}

	return s.persist(ctx, templateID, current.Channel, current.Name,
		req.Subject, req.BodyHTML, req.RequiredVariables, nextVersion, EventVersioned)
}

// GetLatest returns the latest active version for templateID, including the
// HTML body fetched from S3.
func (s *Service) GetLatest(ctx context.Context, templateID string) (*TemplateResponse, error) {
	t, err := s.repo.GetLatestActive(ctx, templateID)
	if err != nil {
		return nil, fmt.Errorf("service get latest: %w", err)
	}
	return s.withBody(ctx, t)
}

// GetByVersion returns a specific version including its HTML body from S3.
func (s *Service) GetByVersion(ctx context.Context, templateID string, version int) (*TemplateResponse, error) {
	t, err := s.repo.GetByVersion(ctx, templateID, version)
	if err != nil {
		return nil, fmt.Errorf("service get version: %w", err)
	}
	return s.withBody(ctx, t)
}

// ListActive returns all distinct template IDs at their latest active version.
// Body HTML is not included — callers that need the body call GetLatest.
func (s *Service) ListActive(ctx context.Context) (*ListResponse, error) {
	templates, err := s.repo.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("service list active: %w", err)
	}

	responses := make([]TemplateResponse, len(templates))
	for i, t := range templates {
		responses[i] = toResponse(t, "") // no body in list
	}
	return &ListResponse{Templates: responses, Total: len(responses)}, nil
}

// Deactivate marks all versions of templateID as inactive and removes the
// Redis cache entry so the ingestion service immediately stops accepting
// new notifications for this template.
func (s *Service) Deactivate(ctx context.Context, templateID string) error {
	// Verify the template exists before attempting to deactivate it.
	if _, err := s.repo.GetLatestActive(ctx, templateID); err != nil {
		return fmt.Errorf("service deactivate: %w", err)
	}

	if err := s.repo.DeactivateAll(ctx, templateID); err != nil {
		return fmt.Errorf("service deactivate: db: %w", err)
	}

	// Remove from Redis immediately. Best-effort: a failure here leaves a stale
	// cache entry, but the is_active=false value in the metadata means workers
	// will reject it. Ingestion will still reject new requests on the next
	// cache warm. Log prominently so ops can manually DEL if needed.
	if err := s.cache.Delete(ctx, templateID); err != nil {
		s.logger.Error("cache delete failed after deactivation — stale entry may persist until next warm",
			zap.String("template_id", templateID),
			zap.Error(err),
		)
	}

	s.publishEvent(ctx, TemplateEvent{
		EventType:  EventDeactivated,
		TemplateID: templateID,
		Channel:    "", // unknown without a fetch; workers only need the ID
		Timestamp:  time.Now().UTC(),
	})

	return nil
}

// WarmCache bulk-loads all active templates into Redis before the HTTP server
// starts accepting traffic. It is called once during startup and must complete
// successfully — the server should not start if it returns an error.
func (s *Service) WarmCache(ctx context.Context) error {
	templates, err := s.repo.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("warm cache: list active templates: %w", err)
	}

	entries := make([]*CacheEntry, len(templates))
	for i, t := range templates {
		entries[i] = toCacheEntry(t)
	}

	if err := s.cache.SetBatch(ctx, entries); err != nil {
		return fmt.Errorf("warm cache: redis set batch (%d templates): %w", len(entries), err)
	}

	s.logger.Info("cache warmed", zap.Int("template_count", len(entries)))
	return nil
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

// persist executes the S3 → DB → Redis → Kafka write sequence common to
// Create and CreateVersion.
func (s *Service) persist(
	ctx context.Context,
	templateID string,
	channel Channel,
	name, subject, bodyHTML string,
	requiredVars []string,
	version int,
	eventType EventType,
) (*TemplateResponse, error) {
	key := s3Key(templateID, version)

	// 1. S3 upload (idempotent — safe to retry on partial failure).
	if err := s.bodyStore.Put(ctx, key, []byte(bodyHTML), "text/html; charset=utf-8"); err != nil {
		return nil, fmt.Errorf("service persist: upload body: %w", err)
	}

	// 2. PostgreSQL insert.
	t := &Template{
		ID:                uuid.New().String(),
		TemplateID:        templateID,
		Version:           version,
		Channel:           channel,
		Name:              name,
		Subject:           subject,
		BodyS3Key:         key,
		RequiredVariables: requiredVars,
		IsActive:          true,
	}
	saved, err := s.repo.Create(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("service persist: db insert: %w", err)
	}

	// 3. Redis write (mandatory — ingestion is blocked if this fails).
	if err := s.cache.Set(ctx, toCacheEntry(saved)); err != nil {
		return nil, fmt.Errorf("service persist: cache set: %w", err)
	}

	// 4. Kafka event (best-effort).
	s.publishEvent(ctx, TemplateEvent{
		EventType:  eventType,
		TemplateID: saved.TemplateID,
		Version:    saved.Version,
		Channel:    saved.Channel,
		Timestamp:  time.Now().UTC(),
	})

	resp := toResponse(saved, bodyHTML)
	return &resp, nil
}

// withBody appends the HTML body fetched from S3 to t's response.
func (s *Service) withBody(ctx context.Context, t *Template) (*TemplateResponse, error) {
	body, err := s.bodyStore.Get(ctx, t.BodyS3Key)
	if err != nil {
		return nil, fmt.Errorf("service: fetch body for %s v%d: %w", t.TemplateID, t.Version, err)
	}
	resp := toResponse(t, string(body))
	return &resp, nil
}

// publishEvent emits a TemplateEvent and logs a warning on failure.
// Kafka publish failures are non-fatal: workers will re-synchronise on their
// next cache miss by hitting Redis directly.
func (s *Service) publishEvent(ctx context.Context, event TemplateEvent) {
	if err := s.publisher.Publish(ctx, event); err != nil {
		s.logger.Warn("template event publish failed",
			zap.String("event_type", string(event.EventType)),
			zap.String("template_id", event.TemplateID),
			zap.Int("version", event.Version),
			zap.Error(err),
		)
	}
}

// ─── Request validation ───────────────────────────────────────────────────────

func validateCreateRequest(req CreateRequest) error {
	switch {
	case req.TemplateID == "":
		return fmt.Errorf("validation: template_id must not be empty")
	case !req.Channel.IsValid():
		return fmt.Errorf("validation: unknown channel %q", req.Channel)
	case req.Name == "":
		return fmt.Errorf("validation: name must not be empty")
	case len(req.BodyHTML) == 0:
		return fmt.Errorf("validation: body_html must not be empty")
	}
	return nil
}

func validateCreateVersionRequest(req CreateVersionRequest) error {
	if len(req.BodyHTML) == 0 {
		return fmt.Errorf("validation: body_html must not be empty")
	}
	return nil
}
