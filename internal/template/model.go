package template

import (
	"fmt"
	"time"
)

// Channel mirrors the ingestion package's Channel type so the template
// package compiles independently. The JSON representations are identical,
// which is the only coupling that matters for the Redis contract.
type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelSlack Channel = "slack"
	ChannelInApp  Channel = "inapp"
)

var knownChannels = map[Channel]struct{}{
	ChannelEmail: {},
	ChannelSlack: {},
	ChannelInApp:  {},
}

func (c Channel) IsValid() bool {
	_, ok := knownChannels[c]
	return ok
}

// EventType classifies a template lifecycle change published to Kafka.
type EventType string

const (
	EventCreated     EventType = "TEMPLATE_CREATED"
	EventVersioned   EventType = "VERSION_CREATED"
	EventDeactivated EventType = "TEMPLATE_DEACTIVATED"
)

// Template is the canonical domain object and mirrors the PostgreSQL row.
// BodyS3Key holds the object key for the full HTML body stored in S3;
// the body is never persisted in the database.
type Template struct {
	ID                string    `db:"id"`
	TemplateID        string    `db:"template_id"`
	Version           int       `db:"version"`
	Channel           Channel   `db:"channel"`
	Name              string    `db:"name"`
	Subject           string    `db:"subject"`    // email only; empty for other channels
	BodyS3Key         string    `db:"body_s3_key"`
	RequiredVariables []string  `db:"variables"`   // stored as JSONB in Postgres
	IsActive          bool      `db:"is_active"`
	CreatedAt         time.Time `db:"created_at"`
}

// CacheEntry is what the Template Service writes into Redis under the key
// "template:{template_id}". It must be JSON-compatible with the
// TemplateMetadata struct in internal/ingestion/model.go — any field rename
// here requires a matching change there.
//
// Contract: key = "template:" + template_id, no TTL (this service owns expiry).
type CacheEntry struct {
	ID                string   `json:"id"`
	Channel           Channel  `json:"channel"`
	Version           int      `json:"version"`
	RequiredVariables []string `json:"required_variables"`
	IsActive          bool     `json:"is_active"`
}

// TemplateEvent is the payload published to the "template.events" Kafka topic
// whenever a template changes. Workers consume this topic to invalidate their
// local in-memory caches.
type TemplateEvent struct {
	EventType  EventType `json:"event_type"`
	TemplateID string    `json:"template_id"`
	Version    int       `json:"version"`
	Channel    Channel   `json:"channel"`
	Timestamp  time.Time `json:"timestamp"`
}

// ─── API request/response types ──────────────────────────────────────────────

// CreateRequest is the body for POST /v1/templates.
type CreateRequest struct {
	TemplateID        string   `json:"template_id"`
	Channel           Channel  `json:"channel"`
	Name              string   `json:"name"`
	Subject           string   `json:"subject"`
	BodyHTML          string   `json:"body_html"`
	RequiredVariables []string `json:"required_variables"`
}

// CreateVersionRequest is the body for POST /v1/templates/{id}/versions.
// TemplateID and Channel are immutable between versions; only content changes.
type CreateVersionRequest struct {
	Subject           string   `json:"subject"`
	BodyHTML          string   `json:"body_html"`
	RequiredVariables []string `json:"required_variables"`
}

// TemplateResponse is the HTTP response body for create and get operations.
// BodyHTML is populated only by GET endpoints (list omits it for brevity).
type TemplateResponse struct {
	ID                string    `json:"id"`
	TemplateID        string    `json:"template_id"`
	Version           int       `json:"version"`
	Channel           Channel   `json:"channel"`
	Name              string    `json:"name"`
	Subject           string    `json:"subject,omitempty"`
	RequiredVariables []string  `json:"required_variables"`
	IsActive          bool      `json:"is_active"`
	CreatedAt         time.Time `json:"created_at"`
	BodyHTML          string    `json:"body_html,omitempty"` // omitted in list responses
}

// ListResponse wraps a slice of TemplateResponse for the list endpoint.
type ListResponse struct {
	Templates []TemplateResponse `json:"templates"`
	Total     int                `json:"total"`
}

// ErrorResponse is the standard error body.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// ─── Domain errors ────────────────────────────────────────────────────────────

// ErrNotFound is returned when a template cannot be located in the repository.
const ErrNotFound = sentinelError("template not found")

// ErrAlreadyExists is returned when a template_id already exists on create.
const ErrAlreadyExists = sentinelError("template already exists")

// ErrInactiveTemplate is returned when attempting to version a deactivated template.
const ErrInactiveTemplate = sentinelError("template is inactive")

type sentinelError string

func (e sentinelError) Error() string { return string(e) }

// ─── Helpers ──────────────────────────────────────────────────────────────────

// toResponse converts a Template (plus an optional fetched body) into the HTTP
// response shape.
func toResponse(t *Template, bodyHTML string) TemplateResponse {
	return TemplateResponse{
		ID:                t.ID,
		TemplateID:        t.TemplateID,
		Version:           t.Version,
		Channel:           t.Channel,
		Name:              t.Name,
		Subject:           t.Subject,
		RequiredVariables: t.RequiredVariables,
		IsActive:          t.IsActive,
		CreatedAt:         t.CreatedAt,
		BodyHTML:          bodyHTML,
	}
}

// toCacheEntry projects a Template into the minimal shape written to Redis.
func toCacheEntry(t *Template) *CacheEntry {
	return &CacheEntry{
		ID:                t.TemplateID,
		Channel:           t.Channel,
		Version:           t.Version,
		RequiredVariables: t.RequiredVariables,
		IsActive:          t.IsActive,
	}
}

// s3Key returns the deterministic S3 object key for the HTML body of a
// given template version.
func s3Key(templateID string, version int) string {
	return fmt.Sprintf("templates/%s/v%d", templateID, version)
}
