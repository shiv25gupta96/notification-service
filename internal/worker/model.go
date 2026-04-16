package worker

import "time"

// DeliveryOutcome is the final result of one worker processing run.
type DeliveryOutcome string

const (
	OutcomeDelivered  DeliveryOutcome = "DELIVERED"
	OutcomeFailed     DeliveryOutcome = "FAILED"
	OutcomeSuppressed DeliveryOutcome = "SUPPRESSED" // user opted out or already delivered
)

// WorkItem carries all state for a single notification as it moves through
// the processing chain. Immutable input fields are populated from the Kafka
// message; mutable fields are written by individual steps.
type WorkItem struct {
	// Immutable — set once from the Kafka message.
	NotificationID       string
	ParentNotificationID string
	UserID               string
	TemplateID           string
	TemplateVersion      int
	Variables            map[string]string
	IdempotencyKey       string
	EnqueuedAt           time.Time

	// Written by TemplateStep after a successful fetch + render.
	RenderedSubject string
	RenderedBody    string

	// Written by the Chain after all steps complete (or on first step failure).
	Outcome DeliveryOutcome
}

// DeliveryAttempt is the immutable audit record persisted to PostgreSQL
// after every processing run, regardless of outcome.
type DeliveryAttempt struct {
	ID             string
	NotificationID string
	ParentID       string
	UserID         string
	TemplateID     string
	Outcome        DeliveryOutcome
	ErrorMessage   string // empty string on success
	AttemptedAt    time.Time
}

// kafkaMessage is the wire format consumed from notif.email.
// Field names and JSON tags are intentionally identical to
// ingestion.KafkaEnvelope so the same bytes are directly unmarshal-able.
type kafkaMessage struct {
	NotificationID       string            `json:"notification_id"`
	ParentNotificationID string            `json:"parent_notification_id"`
	UserID               string            `json:"user_id"`
	TemplateID           string            `json:"template_id"`
	TemplateVersion      int               `json:"template_version"`
	Variables            map[string]string `json:"variables"`
	IdempotencyKey       string            `json:"idempotency_key"`
	EnqueuedAt           time.Time         `json:"enqueued_at"`
}

func (m kafkaMessage) toWorkItem() *WorkItem {
	return &WorkItem{
		NotificationID:       m.NotificationID,
		ParentNotificationID: m.ParentNotificationID,
		UserID:               m.UserID,
		TemplateID:           m.TemplateID,
		TemplateVersion:      m.TemplateVersion,
		Variables:            m.Variables,
		IdempotencyKey:       m.IdempotencyKey,
		EnqueuedAt:           m.EnqueuedAt,
	}
}
