package ingestion

import "time"

// Channel is a strongly typed delivery channel identifier.
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

// IsValid reports whether c is a recognised channel.
func (c Channel) IsValid() bool {
	_, ok := knownChannels[c]
	return ok
}

// Category drives preference-enforcement policy for a notification.
type Category string

const (
	// CategoryTransactional and CategoryAlert always deliver regardless of
	// user opt-outs or quiet hours.
	CategoryTransactional Category = "TRANSACTIONAL"
	CategoryAlert         Category = "ALERT"

	// CategoryProduct and CategoryMarketing respect user opt-outs and
	// quiet-hour windows.
	CategoryProduct   Category = "PRODUCT"
	CategoryMarketing Category = "MARKETING"
)

var knownCategories = map[Category]struct{}{
	CategoryTransactional: {},
	CategoryAlert:         {},
	CategoryProduct:       {},
	CategoryMarketing:     {},
}

// IsValid reports whether c is a recognised category.
func (c Category) IsValid() bool {
	_, ok := knownCategories[c]
	return ok
}

// EnforcesPreferences reports whether the category requires opt-out and
// quiet-hour checks before delivery.
func (c Category) EnforcesPreferences() bool {
	return c == CategoryProduct || c == CategoryMarketing
}

// DeliveryStatus is the initial queueing state returned to callers.
type DeliveryStatus string

const (
	StatusQueued    DeliveryStatus = "QUEUED"
	StatusScheduled DeliveryStatus = "SCHEDULED"
)

// ChannelRequest is one element of the notifications array in the API payload.
type ChannelRequest struct {
	Channel    Channel           `json:"channel"`
	TemplateID string            `json:"template_id"`
	Variables  map[string]string `json:"variables"`
}

// NotificationRequest is the top-level body for POST /v1/notifications.
type NotificationRequest struct {
	UserID         string           `json:"user_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	Category       Category         `json:"category"`
	ScheduleAt     *time.Time       `json:"schedule_at,omitempty"`
	Notifications  []ChannelRequest `json:"notifications"`
}

// ChannelResponse is the per-channel entry in the 202 response body.
type ChannelResponse struct {
	Channel        Channel        `json:"channel"`
	NotificationID string         `json:"notification_id"`
	Status         DeliveryStatus `json:"status"`
}

// NotificationResponse is the 202 Accepted body.
type NotificationResponse struct {
	ParentNotificationID string            `json:"parent_notification_id"`
	Channels             []ChannelResponse `json:"channels"`
}

// ValidationDetail describes one field-level validation failure.
type ValidationDetail struct {
	Channel    Channel `json:"channel,omitempty"`
	TemplateID string  `json:"template_id,omitempty"`
	Error      string  `json:"error"`
}

// ValidationErrorResponse is the 400 Bad Request body.
type ValidationErrorResponse struct {
	Error   string             `json:"error"`
	Details []ValidationDetail `json:"details"`
}

// TemplateMetadata is the shape the Template Service writes into Redis.
// The ingestion service only reads these entries; it never writes them.
type TemplateMetadata struct {
	ID                string   `json:"id"`
	Channel           Channel  `json:"channel"`
	Version           int      `json:"version"`
	RequiredVariables []string `json:"required_variables"`
	IsActive          bool     `json:"is_active"`
}

// enrichedChannel couples a validated ChannelRequest with the data that
// only becomes available after template resolution: the assigned notification
// ID and the template version to embed in the Kafka envelope.
// It is unexported because it is an implementation detail of the pipeline
// and must not cross the package boundary.
type enrichedChannel struct {
	ChannelRequest
	notificationID  string
	templateVersion int
}

// KafkaEnvelope is the message payload written to every channel Kafka topic.
// Workers deserialise this struct to drive the full delivery pipeline.
type KafkaEnvelope struct {
	NotificationID       string            `json:"notification_id"`
	ParentNotificationID string            `json:"parent_notification_id"`
	UserID               string            `json:"user_id"`
	Channel              Channel           `json:"channel"`
	Category             Category          `json:"category"`
	TemplateID           string            `json:"template_id"`
	TemplateVersion      int               `json:"template_version"`
	Variables            map[string]string `json:"variables"`
	ScheduleAt           *time.Time        `json:"schedule_at,omitempty"`
	IdempotencyKey       string            `json:"idempotency_key"`
	EnqueuedAt           time.Time         `json:"enqueued_at"`
}
