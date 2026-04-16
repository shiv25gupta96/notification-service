package scheduler

import "time"

// Channel mirrors the delivery channel type used across services.
// Redeclared here so the scheduler package compiles without importing ingestion.
// The JSON representations are identical — that is the only coupling that matters.
type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelSlack Channel = "slack"
	ChannelInApp  Channel = "inapp"
)

// topicFor returns the Kafka topic name for immediate delivery of a scheduled
// message once its send_at time has elapsed.
func topicFor(ch Channel) string {
	return "notif." + string(ch)
}

// Category mirrors the notification category type used across services.
type Category string

const (
	CategoryTransactional Category = "TRANSACTIONAL"
	CategoryAlert         Category = "ALERT"
	CategoryProduct       Category = "PRODUCT"
	CategoryMarketing     Category = "MARKETING"
)

// scheduleQueueKey is the Redis sorted-set key that backs the schedule queue.
// Score  = Unix timestamp (seconds) of the intended delivery time.
// Member = JSON-serialised ScheduledMessage (deterministic: encoding/json sorts
//
//	map keys alphabetically, making re-serialisation byte-for-byte stable).
const scheduleQueueKey = "scheduler:queue"

// ScheduledMessage is the representation of a notification deferred for future
// delivery. Its JSON field names and types are identical to KafkaEnvelope in
// internal/ingestion/model.go so that a message written to the notif.scheduled
// Kafka topic can be unmarshalled directly into this struct.
type ScheduledMessage struct {
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
