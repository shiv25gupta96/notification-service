package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"notification-service/pkg/transport"
)

// Kafka topic constants. These mirror the topic names defined in the HLD and
// must stay in sync with the IaC that provisions the kafka cluster.
const (
	topicEmail     = "notif.email"
	topicSlack     = "notif.slack"
	topicInApp     = "notif.inapp"
	topicScheduled = "notif.scheduled"
)

// channelTopic maps each known Channel to its Kafka topic for immediate
// (non-scheduled) delivery.
var channelTopic = map[Channel]string{
	ChannelEmail: topicEmail,
	ChannelSlack: topicSlack,
	ChannelInApp:  topicInApp,
}

// publishInput bundles all data needed for a single publish operation.
// It is unexported because it is a pipeline-internal transfer object.
type publishInput struct {
	parentNotificationID string
	userID               string
	category             Category
	idempotencyKey       string
	scheduleAt           *time.Time
	channels             []enrichedChannel // notificationID must be set by caller
}

// Publisher constructs Kafka envelopes from ingestion domain objects and
// delegates the actual I/O to a transport.MessageWriter.
type Publisher struct {
	writer transport.MessageWriter
}

// NewPublisher creates a Publisher backed by writer.
func NewPublisher(writer transport.MessageWriter) *Publisher {
	return &Publisher{writer: writer}
}

// Publish emits one Kafka message per channel in input.channels.
//
// Routing rules:
//   - scheduleAt != nil (future)  → topicScheduled regardless of channel
//   - scheduleAt == nil or past   → channel-specific topic
//
// The entire batch is written in a single MessageWriter.Write call. If that
// call returns an error the caller must treat the full request as unpublished.
func (p *Publisher) Publish(ctx context.Context, input publishInput) error {
	now := time.Now().UTC()
	envelopes := make([]transport.Envelope, 0, len(input.channels))

	for _, ch := range input.channels {
		env, err := p.buildEnvelope(ch, input, now)
		if err != nil {
			return fmt.Errorf("publisher: build envelope for channel %s: %w", ch.Channel, err)
		}
		envelopes = append(envelopes, env)
	}

	if err := p.writer.Write(ctx, envelopes); err != nil {
		return fmt.Errorf("publisher: write messages: %w", err)
	}
	return nil
}

// buildEnvelope serialises one channel's KafkaEnvelope and wraps it in a
// transport.Envelope with the correct topic and partition key.
func (p *Publisher) buildEnvelope(ch enrichedChannel, input publishInput, now time.Time) (transport.Envelope, error) {
	payload := KafkaEnvelope{
		NotificationID:       ch.notificationID,
		ParentNotificationID: input.parentNotificationID,
		UserID:               input.userID,
		Channel:              ch.Channel,
		Category:             input.category,
		TemplateID:           ch.TemplateID,
		TemplateVersion:      ch.templateVersion,
		Variables:            ch.Variables,
		ScheduleAt:           input.scheduleAt,
		IdempotencyKey:       input.idempotencyKey,
		EnqueuedAt:           now,
	}

	value, err := json.Marshal(payload)
	if err != nil {
		return transport.Envelope{}, fmt.Errorf("marshal KafkaEnvelope: %w", err)
	}

	topic := p.routeTopic(ch.Channel, input.scheduleAt)

	return transport.Envelope{
		Topic: topic,
		Key:   partitionKey(ch.Channel, input.userID, input.scheduleAt),
		Value: value,
	}, nil
}

// routeTopic returns the Kafka topic for a channel considering whether the
// notification is scheduled for future delivery.
func (p *Publisher) routeTopic(ch Channel, scheduleAt *time.Time) string {
	if scheduleAt != nil && scheduleAt.After(time.Now().UTC()) {
		return topicScheduled
	}
	topic, ok := channelTopic[ch]
	if !ok {
		// Should never reach here after validation; guard defensively.
		return "notif.dlq"
	}
	return topic
}

// partitionKey determines the Kafka message key used for partition assignment.
//
// Partition strategy:
//   - email / inapp → user_id  — preserves per-user ordering
//   - slack         → user_id  — workspace_id is resolved by the Slack worker
//   - scheduled     → send_at epoch string — collocates same-time notifications
func partitionKey(ch Channel, userID string, scheduleAt *time.Time) []byte {
	if scheduleAt != nil && scheduleAt.After(time.Now().UTC()) {
		return []byte(strconv.FormatInt(scheduleAt.Unix(), 10))
	}
	return []byte(userID)
}
