package template

import (
	"context"
	"encoding/json"
	"fmt"

	"notification-service/pkg/transport"
)

const templateEventsTopic = "template.events"

// EventPublisher is the domain contract for emitting template lifecycle events.
type EventPublisher interface {
	Publish(ctx context.Context, event TemplateEvent) error
}

// KafkaEventPublisher implements EventPublisher by writing to the
// "template.events" Kafka topic. Workers consume this topic to invalidate
// their local in-memory template caches.
type KafkaEventPublisher struct {
	writer transport.MessageWriter
}

// NewKafkaEventPublisher creates a publisher backed by writer.
func NewKafkaEventPublisher(writer transport.MessageWriter) *KafkaEventPublisher {
	return &KafkaEventPublisher{writer: writer}
}

// Publish serialises event and writes it to the template.events topic.
// The message key is the template_id so all events for the same template
// land on the same partition and are consumed in order.
func (p *KafkaEventPublisher) Publish(ctx context.Context, event TemplateEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("event publisher: marshal event: %w", err)
	}

	return p.writer.Write(ctx, []transport.Envelope{{
		Topic: templateEventsTopic,
		Key:   []byte(event.TemplateID),
		Value: value,
	}})
}
