// Package transport defines the cross-cutting message types used by every
// service that publishes to Kafka. Keeping Envelope and MessageWriter here
// means the platform/kafka adapter implements a single interface that the
// ingestion and template packages can both accept without creating an import
// cycle or duplicating type definitions.
package transport

import "context"

// Envelope is the transport-layer representation of a single Kafka message.
// It carries the routing metadata (topic, partition key) alongside the
// pre-serialised payload so the writer layer needs no domain knowledge.
type Envelope struct {
	Topic string
	Key   []byte // partition key; service-specific selection logic
	Value []byte // JSON-encoded domain payload
}

// MessageWriter is the minimal I/O contract for emitting a batch of messages.
// All messages in a single Write call are treated as a logical unit: callers
// should abort the enclosing operation if Write returns an error rather than
// retrying individual envelopes.
type MessageWriter interface {
	Write(ctx context.Context, envelopes []Envelope) error
}
