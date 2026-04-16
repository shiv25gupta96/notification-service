package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"notification-service/pkg/transport"
)

// Writer wraps kafka-go's *kafka.Writer and implements transport.MessageWriter.
// All service packages (ingestion, template, scheduler, …) accept the
// transport.MessageWriter interface; this adapter is the single concrete
// implementation wired in main.go.
type Writer struct {
	w *kafka.Writer
}

// NewWriter constructs a Writer from cfg and verifies broker reachability.
//
// The underlying kafka.Writer is configured for multi-topic writes (no fixed
// Topic field) using consistent-hash partitioning on message keys, matching
// the per-topic partition strategies described in the HLD.
func NewWriter(cfg ProducerConfig) (*Writer, error) {
	transport_, err := buildTransport(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka writer: build transport: %w", err)
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.Hash{},
		BatchSize:              cfg.BatchSize,
		BatchTimeout:           cfg.BatchTimeout,
		WriteTimeout:           cfg.WriteTimeout,
		RequiredAcks:           kafka.RequiredAcks(cfg.RequiredAcks),
		Compression:            compressionFor(cfg.Compression),
		Transport:              transport_,
		AllowAutoTopicCreation: false,
	}

	// Verify at least one broker is reachable at startup so misconfiguration
	// surfaces immediately rather than on the first production request.
	conn, err := kafka.DialContext(context.Background(), "tcp", cfg.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafka writer: dial broker %s: %w", cfg.Brokers[0], err)
	}
	_ = conn.Close()

	return &Writer{w: w}, nil
}

// Write implements transport.MessageWriter. It translates transport.Envelopes
// into kafka-go Messages and delegates to the underlying writer.
//
// kafka-go's WriteMessages is atomic at the broker level when RequiredAcks=-1:
// either all messages in the batch are ACKed, or an error is returned and the
// caller should abort the enclosing operation and retry.
func (w *Writer) Write(ctx context.Context, envelopes []transport.Envelope) error {
	msgs := make([]kafka.Message, len(envelopes))
	for i, e := range envelopes {
		msgs[i] = kafka.Message{
			Topic: e.Topic,
			Key:   e.Key,
			Value: e.Value,
		}
	}

	if err := w.w.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}
	return nil
}

// Close flushes pending messages and releases the underlying connections.
// Must be called during graceful shutdown.
func (w *Writer) Close() error {
	return w.w.Close()
}

// buildTransport constructs a kafka.Transport with optional TLS and SASL.
// SASL mechanism types (plain.Mechanism, *scram.Mechanism) satisfy the
// sasl.Mechanism interface that kafka.Transport.SASL expects; we assign them
// directly without introducing an intermediate adapter.
func buildTransport(cfg ProducerConfig) (*kafka.Transport, error) {
	t := &kafka.Transport{}

	if cfg.TLSEnabled {
		t.TLS = &tls.Config{MinVersion: tls.VersionTLS13}
	}

	if cfg.SASLMechanism == "" {
		return t, nil
	}

	switch cfg.SASLMechanism {
	case "PLAIN":
		t.SASL = plain.Mechanism{
			Username: cfg.SASLUsername,
			Password: cfg.SASLPassword,
		}
	case "SCRAM-SHA-256":
		mech, err := scram.Mechanism(scram.SHA256, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("kafka writer: SCRAM-SHA-256 mechanism: %w", err)
		}
		t.SASL = mech
	case "SCRAM-SHA-512":
		mech, err := scram.Mechanism(scram.SHA512, cfg.SASLUsername, cfg.SASLPassword)
		if err != nil {
			return nil, fmt.Errorf("kafka writer: SCRAM-SHA-512 mechanism: %w", err)
		}
		t.SASL = mech
	default:
		return nil, fmt.Errorf("kafka writer: unknown SASL mechanism %q", cfg.SASLMechanism)
	}

	return t, nil
}

// compressionFor maps a human-readable codec name to kafka-go's Compression
// type. Unrecognised values default to no compression rather than failing;
// the operator should validate cfg upstream.
func compressionFor(name string) kafka.Compression {
	switch name {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return 0
	}
}
