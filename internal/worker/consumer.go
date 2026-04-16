package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	topicEmail    = "notif.email"
	consumerGroup = "worker-email"
	maxFetchBytes = 10 << 20 // 10 MiB
)

// MessageReader abstracts kafka-go's *kafka.Reader for testability.
type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Consumer reads email notifications from Kafka and drives them through the
// processing Chain. Each message is committed only after the chain completes —
// including on OutcomeFailed — so that the audit record is always written
// before the offset advances.
type Consumer struct {
	reader MessageReader
	chain  *Chain
	logger *zap.Logger
}

// NewConsumer creates a Consumer. The caller owns the reader lifecycle.
func NewConsumer(reader MessageReader, chain *Chain, logger *zap.Logger) *Consumer {
	return &Consumer{reader: reader, chain: chain, logger: logger}
}

// NewKafkaReader constructs a kafka-go Reader for the email worker consumer group.
func NewKafkaReader(brokers []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topicEmail,
		GroupID:  consumerGroup,
		MaxBytes: maxFetchBytes,
	})
}

// Run processes messages until ctx is cancelled or a non-recoverable reader
// error occurs. Individual message failures are logged and do not stop the loop.
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("worker consumer started",
		zap.String("topic", topicEmail),
		zap.String("group", consumerGroup),
	)

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, io.EOF) {
				c.logger.Info("worker consumer shutting down")
				return nil
			}
			return fmt.Errorf("worker consumer: read: %w", err)
		}

		item, err := decode(msg.Value)
		if err != nil {
			c.logger.Error("worker consumer: decode failed — skipping message",
				zap.Error(err),
				zap.Int64("offset", msg.Offset),
			)
			// Commit the offset so a corrupt message does not stall the
			// consumer. Operational alerting should detect a rise in decode
			// errors before they become a widespread issue.
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		c.chain.Process(ctx, item)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Warn("worker consumer: commit failed",
				zap.Error(err),
				zap.String("notification_id", item.NotificationID),
			)
		}
	}
}

// decode unmarshals the raw Kafka message bytes into a WorkItem.
func decode(raw []byte) (*WorkItem, error) {
	var m kafkaMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	if m.NotificationID == "" {
		return nil, fmt.Errorf("notification_id is empty")
	}
	return m.toWorkItem(), nil
}
