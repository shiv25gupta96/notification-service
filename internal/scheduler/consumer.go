package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	topicScheduled    = "notif.scheduled"
	consumerGroupID   = "scheduler-consumer"
	maxFetchBytes     = 10 << 20 // 10 MiB
	commitAfterMillis = 500
)

// MessageReader abstracts kafka-go's *kafka.Reader for testability.
// The minimal surface covers the operations used by Consumer.
type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Consumer reads ScheduledMessages from the notif.scheduled Kafka topic and
// persists them to the ScheduleStore for later dispatch by the Poller.
//
// Each message is committed only after a successful Enqueue so that a crash
// between read and store causes the message to be re-delivered rather than
// silently dropped.
type Consumer struct {
	reader MessageReader
	store  ScheduleStore
	logger *zap.Logger
}

// NewConsumer creates a Consumer. The caller retains ownership of reader and
// must call reader.Close() after Run returns.
func NewConsumer(reader MessageReader, store ScheduleStore, logger *zap.Logger) *Consumer {
	return &Consumer{
		reader: reader,
		store:  store,
		logger: logger,
	}
}

// NewKafkaReader constructs a kafka-go Reader configured for the scheduler
// consumer group. Callers pass it to NewConsumer.
func NewKafkaReader(brokers []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topicScheduled,
		GroupID:        consumerGroupID,
		MaxBytes:       maxFetchBytes,
		CommitInterval: commitAfterMillis * time.Millisecond,
		// StartOffset is kafka.LastOffset by default; we only care about new
		// messages. In a fresh deployment this avoids re-processing the entire
		// topic history.
	})
}

// Run processes messages until ctx is cancelled. It returns only when the
// context is done or an unrecoverable reader error occurs (e.g. closed reader).
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("scheduler consumer started", zap.String("topic", topicScheduled))
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
				c.logger.Info("scheduler consumer shutting down")
				return nil
			}
			return fmt.Errorf("consumer: read message: %w", err)
		}

		if err := c.handle(ctx, msg); err != nil {
			// Log and continue — a single bad message must not stall the consumer.
			// The message will be re-delivered when the commit is withheld and
			// the consumer restarts, so persistent decode failures require
			// operator intervention (dead-letter routing is out of scope here).
			c.logger.Error("consumer: handle failed, skipping",
				zap.Error(err),
				zap.Int64("offset", msg.Offset),
				zap.String("partition", fmt.Sprintf("%d", msg.Partition)),
			)
			continue
		}

		// CommitMessages acknowledges that the message has been persisted to
		// the schedule store. kafka-go Reader with CommitInterval handles
		// batching automatically; this call marks the offset as ready.
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Warn("consumer: commit failed",
				zap.Error(err),
				zap.Int64("offset", msg.Offset),
			)
			// Non-fatal: the consumer will re-process on restart, and Enqueue
			// is idempotent (Redis ZADD updates the score on duplicate member).
		}
	}
}

// handle decodes a Kafka message, validates the ScheduleAt field, and persists
// the message to the schedule store.
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var scheduled ScheduledMessage
	if err := json.Unmarshal(msg.Value, &scheduled); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	if scheduled.ScheduleAt == nil {
		return fmt.Errorf("notification %q has nil schedule_at on notif.scheduled topic",
			scheduled.NotificationID)
	}
	if scheduled.NotificationID == "" {
		return fmt.Errorf("notification_id is empty")
	}

	sendAt := *scheduled.ScheduleAt
	if err := c.store.Enqueue(ctx, scheduled, sendAt); err != nil {
		return fmt.Errorf("enqueue %q (send_at=%s): %w",
			scheduled.NotificationID, sendAt.Format(time.RFC3339), err)
	}

	c.logger.Debug("scheduled message enqueued",
		zap.String("notification_id", scheduled.NotificationID),
		zap.String("send_at", sendAt.Format(time.RFC3339)),
		zap.String("channel", string(scheduled.Channel)),
	)
	return nil
}
