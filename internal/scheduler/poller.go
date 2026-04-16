package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"notification-service/pkg/transport"
)

const (
	pollInterval = time.Second
	// maxPollBatch caps the number of due messages processed per tick to
	// prevent a single catch-up burst from exhausting Kafka write buffers.
	// Messages that exceed the cap remain in the sorted set and are picked up
	// on the next tick.
	maxPollBatch = 500
)

// Poller runs a fixed-interval loop that drains due messages from the
// ScheduleStore and publishes them to the appropriate channel Kafka topics.
//
// On each tick Poller:
//  1. Calls store.Poll with the current wall-clock time.
//  2. Builds one transport.Envelope per message, routing to notif.{channel}.
//  3. Writes all envelopes as a single batch (all-or-none Kafka semantics).
//  4. Removes successfully published messages from the store.
//
// If the Kafka write fails the messages remain in the sorted set and are
// retried on the next tick. Workers deduplicate by notification_id so
// at-least-once delivery here is safe.
type Poller struct {
	store    ScheduleStore
	writer   transport.MessageWriter
	logger   *zap.Logger
	interval time.Duration // overridden in tests
}

// NewPoller creates a Poller.
func NewPoller(store ScheduleStore, writer transport.MessageWriter, logger *zap.Logger) *Poller {
	return &Poller{
		store:    store,
		writer:   writer,
		logger:   logger,
		interval: pollInterval,
	}
}

// Run ticks at p.interval until ctx is cancelled. It always returns nil;
// errors are logged and the loop continues.
func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.logger.Info("scheduler poller started", zap.Duration("interval", p.interval))
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("scheduler poller shutting down")
			return nil
		case t := <-ticker.C:
			p.tick(ctx, t)
		}
	}
}

// tick performs one poll-and-publish cycle.
func (p *Poller) tick(ctx context.Context, now time.Time) {
	msgs, err := p.store.Poll(ctx, now)
	if err != nil {
		p.logger.Error("poller: poll failed", zap.Error(err))
		return
	}
	if len(msgs) == 0 {
		return
	}

	// Cap to maxPollBatch so a large backlog does not cause a single
	// oversized Kafka write.
	if len(msgs) > maxPollBatch {
		p.logger.Warn("poller: batch capped",
			zap.Int("due", len(msgs)),
			zap.Int("cap", maxPollBatch),
		)
		msgs = msgs[:maxPollBatch]
	}

	envelopes, err := buildEnvelopes(msgs)
	if err != nil {
		p.logger.Error("poller: build envelopes failed", zap.Error(err))
		return
	}

	if err := p.writer.Write(ctx, envelopes); err != nil {
		p.logger.Error("poller: kafka write failed — messages remain in queue for retry",
			zap.Error(err),
			zap.Int("count", len(msgs)),
		)
		return
	}

	// Remove only after a confirmed write so messages are never silently
	// dropped on a partial Kafka failure.
	if err := p.store.Remove(ctx, msgs); err != nil {
		// Non-fatal: the messages will be re-published on the next tick.
		// Workers handle duplicate notification_ids idempotently.
		p.logger.Warn("poller: remove after publish failed — expect duplicate delivery",
			zap.Error(err),
			zap.Int("count", len(msgs)),
		)
		return
	}

	p.logger.Info("poller: dispatched scheduled notifications",
		zap.Int("count", len(msgs)),
		zap.Time("tick", now),
	)
}

// buildEnvelopes converts ScheduledMessages into transport.Envelopes routed
// to their respective channel topics. The partition key is user_id, matching
// the strategy used for immediate notifications, so per-user ordering is
// preserved across both immediate and deferred delivery.
func buildEnvelopes(msgs []ScheduledMessage) ([]transport.Envelope, error) {
	envelopes := make([]transport.Envelope, len(msgs))
	for i, msg := range msgs {
		value, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("marshal msg[%d] (%s): %w", i, msg.NotificationID, err)
		}
		envelopes[i] = transport.Envelope{
			Topic: topicFor(msg.Channel),
			Key:   []byte(msg.UserID),
			Value: value,
		}
	}
	return envelopes, nil
}
