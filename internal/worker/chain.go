package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ErrSuppressed is returned by a Step to short-circuit the chain without
// recording a failure. It covers two cases:
//   - The user has opted out of this notification category.
//   - The notification was already delivered (idempotency guard).
var ErrSuppressed = errors.New("notification suppressed")

// Step is a single processing stage in the worker pipeline.
// Returning nil advances to the next step.
// Returning ErrSuppressed halts the chain and records OutcomeSuppressed.
// Returning any other error halts the chain and records OutcomeFailed.
type Step interface {
	// Name returns a short identifier used in logs and metrics.
	Name() string
	// Execute runs the step's logic against item, which may be mutated.
	Execute(ctx context.Context, item *WorkItem) error
}

// Chain executes a fixed sequence of Steps and writes a DeliveryAttempt audit
// record after every run. The audit write is always attempted regardless of
// how earlier steps resolved.
type Chain struct {
	steps    []Step
	auditLog AuditLog
	logger   *zap.Logger
}

// NewChain creates a Chain. Steps are executed in the order provided.
func NewChain(steps []Step, auditLog AuditLog, logger *zap.Logger) *Chain {
	return &Chain{steps: steps, auditLog: auditLog, logger: logger}
}

// Process runs all steps for item, sets item.Outcome, and records the audit.
// It never returns an error; failures are logged and reflected in the audit row.
func (c *Chain) Process(ctx context.Context, item *WorkItem) {
	var chainErr error

	for _, step := range c.steps {
		if err := step.Execute(ctx, item); err != nil {
			if errors.Is(err, ErrSuppressed) {
				item.Outcome = OutcomeSuppressed
			} else {
				item.Outcome = OutcomeFailed
				chainErr = err
				c.logger.Error("worker step failed",
					zap.String("step", step.Name()),
					zap.String("notification_id", item.NotificationID),
					zap.Error(err),
				)
			}
			break
		}
	}

	if item.Outcome == "" {
		item.Outcome = OutcomeDelivered
	}

	attempt := DeliveryAttempt{
		ID:             uuid.New().String(),
		NotificationID: item.NotificationID,
		ParentID:       item.ParentNotificationID,
		UserID:         item.UserID,
		TemplateID:     item.TemplateID,
		Outcome:        item.Outcome,
		AttemptedAt:    time.Now().UTC(),
	}
	if chainErr != nil {
		attempt.ErrorMessage = fmt.Sprintf("%s: %s", item.Outcome, chainErr.Error())
	}

	if err := c.auditLog.Record(ctx, attempt); err != nil {
		// Non-fatal: the notification has already been dispatched at this point.
		c.logger.Error("audit log write failed",
			zap.String("notification_id", item.NotificationID),
			zap.Error(err),
		)
	}
}
