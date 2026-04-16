package worker

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditLog persists DeliveryAttempts for observability and compliance.
// It is called unconditionally after every chain execution so the audit trail
// is complete regardless of outcome.
type AuditLog interface {
	Record(ctx context.Context, attempt DeliveryAttempt) error
}

// DDLDeliveryAttempts is the canonical schema for the delivery_attempts table.
// The table is range-partitioned by attempted_at so each monthly partition
// remains small and can be dropped cheaply once the retention window passes.
// Partition creation is managed by the migration tool (or pg_partman);
// the DDL constant here is the source of truth for the parent table schema.
const DDLDeliveryAttempts = `
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id              UUID        NOT NULL,
    notification_id TEXT        NOT NULL,
    parent_id       TEXT        NOT NULL,
    user_id         TEXT        NOT NULL,
    template_id     TEXT        NOT NULL,
    outcome         TEXT        NOT NULL,
    error_message   TEXT        NOT NULL DEFAULT '',
    attempted_at    TIMESTAMPTZ NOT NULL
) PARTITION BY RANGE (attempted_at);

-- Example monthly partition; the migration tool adds new ones before month start.
-- CREATE TABLE delivery_attempts_y2024m01
--     PARTITION OF delivery_attempts
--     FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_notification_id
    ON delivery_attempts (notification_id);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_user_id_attempted_at
    ON delivery_attempts (user_id, attempted_at DESC);
`

// PostgresAuditLog implements AuditLog using a pgxpool connection pool.
type PostgresAuditLog struct {
	pool *pgxpool.Pool
}

// NewPostgresAuditLog creates an audit log backed by pool.
func NewPostgresAuditLog(pool *pgxpool.Pool) *PostgresAuditLog {
	return &PostgresAuditLog{pool: pool}
}

// Record inserts a DeliveryAttempt row into the partitioned table.
// Callers should treat a Record error as non-fatal (log and continue)
// since the notification has already been delivered at this point.
func (a *PostgresAuditLog) Record(ctx context.Context, attempt DeliveryAttempt) error {
	const q = `
		INSERT INTO delivery_attempts
			(id, notification_id, parent_id, user_id, template_id, outcome, error_message, attempted_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := a.pool.Exec(ctx, q,
		attempt.ID,
		attempt.NotificationID,
		attempt.ParentID,
		attempt.UserID,
		attempt.TemplateID,
		string(attempt.Outcome),
		attempt.ErrorMessage,
		attempt.AttemptedAt,
	)
	if err != nil {
		return fmt.Errorf("audit log: insert attempt %s: %w", attempt.NotificationID, err)
	}
	return nil
}
