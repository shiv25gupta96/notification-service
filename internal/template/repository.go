package template

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository abstracts all PostgreSQL operations for the template domain.
// The interface is defined here (at point of use) per Go conventions.
type Repository interface {
	// Create inserts a new template row and returns the persisted record.
	Create(ctx context.Context, t *Template) (*Template, error)

	// GetLatestActive returns the highest-version active row for templateID.
	// Returns ErrNotFound when no active row exists.
	GetLatestActive(ctx context.Context, templateID string) (*Template, error)

	// GetByVersion returns the row for templateID at the exact version.
	// Returns ErrNotFound when absent.
	GetByVersion(ctx context.Context, templateID string, version int) (*Template, error)

	// ListActive returns one row per distinct template_id — the highest active
	// version for each — ordered by template_id.
	ListActive(ctx context.Context) ([]*Template, error)

	// DeactivateAll marks every version of templateID as is_active=false.
	DeactivateAll(ctx context.Context, templateID string) error

	// MaxVersion returns the highest version number for templateID, or 0 if no
	// rows exist for that ID yet.
	MaxVersion(ctx context.Context, templateID string) (int, error)
}

// PostgresRepository implements Repository using a pgxpool connection pool.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository creates a repository backed by pool.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Create inserts t into the templates table.
// The caller must set all fields except CreatedAt, which is set by the DB.
func (r *PostgresRepository) Create(ctx context.Context, t *Template) (*Template, error) {
	const q = `
		INSERT INTO templates
			(id, template_id, version, channel, name, subject, body_s3_key, variables, is_active, created_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		RETURNING id, template_id, version, channel, name, subject, body_s3_key, variables, is_active, created_at`

	row := r.pool.QueryRow(ctx, q,
		t.ID,
		t.TemplateID,
		t.Version,
		string(t.Channel),
		t.Name,
		t.Subject,
		t.BodyS3Key,
		t.RequiredVariables, // pgx marshals []string → Postgres JSONB automatically
		t.IsActive,
	)

	out, err := scanTemplate(row)
	if err != nil {
		return nil, fmt.Errorf("repository: create template %q: %w", t.TemplateID, err)
	}
	return out, nil
}

// GetLatestActive returns the highest-version active template for templateID.
func (r *PostgresRepository) GetLatestActive(ctx context.Context, templateID string) (*Template, error) {
	const q = `
		SELECT id, template_id, version, channel, name, subject, body_s3_key, variables, is_active, created_at
		FROM templates
		WHERE template_id = $1 AND is_active = true
		ORDER BY version DESC
		LIMIT 1`

	row := r.pool.QueryRow(ctx, q, templateID)
	t, err := scanTemplate(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, templateID)
	}
	if err != nil {
		return nil, fmt.Errorf("repository: get latest active %q: %w", templateID, err)
	}
	return t, nil
}

// GetByVersion returns the template at the exact version.
func (r *PostgresRepository) GetByVersion(ctx context.Context, templateID string, version int) (*Template, error) {
	const q = `
		SELECT id, template_id, version, channel, name, subject, body_s3_key, variables, is_active, created_at
		FROM templates
		WHERE template_id = $1 AND version = $2`

	row := r.pool.QueryRow(ctx, q, templateID, version)
	t, err := scanTemplate(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s v%d", ErrNotFound, templateID, version)
	}
	if err != nil {
		return nil, fmt.Errorf("repository: get by version %q v%d: %w", templateID, version, err)
	}
	return t, nil
}

// ListActive returns the latest active version for every distinct template_id.
// DISTINCT ON is PostgreSQL-specific; it picks the first row per group after
// the ORDER BY, so DESC on version gives us the highest version per template.
func (r *PostgresRepository) ListActive(ctx context.Context) ([]*Template, error) {
	const q = `
		SELECT DISTINCT ON (template_id)
			id, template_id, version, channel, name, subject, body_s3_key, variables, is_active, created_at
		FROM templates
		WHERE is_active = true
		ORDER BY template_id, version DESC`

	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("repository: list active: %w", err)
	}
	defer rows.Close()

	var templates []*Template
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, fmt.Errorf("repository: list active scan: %w", err)
		}
		templates = append(templates, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: list active rows: %w", err)
	}
	return templates, nil
}

// DeactivateAll sets is_active=false for every version of templateID.
func (r *PostgresRepository) DeactivateAll(ctx context.Context, templateID string) error {
	const q = `UPDATE templates SET is_active = false WHERE template_id = $1`
	if _, err := r.pool.Exec(ctx, q, templateID); err != nil {
		return fmt.Errorf("repository: deactivate %q: %w", templateID, err)
	}
	return nil
}

// MaxVersion returns the highest version for templateID, or 0 if none exist.
func (r *PostgresRepository) MaxVersion(ctx context.Context, templateID string) (int, error) {
	const q = `SELECT COALESCE(MAX(version), 0) FROM templates WHERE template_id = $1`
	var max int
	if err := r.pool.QueryRow(ctx, q, templateID).Scan(&max); err != nil {
		return 0, fmt.Errorf("repository: max version %q: %w", templateID, err)
	}
	return max, nil
}

// ─── Scanner ──────────────────────────────────────────────────────────────────

// pgxScanner is satisfied by both pgx.Row and pgx.Rows, letting scanTemplate
// work for both QueryRow and the scan loop inside ListActive.
type pgxScanner interface {
	Scan(dest ...any) error
}

// scanTemplate reads one row of the full template column set.
// Column order must match every SELECT that uses it.
func scanTemplate(s pgxScanner) (*Template, error) {
	var t Template
	var channel string
	err := s.Scan(
		&t.ID,
		&t.TemplateID,
		&t.Version,
		&channel,
		&t.Name,
		&t.Subject,
		&t.BodyS3Key,
		&t.RequiredVariables,
		&t.IsActive,
		&t.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	t.Channel = Channel(channel)
	return &t, nil
}

// ─── Schema DDL (used in integration tests and migration tooling) ─────────────

// DDL is the CREATE TABLE statement for the templates table.
// In production this is applied by the migration framework (e.g. golang-migrate);
// it is kept here as a single source of truth for integration tests.
const DDL = `
CREATE TABLE IF NOT EXISTS templates (
    id          UUID        PRIMARY KEY,
    template_id VARCHAR(255) NOT NULL,
    version     INT         NOT NULL,
    channel     VARCHAR(16) NOT NULL,
    name        VARCHAR(255) NOT NULL,
    subject     VARCHAR(255) NOT NULL DEFAULT '',
    body_s3_key VARCHAR(512) NOT NULL,
    variables   JSONB       NOT NULL DEFAULT '[]',
    is_active   BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_template_version UNIQUE (template_id, version)
);

CREATE INDEX IF NOT EXISTS idx_templates_template_id_active
    ON templates (template_id, is_active)
    WHERE is_active = true;
`
