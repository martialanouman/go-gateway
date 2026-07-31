package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// ContentAccessAuditRepo appends the content-access audit trail (control_plane.content_access_audit, §14): one
// row per content:read access, recording who read which message and the outcome — never the plaintext.
type ContentAccessAuditRepo struct {
	q *sqlcgen.Queries
}

// NewContentAccessAuditRepo returns the audit repository backed by pool.
func NewContentAccessAuditRepo(pool *pgxpool.Pool) *ContentAccessAuditRepo {
	return &ContentAccessAuditRepo{q: sqlcgen.New(pool)}
}

// Record appends one audited content:read access.
func (r *ContentAccessAuditRepo) Record(ctx context.Context, a cp.ContentAccess) error {
	if err := r.q.InsertContentAccess(ctx, sqlcgen.InsertContentAccessParams{
		Operator:   a.Operator,
		MessageID:  a.MessageID,
		CustomerID: a.CustomerID,
		Outcome:    string(a.Outcome),
	}); err != nil {
		return translate("record content access", err)
	}
	return nil
}
