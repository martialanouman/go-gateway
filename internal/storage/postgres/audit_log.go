package postgres

import (
	"context"
	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// AuditLogRepo appends the consolidated operator audit trail (control_plane.audit_log, step-290c).
type AuditLogRepo struct {
	q *sqlcgen.Queries
}

// NewAuditLogRepo returns the audit-log repository backed by pool.
func NewAuditLogRepo(pool *pgxpool.Pool) *AuditLogRepo {
	return &AuditLogRepo{q: sqlcgen.New(pool)}
}

// Begin records a request before its handler runs and returns the row to complete with Finish.
func (r *AuditLogRepo) Begin(ctx context.Context, in cp.AuditIntent) (uuid.UUID, error) {
	var reqID *string
	if in.RequestID != "" {
		reqID = &in.RequestID
	}
	id, err := r.q.InsertAuditIntent(ctx, sqlcgen.InsertAuditIntentParams{
		Operator:    in.Operator,
		OperationID: in.OperationID,
		Method:      in.Method,
		Target:      in.Target,
		RequestID:   reqID,
	})
	if err != nil {
		return uuid.Nil, translate("record audit intent", err)
	}
	return id, nil
}

// Finish records the HTTP status of an audited request. It writes once: a row that already has an outcome
// keeps it. A status outside the smallint range leaves the outcome unrecorded (NULL).
func (r *AuditLogRepo) Finish(ctx context.Context, id uuid.UUID, status int) error {
	if status < 0 || status > math.MaxInt16 {
		return nil
	}
	s := int16(status)
	if err := r.q.FinishAudit(ctx, sqlcgen.FinishAuditParams{ID: id, Status: &s}); err != nil {
		return translate("record audit outcome", err)
	}
	return nil
}
