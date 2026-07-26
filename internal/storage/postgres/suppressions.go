package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// SuppressionRepo is the read-side repository for opt-out suppressions. It satisfies the opt-out
// package's SuppressionLister and ExactChecker structurally (step-061).
type SuppressionRepo struct {
	q *sqlcgen.Queries
}

// NewSuppressionRepo returns the suppressions repository backed by pool.
func NewSuppressionRepo(pool *pgxpool.Pool) *SuppressionRepo {
	return &SuppressionRepo{q: sqlcgen.New(pool)}
}

// ListSuppressions returns every suppression, for seeding the opt-out Bloom snapshot at boot.
func (r *SuppressionRepo) ListSuppressions(ctx context.Context) ([]cp.Suppression, error) {
	rows, err := r.q.ListSuppressions(ctx)
	if err != nil {
		return nil, translate("list suppressions", err)
	}
	out := make([]cp.Suppression, 0, len(rows))
	for _, row := range rows {
		out = append(out, suppressionFromRow(row))
	}
	return out, nil
}

// Create writes a suppression, idempotently: a repeated STOP for the same (scope, scope_id, msisdn)
// does not duplicate (ON CONFLICT DO NOTHING against suppressions_uq). It reports whether a new row
// was inserted (false = already suppressed).
func (r *SuppressionRepo) Create(ctx context.Context, in cp.NewSuppression) (bool, error) {
	n, err := r.q.CreateSuppression(ctx, sqlcgen.CreateSuppressionParams{
		Scope:   string(in.Scope),
		ScopeID: in.ScopeID,
		Msisdn:  in.MSISDN,
		Source:  string(in.Source),
		Reason:  in.Reason,
	})
	if err != nil {
		return false, translate("create suppression", err)
	}
	return n > 0, nil
}

// DeleteByKey removes a suppression by its natural key (an unsuppress / START). It reports whether a
// row was removed (false = there was nothing to remove). scopeID is nil for the platform scope.
func (r *SuppressionRepo) DeleteByKey(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) (bool, error) {
	n, err := r.q.DeleteSuppressionByKey(ctx, sqlcgen.DeleteSuppressionByKeyParams{
		Scope:   string(scope),
		ScopeID: scopeID,
		Msisdn:  msisdn,
	})
	if err != nil {
		return false, translate("delete suppression", err)
	}
	return n > 0, nil
}

// IsSuppressed confirms exactly whether msisdn is suppressed in the given scope, the source of truth
// behind a Bloom hit. scopeID is nil for the platform scope.
func (r *SuppressionRepo) IsSuppressed(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) (bool, error) {
	ok, err := r.q.IsSuppressed(ctx, sqlcgen.IsSuppressedParams{
		Scope:   string(scope),
		ScopeID: scopeID,
		Msisdn:  msisdn,
	})
	if err != nil {
		return false, translate("is suppressed", err)
	}
	return ok, nil
}

func suppressionFromRow(row sqlcgen.ControlPlaneSuppression) cp.Suppression {
	return cp.Suppression{
		ID:        row.ID,
		Scope:     cp.SuppressionScope(row.Scope),
		ScopeID:   row.ScopeID,
		MSISDN:    row.Msisdn,
		Source:    cp.SuppressionSource(row.Source),
		Reason:    row.Reason,
		CreatedAt: tsVal(row.CreatedAt),
	}
}
