package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
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

// CreateReturning writes a suppression and returns it (Admin, step-064). Unlike the idempotent MO
// STOP write, a duplicate violates suppressions_uq and surfaces as ErrConflict (409).
func (r *SuppressionRepo) CreateReturning(ctx context.Context, in cp.NewSuppression) (cp.Suppression, error) {
	row, err := r.q.CreateSuppressionReturning(ctx, sqlcgen.CreateSuppressionReturningParams{
		Scope:   string(in.Scope),
		ScopeID: in.ScopeID,
		Msisdn:  in.MSISDN,
		Source:  string(in.Source),
		Reason:  in.Reason,
	})
	if err != nil {
		return cp.Suppression{}, translate("create suppression", err)
	}
	return suppressionFromRow(row), nil
}

// ListPage returns a filtered, keyset-paginated page of suppressions (Admin, step-064).
func (r *SuppressionRepo) ListPage(ctx context.Context, f cp.SuppressionFilter) (cp.Page[cp.Suppression], error) {
	rows, err := r.q.ListSuppressionsPage(ctx, sqlcgen.ListSuppressionsPageParams{
		Scope:   strPtr(f.Scope),
		ScopeID: f.ScopeID,
		Msisdn:  f.MSISDN,
		After:   afterPtr(f.After),
		//nolint:gosec // G115: Limit is capped at 500 by the API, so +1 cannot overflow int32.
		Lim: int32(f.Limit) + 1,
	})
	if err != nil {
		return cp.Page[cp.Suppression]{}, translate("list suppressions", err)
	}
	items := make([]cp.Suppression, 0, len(rows))
	for _, row := range rows {
		items = append(items, suppressionFromRow(row))
	}
	return paginate(items, f.Limit, func(s cp.Suppression) uuid.UUID { return s.ID }), nil
}

// DeleteByID removes a suppression by id (Admin un-suppress, step-064), or reports ErrNotFound.
func (r *SuppressionRepo) DeleteByID(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteSuppressionByID(ctx, id)
	if err != nil {
		return translate("delete suppression", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// Import bulk-inserts suppressions idempotently (Admin, step-064): duplicates are skipped. It returns
// the count of NEW rows inserted. A non-canonical msisdn fails the batch (the CHECK).
func (r *SuppressionRepo) Import(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, source cp.SuppressionSource, msisdns []string) (int64, error) {
	n, err := r.q.ImportSuppressions(ctx, sqlcgen.ImportSuppressionsParams{
		Scope:   string(scope),
		ScopeID: scopeID,
		Source:  string(source),
		Msisdns: msisdns,
	})
	if err != nil {
		return 0, translate("import suppressions", err)
	}
	return n, nil
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
