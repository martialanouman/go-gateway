package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// RatePlanRepo is the rate_plans repository (§3.1). It satisfies the admin rate-plan store structurally; the
// interface is declared consumer-side.
type RatePlanRepo struct {
	q *sqlcgen.Queries
}

// NewRatePlanRepo returns the rate-plan repository backed by pool.
func NewRatePlanRepo(pool *pgxpool.Pool) *RatePlanRepo {
	return &RatePlanRepo{q: sqlcgen.New(pool)}
}

// List returns every rate plan, ordered by name.
func (r *RatePlanRepo) List(ctx context.Context) ([]cp.RatePlan, error) {
	rows, err := r.q.ListRatePlans(ctx)
	if err != nil {
		return nil, translate("list rate plans", err)
	}
	out := make([]cp.RatePlan, 0, len(rows))
	for _, row := range rows {
		out = append(out, ratePlanFromRow(row))
	}
	return out, nil
}

// Create inserts a rate plan.
func (r *RatePlanRepo) Create(ctx context.Context, in cp.NewRatePlan) (cp.RatePlan, error) {
	row, err := r.q.CreateRatePlan(ctx, sqlcgen.CreateRatePlanParams{
		Name:                    in.Name,
		CreditsPerSegmentMtJson: in.CreditsPerSegmentMT,
		CreditsPerSegmentMoJson: in.CreditsPerSegmentMO,
		BillingMode:             strPtr(in.BillingMode),
		ChargeOn:                strPtr(in.ChargeOn),
	})
	if err != nil {
		return cp.RatePlan{}, translate("create rate plan", err)
	}
	return ratePlanFromRow(row), nil
}

// Update applies a partial change and returns the updated plan, or ErrNotFound.
func (r *RatePlanRepo) Update(ctx context.Context, id uuid.UUID, p cp.RatePlanPatch) (cp.RatePlan, error) {
	row, err := r.q.UpdateRatePlan(ctx, sqlcgen.UpdateRatePlanParams{
		ID:                      id,
		Name:                    p.Name,
		CreditsPerSegmentMtJson: p.CreditsPerSegmentMT,
		CreditsPerSegmentMoJson: p.CreditsPerSegmentMO,
		BillingMode:             strPtr(p.BillingMode),
		ChargeOn:                strPtr(p.ChargeOn),
		Status:                  strPtr(p.Status),
	})
	if err != nil {
		return cp.RatePlan{}, translate("update rate plan", err)
	}
	return ratePlanFromRow(row), nil
}

// Delete removes a rate plan. A plan still assigned to a customer (customers.rate_plan_id RESTRICT) raises a
// foreign-key violation surfaced as a 409 conflict; a delete matching no row is ErrNotFound.
func (r *RatePlanRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteRatePlan(ctx, id)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation {
			return errs.ErrConflict
		}
		return translate("delete rate plan", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func ratePlanFromRow(row sqlcgen.ControlPlaneRatePlan) cp.RatePlan {
	return cp.RatePlan{
		ID:                  row.ID,
		Name:                row.Name,
		CreditsPerSegmentMT: row.CreditsPerSegmentMtJson,
		CreditsPerSegmentMO: row.CreditsPerSegmentMoJson,
		BillingMode:         row.BillingMode,
		ChargeOn:            row.ChargeOn,
		Status:              row.Status,
		CreatedAt:           tsVal(row.CreatedAt),
		UpdatedAt:           tsVal(row.UpdatedAt),
	}
}
