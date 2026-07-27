package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// ExactRouteRepo persists exact-number routes (control_plane.exact_routes) — the L0 routing overrides
// keyed by MSISDN (ported numbers / MNP).
type ExactRouteRepo struct {
	q *sqlcgen.Queries
}

// NewExactRouteRepo returns the exact-routes repository backed by pool.
func NewExactRouteRepo(pool *pgxpool.Pool) *ExactRouteRepo {
	return &ExactRouteRepo{q: sqlcgen.New(pool)}
}

// Get returns the exact route for an MSISDN. found is false when none is configured — a normal "no
// override, resolve normally" state, not an error.
func (r *ExactRouteRepo) Get(ctx context.Context, msisdn string) (exact.Route, bool, error) {
	row, err := r.q.GetExactRoute(ctx, msisdn)
	if errors.Is(err, pgx.ErrNoRows) {
		return exact.Route{}, false, nil
	}
	if err != nil {
		return exact.Route{}, false, translate("get exact route", err)
	}
	return toExactRoute(row), true, nil
}

// List returns a page of exact routes in msisdn order, after the `after` cursor (the empty string for
// the first page). It returns at most limit rows; callers request limit+1 to detect a further page.
func (r *ExactRouteRepo) List(ctx context.Context, after string, limit int) ([]exact.Route, error) {
	rows, err := r.q.ListExactRoutes(ctx, sqlcgen.ListExactRoutesParams{
		After: after,
		Lim:   int32(limit), //nolint:gosec // a page size is a small positive integer
	})
	if err != nil {
		return nil, translate("list exact routes", err)
	}
	out := make([]exact.Route, len(rows))
	for i, row := range rows {
		out[i] = toExactRoute(row)
	}
	return out, nil
}

// Upsert creates or overwrites the exact route for its MSISDN. It is idempotent by msisdn (the primary
// key): the same route upserted twice yields the same row.
func (r *ExactRouteRepo) Upsert(ctx context.Context, route exact.Route) (exact.Route, error) {
	row, err := r.q.UpsertExactRoute(ctx, sqlcgen.UpsertExactRouteParams{
		Msisdn:     route.MSISDN,
		TargetType: string(route.Target.Type),
		TargetID:   route.Target.ID,
		Source:     string(route.Source),
		ImportedAt: tsFromPtr(route.ImportedAt),
	})
	if err != nil {
		return exact.Route{}, translate("upsert exact route", err)
	}
	return toExactRoute(row), nil
}

// Delete removes an exact route. found is false when no row matched the MSISDN.
func (r *ExactRouteRepo) Delete(ctx context.Context, msisdn string) (bool, error) {
	n, err := r.q.DeleteExactRoute(ctx, msisdn)
	if err != nil {
		return false, translate("delete exact route", err)
	}
	return n > 0, nil
}

// BulkUpsert creates or overwrites many exact routes in one pgx.Batch (an MNP / carrier-feed import).
// It is idempotent by msisdn, so re-importing the same feed is safe. An empty slice is a no-op.
func (r *ExactRouteRepo) BulkUpsert(ctx context.Context, routes []exact.Route) error {
	if len(routes) == 0 {
		return nil
	}
	params := make([]sqlcgen.BatchUpsertExactRouteParams, len(routes))
	for i, route := range routes {
		params[i] = sqlcgen.BatchUpsertExactRouteParams{
			Msisdn:     route.MSISDN,
			TargetType: string(route.Target.Type),
			TargetID:   route.Target.ID,
			Source:     string(route.Source),
			ImportedAt: tsFromPtr(route.ImportedAt),
		}
	}
	br := r.q.BatchUpsertExactRoute(ctx, params)
	defer func() { _ = br.Close() }()

	var firstErr error
	br.Exec(func(_ int, err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	})
	if firstErr != nil {
		return translate("bulk upsert exact routes", firstErr)
	}
	return nil
}

// toExactRoute maps a stored row to the domain type.
func toExactRoute(row sqlcgen.ControlPlaneExactRoute) exact.Route {
	return exact.Route{
		MSISDN:     row.Msisdn,
		Target:     exact.Target{Type: exact.TargetType(row.TargetType), ID: row.TargetID},
		Source:     exact.Source(row.Source),
		ImportedAt: tsPtr(row.ImportedAt),
		UpdatedAt:  tsVal(row.UpdatedAt),
	}
}
