package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// RouteRepo is the routes repository. It satisfies adminapi.RouteStore structurally. A route and its
// targets are written in one transaction, so a route never exists with a half-written target set.
type RouteRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// NewRouteRepo returns the routes repository backed by pool.
func NewRouteRepo(pool *pgxpool.Pool) *RouteRepo {
	return &RouteRepo{pool: pool, q: sqlcgen.New(pool)}
}

// Create inserts a route and its targets in one transaction.
func (r *RouteRepo) Create(ctx context.Context, in cp.NewRoute) (cp.Route, error) {
	var route cp.Route
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		qtx := r.q.WithTx(tx)
		row, err := qtx.CreateRoute(ctx, sqlcgen.CreateRouteParams{
			Name:                 in.Name,
			Priority:             i32ptr(in.Priority),
			MatchAccountID:       in.MatchAccountID,
			MatchCustomerID:      in.MatchCustomerID,
			MatchSenderPattern:   in.MatchSenderPattern,
			MatchDestPattern:     in.MatchDestPattern,
			MatchContentPattern:  in.MatchContentPattern,
			DistributionStrategy: string(in.DistributionStrategy),
			TargetConnectorID:    in.TargetConnectorID,
			FallbackRouteID:      in.FallbackRouteID,
		})
		if err != nil {
			return err
		}
		if err := insertTargets(ctx, qtx, row.ID, in.Targets); err != nil {
			return err
		}
		route = routeFromRow(row)
		route.Targets = in.Targets
		return nil
	})
	if err != nil {
		return cp.Route{}, translate("create route", err)
	}
	return route, nil
}

// Get returns a route with its targets, or ErrNotFound.
func (r *RouteRepo) Get(ctx context.Context, id uuid.UUID) (cp.Route, error) {
	row, err := r.q.GetRoute(ctx, id)
	if err != nil {
		return cp.Route{}, translate("get route", err)
	}
	route := routeFromRow(row)
	targets, err := r.targetsOf(ctx, id)
	if err != nil {
		return cp.Route{}, err
	}
	route.Targets = targets
	return route, nil
}

// List returns every route (ordered by priority) with its targets. The contract returns a bare
// array (no pagination).
//
// Known limitation (M1): this reads targets per route (1 + N round-trips), outside any transaction,
// so a large listing is neither a single snapshot nor batched. Acceptable at the control plane's
// low QPS; the fix when it matters is a single `WHERE route_id = ANY($1)` fetch keyed by the route
// ids, joined in memory.
func (r *RouteRepo) List(ctx context.Context) ([]cp.Route, error) {
	rows, err := r.q.ListRoutes(ctx)
	if err != nil {
		return nil, translate("list routes", err)
	}
	out := make([]cp.Route, 0, len(rows))
	for _, row := range rows {
		route := routeFromRow(row)
		targets, err := r.targetsOf(ctx, row.ID)
		if err != nil {
			return nil, err
		}
		route.Targets = targets
		out = append(out, route)
	}
	return out, nil
}

// Update applies a partial change and returns the route with its targets. When Targets is non-nil,
// the whole target set is replaced inside the same transaction; a nil Targets leaves them untouched.
func (r *RouteRepo) Update(ctx context.Context, id uuid.UUID, p cp.RoutePatch) (cp.Route, error) {
	var route cp.Route
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		qtx := r.q.WithTx(tx)
		row, err := qtx.UpdateRoute(ctx, sqlcgen.UpdateRouteParams{
			ID:                   id,
			Name:                 p.Name,
			Priority:             i32ptr(p.Priority),
			MatchAccountID:       p.MatchAccountID,
			MatchCustomerID:      p.MatchCustomerID,
			MatchSenderPattern:   p.MatchSenderPattern,
			MatchDestPattern:     p.MatchDestPattern,
			MatchContentPattern:  p.MatchContentPattern,
			DistributionStrategy: strPtr(p.DistributionStrategy),
			TargetConnectorID:    p.TargetConnectorID,
			FallbackRouteID:      p.FallbackRouteID,
			Status:               strPtr(p.Status),
		})
		if err != nil {
			return err
		}
		route = routeFromRow(row)

		if p.Targets != nil {
			if err := qtx.DeleteRouteTargets(ctx, id); err != nil {
				return err
			}
			if err := insertTargets(ctx, qtx, id, p.Targets); err != nil {
				return err
			}
			route.Targets = p.Targets
			return nil
		}
		// Targets unchanged: read them back for the response.
		targets, err := targetsFrom(ctx, qtx, id)
		if err != nil {
			return err
		}
		route.Targets = targets
		return nil
	})
	if err != nil {
		return cp.Route{}, translate("update route", err)
	}
	return route, nil
}

// Delete removes a route (its targets cascade), or reports ErrNotFound.
func (r *RouteRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteRoute(ctx, id)
	if err != nil {
		return translate("delete route", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func (r *RouteRepo) targetsOf(ctx context.Context, routeID uuid.UUID) ([]cp.RouteTarget, error) {
	targets, err := targetsFrom(ctx, r.q, routeID)
	if err != nil {
		return nil, translate("list route targets", err)
	}
	return targets, nil
}

// insertTargets writes a route's targets through the given querier (inside a transaction).
func insertTargets(ctx context.Context, q *sqlcgen.Queries, routeID uuid.UUID, targets []cp.RouteTarget) error {
	for _, t := range targets {
		if err := q.InsertRouteTarget(ctx, sqlcgen.InsertRouteTargetParams{
			RouteID:     routeID,
			ConnectorID: t.ConnectorID,
			Weight:      i32ptr(&t.Weight),
			Priority:    i32ptr(&t.Priority),
		}); err != nil {
			return err
		}
	}
	return nil
}

// targetsFrom reads a route's targets through the given querier.
func targetsFrom(ctx context.Context, q *sqlcgen.Queries, routeID uuid.UUID) ([]cp.RouteTarget, error) {
	rows, err := q.ListRouteTargets(ctx, routeID)
	if err != nil {
		return nil, err
	}
	out := make([]cp.RouteTarget, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.RouteTarget{
			ConnectorID: row.ConnectorID,
			Weight:      int(row.Weight),
			Priority:    int(row.Priority),
		})
	}
	return out, nil
}

func routeFromRow(row sqlcgen.ControlPlaneRoute) cp.Route {
	return cp.Route{
		ID:                   row.ID,
		Name:                 row.Name,
		Priority:             int(row.Priority),
		MatchAccountID:       row.MatchAccountID,
		MatchCustomerID:      row.MatchCustomerID,
		MatchSenderPattern:   row.MatchSenderPattern,
		MatchDestPattern:     row.MatchDestPattern,
		MatchContentPattern:  row.MatchContentPattern,
		DistributionStrategy: cp.DistributionStrategy(row.DistributionStrategy),
		TargetConnectorID:    row.TargetConnectorID,
		FallbackRouteID:      row.FallbackRouteID,
		Status:               cp.RouteStatus(row.Status),
		CreatedAt:            tsVal(row.CreatedAt),
		UpdatedAt:            tsVal(row.UpdatedAt),
	}
}
