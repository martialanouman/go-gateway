// Package routing resolves a destination to a connector for the MT pipeline. The declarative resolver
// is an immutable snapshot of the active static routes, matched by numeric destination prefix. The
// snapshot is held behind an atomic pointer so config-sync (step-105) can rebuild and swap it whole,
// without a lock and without downtime on the hot path. Routing scripts, weighted/failover distribution
// and per-connector load live in later milestones; volatile state (load, breaker) is deliberately kept
// OUT of the immutable snapshot and will be read from a separate overlay alongside it (step-113/M8),
// never mutated into a published snapshot.
package routing

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/routing/strategy"
)

// RouteLister loads the declarative routes. *postgres.RouteRepo satisfies it structurally.
type RouteLister interface {
	List(ctx context.Context) ([]cp.Route, error)
}

// Snapshot is an immutable, compiled set of routes. Nothing mutates it after BuildSnapshot, so a
// reader that has it in hand always sees a consistent whole; evolution is a NEW Snapshot swapped in,
// never an in-place write. Safe for concurrent reads.
type Snapshot struct {
	routes []compiledRoute
}

type compiledRoute struct {
	prefix          string    // digits only ("" = catch-all)
	connectorID     uuid.UUID // static strategy only
	routeID         uuid.UUID
	priority        int
	strategy        cp.DistributionStrategy
	targets         []strategy.Target // non-static strategies
	fallbackRouteID *uuid.UUID        // route-level fallback (step-114)
}

// SnapshotResolver serves route resolution from the current Snapshot, held behind an atomic pointer.
// Per-message reads Load() lock-free (hot path, ~8000/s); config-sync replaces the whole set with Swap.
// The round-robin counters live in a mutable overlay (rr) beside the immutable snapshot, never inside
// it (step-104): a config swap replaces the routes but the counters persist per route id.
type SnapshotResolver struct {
	current atomic.Pointer[Snapshot]
	rr      sync.Map // routeID -> *atomic.Uint64
}

// BuildSnapshot reads the active routes and compiles them for prefix matching, returning an immutable
// Snapshot. It is the reusable rebuild step config-sync (step-105) calls on invalidation. A static
// route compiles to its single connector; a non-static route compiles its route_targets (sorted by
// connector id for a stable weighted/hash_based mapping). failover_priority/least_loaded are compiled
// but selected in step-114.
func BuildSnapshot(ctx context.Context, lister RouteLister) (*Snapshot, error) {
	all, err := lister.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("routing: load routes: %w", err)
	}

	var compiled []compiledRoute
	for _, r := range all {
		if r.Status != cp.RouteActive || !selectableStrategy(r.DistributionStrategy) {
			continue
		}
		// A static route needs a connector; a non-static route needs targets. Skip an ill-formed route
		// rather than compiling a dead one.
		if r.DistributionStrategy == cp.DistributionStatic && r.TargetConnectorID == nil {
			continue
		}
		if r.DistributionStrategy != cp.DistributionStatic && len(r.Targets) == 0 {
			continue
		}
		prefix := ""
		if r.MatchDestPattern != nil {
			prefix = digits(*r.MatchDestPattern)
		}
		cr := compiledRoute{
			prefix:          prefix,
			routeID:         r.ID,
			priority:        r.Priority,
			strategy:        r.DistributionStrategy,
			fallbackRouteID: r.FallbackRouteID,
		}
		if r.DistributionStrategy == cp.DistributionStatic {
			cr.connectorID = *r.TargetConnectorID
		} else {
			cr.targets = compileTargets(r.Targets)
		}
		compiled = append(compiled, cr)
	}

	// Most specific first: a longer prefix outranks a shorter one; equal lengths tie-break on the
	// lower priority number (evaluated first, per the schema).
	sort.SliceStable(compiled, func(i, j int) bool {
		if len(compiled[i].prefix) != len(compiled[j].prefix) {
			return len(compiled[i].prefix) > len(compiled[j].prefix)
		}
		return compiled[i].priority < compiled[j].priority
	})

	return &Snapshot{routes: compiled}, nil
}

// selectableStrategy reports whether a strategy can pick a connector today. failover_priority and
// least_loaded arrive in step-114; compiling them now would make a matching route black-hole its
// traffic (match → no connector → ErrNoRoute) and shadow a working fallback, so they are skipped until
// then — a route with those strategies falls through to a less-specific route, as before this step.
func selectableStrategy(s cp.DistributionStrategy) bool {
	switch s {
	case cp.DistributionStatic, cp.DistributionRoundRobin, cp.DistributionWeighted, cp.DistributionHashBased:
		return true
	default:
		return false
	}
}

// NewResolver wraps a prebuilt Snapshot as the current one. config-sync builds the first snapshot and
// later swaps in rebuilt ones. snap must be non-nil (BuildSnapshot always returns a non-nil snapshot,
// even for zero routes); a nil snapshot would panic a later read.
func NewResolver(snap *Snapshot) *SnapshotResolver {
	r := &SnapshotResolver{}
	r.current.Store(snap)
	return r
}

// LoadSnapshot builds the first snapshot and returns a resolver serving it. It is the boot path;
// config-sync uses BuildSnapshot + Swap thereafter.
func LoadSnapshot(ctx context.Context, lister RouteLister) (*SnapshotResolver, error) {
	snap, err := BuildSnapshot(ctx, lister)
	if err != nil {
		return nil, err
	}
	return NewResolver(snap), nil
}

// Swap atomically replaces the served snapshot. In-flight reads keep the old one (consistent); every
// read after Swap sees the new one. It never blocks a reader. snap must be non-nil: config-sync swaps
// only a successfully rebuilt snapshot and keeps the old one on a rebuild failure (step-105), so a nil
// is never swapped in.
func (r *SnapshotResolver) Swap(snap *Snapshot) {
	r.current.Store(snap)
}

// Resolve returns the connector for dest, or ErrNoRoute when nothing matches (or the matched route
// retains no target). dest is the E.164 form ("+225…"); matching is on its digits, so a "225" prefix
// matches "+225…". A non-static route selects a connector from its targets via its distribution
// strategy (dest is the hash/weight key, so all segments of a message route alike).
func (r *SnapshotResolver) Resolve(_ context.Context, dest string) (pipeline.Route, error) {
	cr, ok := r.current.Load().match(dest)
	if !ok {
		return pipeline.Route{}, errs.ErrNoRoute
	}
	return r.route(cr, dest)
}

// route selects a connector for a matched route via its strategy. ErrNoRoute when no target is retained
// (step-114 chains fallback_route here).
func (r *SnapshotResolver) route(cr *compiledRoute, dest string) (pipeline.Route, error) {
	conn, ok := r.selectConnector(cr, dest)
	if !ok {
		return pipeline.Route{}, errs.ErrNoRoute
	}
	routeID := cr.routeID
	return pipeline.Route{ConnectorID: conn, RouteID: &routeID}, nil
}

// selectConnector applies a route's distribution strategy. static returns the single connector;
// round_robin uses the mutable per-route counter; weighted/hash_based are deterministic in dest.
// failover_priority/least_loaded are compiled but not yet selected (step-114) → ok=false for now.
func (r *SnapshotResolver) selectConnector(cr *compiledRoute, dest string) (uuid.UUID, bool) {
	switch cr.strategy {
	case cp.DistributionStatic:
		return cr.connectorID, true
	case cp.DistributionRoundRobin:
		return strategy.RoundRobin(cr.targets, r.rrNext(cr.routeID))
	case cp.DistributionWeighted:
		return strategy.Weighted(cr.targets, dest)
	case cp.DistributionHashBased:
		return strategy.HashBased(cr.targets, dest)
	default:
		return uuid.Nil, false
	}
}

// rrNext returns the next monotonic counter value for a route's round-robin rotation, from the mutable
// overlay (never the immutable snapshot).
func (r *SnapshotResolver) rrNext(routeID uuid.UUID) uint64 {
	v, ok := r.rr.Load(routeID)
	if !ok {
		v, _ = r.rr.LoadOrStore(routeID, new(atomic.Uint64)) // allocate only on the cold path
	}
	return v.(*atomic.Uint64).Add(1) - 1
}

// routeForTarget maps an exact-route Target to a pipeline.Route against the current snapshot (used by
// the L0 short-cut and routing scripts). A connector target routes straight to that connector; a route
// target is resolved through that route's distribution strategy (dest is the key). matched is false
// when a route target no longer exists in the snapshot or retains no target, so the caller falls back.
//
// A connector target is trusted without an existence check: the snapshot holds no connector registry,
// so this mirrors the declarative resolver. A dangling connector is caught downstream at send time.
func (r *SnapshotResolver) routeForTarget(t exact.Target, dest string) (pipeline.Route, bool) {
	switch t.Type {
	case exact.TargetConnector:
		return pipeline.Route{ConnectorID: t.ID}, true
	case exact.TargetRoute:
		if cr, ok := r.current.Load().findByID(t.ID); ok {
			route, err := r.route(cr, dest)
			return route, err == nil
		}
	}
	return pipeline.Route{}, false
}

// routeByID resolves a route id (as a routing script returns) to a connector via that route's strategy.
// matched is false when the id is not an active route in the snapshot or retains no target.
func (r *SnapshotResolver) routeByID(routeID uuid.UUID, dest string) (pipeline.Route, bool) {
	return r.routeForTarget(exact.Target{Type: exact.TargetRoute, ID: routeID}, dest)
}

// match returns the first compiled route whose prefix matches dest's digits (most specific first).
func (s *Snapshot) match(dest string) (*compiledRoute, bool) {
	d := digits(dest)
	for i := range s.routes {
		if s.routes[i].prefix == "" || strings.HasPrefix(d, s.routes[i].prefix) {
			return &s.routes[i], true
		}
	}
	return nil, false
}

// findByID returns the compiled route with the given id.
func (s *Snapshot) findByID(routeID uuid.UUID) (*compiledRoute, bool) {
	for i := range s.routes {
		if s.routes[i].routeID == routeID {
			return &s.routes[i], true
		}
	}
	return nil, false
}

// compileTargets converts route targets to the strategy form, sorted by connector id so weighted and
// hash_based map a key to the same connector across reloads (the order must not depend on row order).
func compileTargets(targets []cp.RouteTarget) []strategy.Target {
	out := make([]strategy.Target, len(targets))
	for i, t := range targets {
		out[i] = strategy.Target{ConnectorID: t.ConnectorID, Weight: t.Weight, Priority: t.Priority}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConnectorID.String() < out[j].ConnectorID.String() })
	return out
}

// digits keeps only 0-9, dropping the leading "+" and any separators.
func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
