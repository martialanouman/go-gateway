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

// LoadReader reads a connector's in-flight gauge (connectorload:{id}, Appendix B) for least_loaded. A
// missing gauge reads 0. It is part of the mutable overlay — volatile state read beside the immutable
// snapshot, never compiled into it (step-104).
type LoadReader interface {
	InFlight(ctx context.Context, connectorID uuid.UUID) int
}

// AvailabilityReader reports which of the given connectors are currently unavailable — breaker open
// (breaker:state, step-122). It is consulted ONLY when the snapshot is (re)built (RefreshAvailability),
// never on the per-message resolve path: the hot path must not touch Redis (§12). The returned map holds
// the open connectors; any connector absent from it is treated as available. The map becomes the
// resolver's — the reader must not retain or mutate it after returning.
type AvailabilityReader interface {
	Unavailable(ctx context.Context, connectorIDs []uuid.UUID) (map[uuid.UUID]bool, error)
}

// SnapshotResolver serves route resolution from the current Snapshot, held behind an atomic pointer.
// Per-message reads Load() lock-free (hot path, ~8000/s); config-sync replaces the whole set with Swap.
// The round-robin counters, the load reader and the availability overlay live beside the immutable
// snapshot, never inside it (step-104): a config swap replaces the routes but the counters persist per
// route id, and the availability overlay is refreshed at each rebuild (breaker awareness, step-123).
type SnapshotResolver struct {
	current     atomic.Pointer[Snapshot]
	rr          sync.Map                        // routeID -> *atomic.Uint64
	loadReader  LoadReader                      // nil = least_loaded treats every connector as load 0
	unavailable atomic.Pointer[availabilitySet] // connectors to exclude (breaker open); nil = all available
}

// availabilitySet is the set of connectors excluded from selection (breaker open). It is replaced whole
// behind the atomic pointer at each rebuild, never mutated in place — mirroring the immutable snapshot.
type availabilitySet map[uuid.UUID]bool

// UseLoadReader sets the connector-load source for least_loaded (the mutable overlay). The router wires
// a Redis-backed reader; a nil reader makes least_loaded pick deterministically as if all loads are 0.
func (r *SnapshotResolver) UseLoadReader(lr LoadReader) { r.loadReader = lr }

// RefreshAvailability re-reads the breaker state of every connector in the current snapshot and replaces
// the availability overlay atomically. Call it right after a Swap (and once at boot): it is the ONLY
// place breaker:state is read, keeping the per-message path free of Redis. A nil reader clears the
// overlay (everything available). On a reader error the overlay is left unchanged (serve with the last
// known availability rather than blackhole the whole fleet) and the error is returned for the caller
// to log.
func (r *SnapshotResolver) RefreshAvailability(ctx context.Context, reader AvailabilityReader) error {
	if reader == nil {
		r.unavailable.Store(&availabilitySet{})
		return nil
	}
	snap := r.current.Load()
	if snap == nil {
		return nil
	}
	ids := snap.connectorIDs()
	if len(ids) == 0 {
		r.unavailable.Store(&availabilitySet{})
		return nil
	}
	open, err := reader.Unavailable(ctx, ids)
	if err != nil {
		return err
	}
	set := availabilitySet(open)
	if set == nil {
		set = availabilitySet{}
	}
	r.unavailable.Store(&set)
	return nil
}

// availableTargets drops the breaker-open connectors from targets. The common case — nothing open —
// returns the original slice with no allocation, so the hot path pays nothing when the fleet is healthy.
func (r *SnapshotResolver) availableTargets(targets []strategy.Target) []strategy.Target {
	open := r.unavailable.Load()
	if open == nil || len(*open) == 0 {
		return targets
	}
	filtered := make([]strategy.Target, 0, len(targets))
	for _, t := range targets {
		if !(*open)[t.ConnectorID] {
			filtered = append(filtered, t)
		}
	}
	return filtered
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
		// A non-static route with no targets is dead unless it has a fallback to chain to: skip the
		// truly-dead ones (fall through) but compile a 0-target route that falls back (step-114).
		if r.DistributionStrategy != cp.DistributionStatic && len(r.Targets) == 0 && r.FallbackRouteID == nil {
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

	// Drop any 0-target route whose fallback chain cannot reach a connector (its fallback was disabled
	// or deleted): keeping it would let it shadow — and black-hole — a less-specific route that could
	// deliver. Dropping it lets match fall through to that route instead.
	compiled = pruneDeadFallbacks(compiled)

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

// canSelect reports whether a route can pick a connector directly (a static connector or at least one
// target), independent of any fallback.
func (cr *compiledRoute) canSelect() bool {
	return cr.connectorID != uuid.Nil || len(cr.targets) > 0
}

// pruneDeadFallbacks removes routes that can neither select a connector directly nor reach one through
// their fallback chain (cycle-guarded). This runs once at build time, off the hot path.
func pruneDeadFallbacks(compiled []compiledRoute) []compiledRoute {
	byID := make(map[uuid.UUID]*compiledRoute, len(compiled))
	for i := range compiled {
		byID[compiled[i].routeID] = &compiled[i]
	}
	var delivers func(cr *compiledRoute, visited map[uuid.UUID]bool) bool
	delivers = func(cr *compiledRoute, visited map[uuid.UUID]bool) bool {
		if cr.canSelect() {
			return true
		}
		if visited[cr.routeID] {
			return false // fallback cycle with no selectable route
		}
		visited[cr.routeID] = true
		if cr.fallbackRouteID != nil {
			if fb, ok := byID[*cr.fallbackRouteID]; ok {
				return delivers(fb, visited)
			}
		}
		return false
	}
	// A fresh slice, not an in-place filter: byID holds pointers into `compiled`, so it must not be
	// overwritten while delivers still reads it.
	kept := make([]compiledRoute, 0, len(compiled))
	for i := range compiled {
		if delivers(&compiled[i], map[uuid.UUID]bool{}) {
			kept = append(kept, compiled[i])
		}
	}
	return kept
}

// selectableStrategy reports whether a strategy can pick a connector. All six are supported as of
// step-114; an unknown value (never valid per the schema CHECK) is skipped defensively.
func selectableStrategy(s cp.DistributionStrategy) bool {
	return s.Valid()
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
func (r *SnapshotResolver) Resolve(ctx context.Context, dest string) (pipeline.Route, error) {
	snap := r.current.Load()
	cr, ok := snap.match(dest)
	if !ok {
		return pipeline.Route{}, errs.ErrNoRoute
	}
	return r.resolveFrom(ctx, snap, cr, dest, map[uuid.UUID]bool{})
}

// resolveFrom selects a connector for cr, chaining to its fallback_route when it retains no target
// (spec §6.1: route-level fallback). visited guards against a fallback cycle.
func (r *SnapshotResolver) resolveFrom(ctx context.Context, snap *Snapshot, cr *compiledRoute, dest string, visited map[uuid.UUID]bool) (pipeline.Route, error) {
	if visited[cr.routeID] {
		return pipeline.Route{}, errs.ErrNoRoute // fallback cycle
	}
	visited[cr.routeID] = true

	if conn, ok := r.selectConnector(ctx, cr, dest); ok {
		routeID := cr.routeID
		return pipeline.Route{ConnectorID: conn, RouteID: &routeID, FallbackChain: r.fallbackChain(ctx, cr)}, nil
	}
	// No target retained → follow the route-level fallback if configured.
	if cr.fallbackRouteID != nil {
		if fb, ok := snap.findByID(*cr.fallbackRouteID); ok {
			return r.resolveFrom(ctx, snap, fb, dest, visited)
		}
	}
	return pipeline.Route{}, errs.ErrNoRoute
}

// selectConnector applies a route's distribution strategy. static returns the single connector;
// round_robin uses the mutable per-route counter; weighted/hash_based are deterministic in dest;
// failover_priority picks the lowest-priority target; least_loaded reads the connector-load overlay.
// ok is false only when the route retains no target (an empty target set) → the caller falls back.
func (r *SnapshotResolver) selectConnector(ctx context.Context, cr *compiledRoute, dest string) (uuid.UUID, bool) {
	switch cr.strategy {
	case cp.DistributionStatic:
		return cr.connectorID, true
	case cp.DistributionRoundRobin:
		return strategy.RoundRobin(cr.targets, r.rrNext(cr.routeID))
	case cp.DistributionWeighted:
		return strategy.Weighted(cr.targets, dest)
	case cp.DistributionHashBased:
		return strategy.HashBased(cr.targets, dest)
	case cp.DistributionFailoverPriority:
		// Breaker-aware (step-123): exclude open connectors so failover moves to a healthy target. A
		// half_open connector stays selectable — its trickle of probe traffic is bounded downstream by
		// the breaker's half-open probe quota, not here.
		return strategy.FailoverPriority(r.availableTargets(cr.targets))
	case cp.DistributionLeastLoaded:
		return strategy.LeastLoaded(r.availableTargets(cr.targets), func(id uuid.UUID) int { return r.inFlight(ctx, id) })
	default:
		return uuid.Nil, false
	}
}

// fallbackChain is the ordered connector fallback order the connector pool follows on reroute
// (step-125), for the breaker-aware strategies only (failover_priority / least_loaded). It is the FULL
// ordered target list (not availability-filtered): the pool re-checks each connector's breaker at
// reroute time and skips the open ones, so a connector that recovers can still serve as a later
// fallback. Other strategies get no chain — a single terminal outcome, no reroute.
func (r *SnapshotResolver) fallbackChain(ctx context.Context, cr *compiledRoute) []uuid.UUID {
	switch cr.strategy {
	case cp.DistributionFailoverPriority:
		return strategy.FailoverPriorityChain(cr.targets)
	case cp.DistributionLeastLoaded:
		return strategy.LeastLoadedChain(cr.targets, func(id uuid.UUID) int { return r.inFlight(ctx, id) })
	default:
		return nil
	}
}

// inFlight reads a connector's load gauge via the overlay's reader, or 0 when no reader is wired.
func (r *SnapshotResolver) inFlight(ctx context.Context, connectorID uuid.UUID) int {
	if r.loadReader == nil {
		return 0
	}
	return r.loadReader.InFlight(ctx, connectorID)
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
func (r *SnapshotResolver) routeForTarget(ctx context.Context, t exact.Target, dest string) (pipeline.Route, bool) {
	switch t.Type {
	case exact.TargetConnector:
		return pipeline.Route{ConnectorID: t.ID}, true
	case exact.TargetRoute:
		snap := r.current.Load()
		if cr, ok := snap.findByID(t.ID); ok {
			route, err := r.resolveFrom(ctx, snap, cr, dest, map[uuid.UUID]bool{})
			return route, err == nil
		}
	}
	return pipeline.Route{}, false
}

// routeByID resolves a route id (as a routing script returns) to a connector via that route's strategy
// (with its fallback chain). matched is false when the id is not an active route in the snapshot or
// retains no target.
func (r *SnapshotResolver) routeByID(ctx context.Context, routeID uuid.UUID, dest string) (pipeline.Route, bool) {
	return r.routeForTarget(ctx, exact.Target{Type: exact.TargetRoute, ID: routeID}, dest)
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

// connectorIDs returns the distinct connectors referenced by the snapshot — a static route's single
// connector and every non-static route's targets. RefreshAvailability reads their breaker state; the
// order is deterministic (first appearance) but the caller does not depend on it.
func (s *Snapshot) connectorIDs() []uuid.UUID {
	seen := make(map[uuid.UUID]struct{})
	var ids []uuid.UUID
	add := func(id uuid.UUID) {
		if id == uuid.Nil {
			return
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	for i := range s.routes {
		if s.routes[i].strategy == cp.DistributionStatic {
			add(s.routes[i].connectorID)
			continue
		}
		for _, t := range s.routes[i].targets {
			add(t.ConnectorID)
		}
	}
	return ids
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
