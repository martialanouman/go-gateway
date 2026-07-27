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
	"sync/atomic"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
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
	prefix      string // digits only ("" = catch-all)
	connectorID uuid.UUID
	routeID     uuid.UUID
	priority    int
}

// SnapshotResolver serves route resolution from the current Snapshot, held behind an atomic pointer.
// Per-message reads Load() lock-free (hot path, ~8000/s); config-sync replaces the whole set with Swap.
type SnapshotResolver struct {
	current atomic.Pointer[Snapshot]
}

// BuildSnapshot reads the active static routes and compiles them for prefix matching, returning an
// immutable Snapshot. It is the reusable rebuild step config-sync (step-105) calls on invalidation. It
// skips non-static strategies and routes without a target connector (those belong to later milestones).
func BuildSnapshot(ctx context.Context, lister RouteLister) (*Snapshot, error) {
	all, err := lister.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("routing: load routes: %w", err)
	}

	var compiled []compiledRoute
	for _, r := range all {
		if r.Status != cp.RouteActive || r.DistributionStrategy != cp.DistributionStatic || r.TargetConnectorID == nil {
			continue
		}
		prefix := ""
		if r.MatchDestPattern != nil {
			prefix = digits(*r.MatchDestPattern)
		}
		compiled = append(compiled, compiledRoute{
			prefix:      prefix,
			connectorID: *r.TargetConnectorID,
			routeID:     r.ID,
			priority:    r.Priority,
		})
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

// Resolve returns the connector for dest, or ErrNoRoute when nothing matches. dest is the E.164 form
// ("+225…"); matching is on its digits, so a "225" prefix matches "+225…".
func (r *SnapshotResolver) Resolve(_ context.Context, dest string) (pipeline.Route, error) {
	return r.current.Load().resolve(dest)
}

// routeForTarget maps an exact-route Target to a pipeline.Route against the current snapshot (used by
// the L0 short-cut). It reads Load() once so it sees a single consistent snapshot.
func (r *SnapshotResolver) routeForTarget(t exact.Target) (pipeline.Route, bool) {
	return r.current.Load().routeForTarget(t)
}

// routeByID resolves a route id (as a routing script returns) to its connector via the current
// snapshot. matched is false when the id is not an active route in the snapshot.
func (r *SnapshotResolver) routeByID(routeID uuid.UUID) (pipeline.Route, bool) {
	return r.current.Load().routeForTarget(exact.Target{Type: exact.TargetRoute, ID: routeID})
}

// resolve matches dest against the immutable compiled routes.
func (s *Snapshot) resolve(dest string) (pipeline.Route, error) {
	d := digits(dest)
	for _, r := range s.routes {
		if r.prefix == "" || strings.HasPrefix(d, r.prefix) {
			routeID := r.routeID
			return pipeline.Route{ConnectorID: r.connectorID, RouteID: &routeID}, nil
		}
	}
	return pipeline.Route{}, errs.ErrNoRoute
}

// routeForTarget maps an exact-route Target to a pipeline.Route. A connector target routes straight to
// that connector (no matched route); a route target is resolved to its connector via the compiled
// routes. matched is false when a route target no longer exists in the snapshot, so the caller falls
// back.
//
// A connector target is trusted without an existence check: the snapshot holds no connector registry,
// so this mirrors the declarative resolver, which likewise trusts its TargetConnectorID. A dangling
// connector is caught downstream at send time (circuit-open / disabled), not here.
func (s *Snapshot) routeForTarget(t exact.Target) (pipeline.Route, bool) {
	switch t.Type {
	case exact.TargetConnector:
		return pipeline.Route{ConnectorID: t.ID}, true
	case exact.TargetRoute:
		for _, r := range s.routes {
			if r.routeID == t.ID {
				routeID := r.routeID
				return pipeline.Route{ConnectorID: r.connectorID, RouteID: &routeID}, true
			}
		}
	}
	return pipeline.Route{}, false
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
