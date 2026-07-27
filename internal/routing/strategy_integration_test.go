package routing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/routing"
)

// nonStaticRoute builds one active route with a distribution strategy and two targets (prefix 225).
func nonStaticRoute(dist cp.DistributionStrategy, connA, connB uuid.UUID, weights [2]int) cp.Route {
	return cp.Route{
		ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: dist,
		MatchDestPattern: ptr("225"),
		Targets: []cp.RouteTarget{
			{ConnectorID: connA, Weight: weights[0]},
			{ConnectorID: connB, Weight: weights[1]},
		},
	}
}

// TestResolveWeightedRoute: a weighted route resolves to one of its target connectors, deterministically
// for a given destination.
func TestResolveWeightedRoute(t *testing.T) {
	connA, connB := uuid.New(), uuid.New()
	r, err := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{
		nonStaticRoute(cp.DistributionWeighted, connA, connB, [2]int{70, 30}),
	}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	valid := map[uuid.UUID]bool{connA: true, connB: true}

	got, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil || !valid[got.ConnectorID] {
		t.Fatalf("weighted resolve = (%s, %v), want a target connector", got.ConnectorID, err)
	}
	// Deterministic: the same destination resolves identically.
	again, _ := r.Resolve(context.Background(), "+2250700000001")
	if again.ConnectorID != got.ConnectorID {
		t.Errorf("weighted not deterministic per destination: %s != %s", got.ConnectorID, again.ConnectorID)
	}
}

// TestResolveRoundRobinRoute: a round_robin route rotates across its two targets over successive
// resolutions (the counter lives in the mutable overlay).
func TestResolveRoundRobinRoute(t *testing.T) {
	connA, connB := uuid.New(), uuid.New()
	r, err := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{
		nonStaticRoute(cp.DistributionRoundRobin, connA, connB, [2]int{1, 1}),
	}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	seen := map[uuid.UUID]int{}
	for i := 0; i < 10; i++ {
		got, err := r.Resolve(context.Background(), "+2250700000001")
		if err != nil {
			t.Fatalf("resolve %d: %v", i, err)
		}
		seen[got.ConnectorID]++
	}
	if seen[connA] != 5 || seen[connB] != 5 {
		t.Errorf("round_robin over 10 = {A:%d B:%d}, want 5/5", seen[connA], seen[connB])
	}
}

// TestResolveHashBasedRoute: a hash_based route maps a destination stably to one connector.
func TestResolveHashBasedRoute(t *testing.T) {
	connA, connB := uuid.New(), uuid.New()
	r, err := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{
		nonStaticRoute(cp.DistributionHashBased, connA, connB, [2]int{1, 1}),
	}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	first, _ := r.Resolve(context.Background(), "+2250700000042")
	for i := 0; i < 5; i++ {
		again, _ := r.Resolve(context.Background(), "+2250700000042")
		if again.ConnectorID != first.ConnectorID {
			t.Fatalf("hash_based not stable: %s != %s", again.ConnectorID, first.ConnectorID)
		}
	}
}

// TestHashMappingStableAcrossReload: two snapshots with the targets in DIFFERENT row order map a
// destination to the same connector — compileTargets' sort-by-connector-id makes the mapping order-
// independent, so a config reload never remaps keys.
func TestHashMappingStableAcrossReload(t *testing.T) {
	connA, connB := uuid.New(), uuid.New()
	forward := nonStaticRoute(cp.DistributionHashBased, connA, connB, [2]int{1, 1})
	reverse := forward
	reverse.Targets = []cp.RouteTarget{{ConnectorID: connB, Weight: 1}, {ConnectorID: connA, Weight: 1}}

	r1, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{forward}})
	r2, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{reverse}})

	for i := 0; i < 20; i++ {
		dest := "+225070000" + string(rune('0'+i%10)) + "000"
		g1, _ := r1.Resolve(context.Background(), dest)
		g2, _ := r2.Resolve(context.Background(), dest)
		if g1.ConnectorID != g2.ConnectorID {
			t.Fatalf("hash mapping changed with target order for %s: %s != %s", dest, g1.ConnectorID, g2.ConnectorID)
		}
	}
}

// TestRoundRobinCounterPersistsAcrossSwap: the round-robin counter lives in the mutable overlay, so a
// snapshot Swap (config reload) does not reset the rotation.
func TestRoundRobinCounterPersistsAcrossSwap(t *testing.T) {
	connA, connB := uuid.New(), uuid.New()
	route := nonStaticRoute(cp.DistributionRoundRobin, connA, connB, [2]int{1, 1})
	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{route}})

	first, _ := r.Resolve(context.Background(), "+2250700000001") // counter 0 → A

	// Rebuild the SAME route (same id) and swap it in.
	snap, err := routing.BuildSnapshot(context.Background(), fakeLister{routes: []cp.Route{route}})
	if err != nil {
		t.Fatalf("BuildSnapshot: %v", err)
	}
	r.Swap(snap)

	second, _ := r.Resolve(context.Background(), "+2250700000001") // counter 1 → B (not reset to A)
	if first.ConnectorID == second.ConnectorID {
		t.Errorf("round-robin reset across swap: both resolved to %s", first.ConnectorID)
	}
}

// fakeLoad is an injected connector-load reader for least_loaded.
type fakeLoad map[uuid.UUID]int

func (f fakeLoad) InFlight(_ context.Context, id uuid.UUID) int { return f[id] }

// TestResolveFailoverPriority: a failover_priority route picks the lowest-priority target (the primary;
// in M7 all targets are available, no breaker).
func TestResolveFailoverPriority(t *testing.T) {
	primary, secondary := uuid.New(), uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionFailoverPriority,
		MatchDestPattern: ptr("225"),
		Targets: []cp.RouteTarget{
			{ConnectorID: secondary, Priority: 2},
			{ConnectorID: primary, Priority: 1},
		}}
	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{route}})

	got, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != primary {
		t.Errorf("failover routed to %s, want the priority-1 target %s", got.ConnectorID, primary)
	}
}

// TestResolveLeastLoaded: a least_loaded route picks the connector with the smallest injected load
// gauge (read via the overlay's LoadReader, no Go read-modify-write).
func TestResolveLeastLoaded(t *testing.T) {
	busy, idle := uuid.New(), uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionLeastLoaded,
		MatchDestPattern: ptr("225"),
		Targets:          []cp.RouteTarget{{ConnectorID: busy, Weight: 1}, {ConnectorID: idle, Weight: 1}}}
	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{route}})
	r.UseLoadReader(fakeLoad{busy: 100, idle: 3})

	got, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != idle {
		t.Errorf("least_loaded routed to %s, want the idle connector %s", got.ConnectorID, idle)
	}
}

// TestFallbackRoute: a primary route with no retained target (empty target set) chains to its
// fallback_route rather than dropping the message.
func TestFallbackRoute(t *testing.T) {
	fallbackConn := uuid.New()
	fallback := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID: &fallbackConn} // catch-all fallback
	primary := cp.Route{ID: uuid.New(), Priority: 50, Status: cp.RouteActive, DistributionStrategy: cp.DistributionWeighted,
		MatchDestPattern: ptr("225"), Targets: nil, FallbackRouteID: &fallback.ID} // no targets → falls back
	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{primary, fallback}})

	got, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != fallbackConn {
		t.Errorf("no-target primary routed to %s, want the fallback %s", got.ConnectorID, fallbackConn)
	}
}

// TestFallbackToDisabledRouteFallsThrough: a 0-target route whose fallback is disabled (skipped from
// the snapshot) is itself dropped, so traffic falls through to a less-specific route rather than being
// black-holed (the MAJEUR fix).
func TestFallbackToDisabledRouteFallsThrough(t *testing.T) {
	catchConn, disabledConn := uuid.New(), uuid.New()
	disabled := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteDisabled, DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID: &disabledConn} // the fallback target — DISABLED
	primary := cp.Route{ID: uuid.New(), Priority: 50, Status: cp.RouteActive, DistributionStrategy: cp.DistributionWeighted,
		MatchDestPattern: ptr("225"), Targets: nil, FallbackRouteID: &disabled.ID} // 0 targets, fallback→disabled
	catch := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID: &catchConn} // catch-all

	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{primary, disabled, catch}})
	got, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != catchConn {
		t.Errorf("dead-fallback primary routed to %s, want the catch-all fall-through %s", got.ConnectorID, catchConn)
	}
}

// TestFallbackCycleTerminates: a fallback cycle A→B→A among 0-target routes is dropped at build time
// (neither can deliver), so a matching destination falls through and never hangs.
func TestFallbackCycleTerminates(t *testing.T) {
	catchConn := uuid.New()
	idA, idB := uuid.New(), uuid.New()
	a := cp.Route{ID: idA, Priority: 40, Status: cp.RouteActive, DistributionStrategy: cp.DistributionWeighted,
		MatchDestPattern: ptr("225"), FallbackRouteID: &idB}
	b := cp.Route{ID: idB, Priority: 41, Status: cp.RouteActive, DistributionStrategy: cp.DistributionWeighted,
		MatchDestPattern: ptr("2250"), FallbackRouteID: &idA}
	catch := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic, TargetConnectorID: &catchConn}

	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{a, b, catch}})
	got, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil {
		t.Fatalf("resolve (cycle must not hang): %v", err)
	}
	if got.ConnectorID != catchConn {
		t.Errorf("cyclic-fallback routes routed to %s, want the catch-all %s", got.ConnectorID, catchConn)
	}
}

// TestLeastLoadedNilReaderIsDeterministic: with no load reader wired, least_loaded treats every load as
// 0 and picks deterministically (smallest connector id), never panics.
func TestLeastLoadedNilReaderIsDeterministic(t *testing.T) {
	c1, c2 := uuid.New(), uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionLeastLoaded,
		MatchDestPattern: ptr("225"), Targets: []cp.RouteTarget{{ConnectorID: c1}, {ConnectorID: c2}}}
	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{route}})
	// No UseLoadReader call → nil reader.
	first, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	again, _ := r.Resolve(context.Background(), "+2250700000001")
	if first.ConnectorID != again.ConnectorID {
		t.Errorf("least_loaded with nil reader not deterministic: %s != %s", first.ConnectorID, again.ConnectorID)
	}
}
