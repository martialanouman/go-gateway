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

// TestFailoverPriorityNotCompiledYet: a failover_priority route is not selectable until step-114, so it
// is skipped from the snapshot and traffic falls through to a less-specific route rather than being
// black-holed.
func TestFailoverPriorityNotCompiledYet(t *testing.T) {
	connFO, connCatch := uuid.New(), uuid.New()
	fo := nonStaticRoute(cp.DistributionFailoverPriority, connFO, uuid.New(), [2]int{1, 1})
	fo.MatchDestPattern = ptr("2250")
	catch := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID: &connCatch} // catch-all
	r, _ := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{fo, catch}})

	got, err := r.Resolve(context.Background(), "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != connCatch {
		t.Errorf("failover route not skipped: routed to %s, want the catch-all %s", got.ConnectorID, connCatch)
	}
}
