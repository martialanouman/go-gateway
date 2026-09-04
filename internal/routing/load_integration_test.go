package routing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/status"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestLeastLoadedFollowsThePublishedGauge is the end of the chain step-114 never closed: the pool
// publishes per-bind in_flight (status.PublishBind), the gauge is derived in Redis, and least_loaded
// picks the target whose published load is smallest — through the real LoadReader, not an injected
// map. The target must CHANGE when the published loads swap: with no writer both loads read 0 and the
// UUID tie-break picks the same connector twice, so one of the two assertions always fails.
func TestLeastLoadedFollowsThePublishedGauge(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	a, b := uuid.New(), uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionLeastLoaded,
		MatchDestPattern: ptr("225"),
		Targets:          []cp.RouteTarget{{ConnectorID: a, Weight: 1}, {ConnectorID: b, Weight: 1}}}
	r, err := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{route}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	// No cache: each resolve reads the key, so the second publish is visible without waiting a TTL.
	r.UseLoadReader(status.NewLoadReader(rdb, status.WithLoadCacheTTL(0)))
	pub := status.NewReader(rdb)

	publish := func(connectorID uuid.UUID, inFlight int) {
		t.Helper()
		if err := pub.PublishBind(ctx, connectorID, "pod-a", 0, status.LinkUp, inFlight); err != nil {
			t.Fatalf("publish %s: %v", connectorID, err)
		}
	}

	publish(a, 10)
	publish(b, 1)
	got, err := r.Resolve(ctx, "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != b {
		t.Fatalf("with loads a=10 b=1 least_loaded picked %s, want b=%s", got.ConnectorID, b)
	}

	publish(a, 0)
	publish(b, 20)
	got, err = r.Resolve(ctx, "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != a {
		t.Fatalf("with loads a=0 b=20 least_loaded still picked %s, want a=%s: the strategy is not "+
			"following the published gauge", got.ConnectorID, a)
	}
}

// TestLeastLoadedReadsTheGaugeThroughItsCache: the router wires the reader WITH its cache, so a resolve
// must not cost a Redis round trip per message. The gauge is republished every heartbeat (2 s); a 1 s
// cache never serves a value staler than the gauge itself. Within the TTL the strategy keeps its last
// answer even though the published loads have swapped — that is the trade, and it is bounded.
func TestLeastLoadedReadsTheGaugeThroughItsCache(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	a, b := uuid.New(), uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionLeastLoaded,
		MatchDestPattern: ptr("225"),
		Targets:          []cp.RouteTarget{{ConnectorID: a, Weight: 1}, {ConnectorID: b, Weight: 1}}}
	r, _ := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{route}})
	now := time.Unix(1_700_000_000, 0)
	r.UseLoadReader(status.NewLoadReader(rdb, status.WithLoadCacheTTL(time.Second),
		status.WithLoadClock(func() time.Time { return now })))
	pub := status.NewReader(rdb)
	publish := func(connectorID uuid.UUID, inFlight int) {
		t.Helper()
		if err := pub.PublishBind(ctx, connectorID, "pod-a", 0, status.LinkUp, inFlight); err != nil {
			t.Fatalf("publish %s: %v", connectorID, err)
		}
	}
	resolve := func() uuid.UUID {
		t.Helper()
		got, err := r.Resolve(ctx, "+2250700000001")
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		return got.ConnectorID
	}
	publish(a, 10)
	publish(b, 1)
	if first := resolve(); first != b {
		t.Fatalf("first resolve picked %s, want b=%s", first, b)
	}

	publish(a, 0)
	publish(b, 20)
	if cached := resolve(); cached != b {
		t.Errorf("inside the cache TTL the resolve moved to %s: the reader is hitting Redis per message", cached)
	}

	now = now.Add(1500 * time.Millisecond)
	if fresh := resolve(); fresh != a {
		t.Errorf("after the cache TTL the resolve still picked %s, want a=%s", fresh, a)
	}
}
