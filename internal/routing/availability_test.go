package routing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/routing"
)

// fakeAvail is an injected availability reader. It returns a fixed open-set and counts its calls so a
// test can prove the reader is consulted at rebuild only, never on the per-message resolve path.
type fakeAvail struct {
	open  map[uuid.UUID]bool
	calls int
}

func (f *fakeAvail) Unavailable(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]bool, error) {
	f.calls++
	return f.open, nil
}

type errAvail struct{}

func (errAvail) Unavailable(context.Context, []uuid.UUID) (map[uuid.UUID]bool, error) {
	return nil, errors.New("redis down")
}

func failoverRoute(primary, secondary uuid.UUID) cp.Route {
	return cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionFailoverPriority,
		MatchDestPattern: ptr("225"),
		Targets: []cp.RouteTarget{
			{ConnectorID: secondary, Priority: 2},
			{ConnectorID: primary, Priority: 1},
		}}
}

// TestFailoverPriorityExcludesOpen: an open primary is skipped for the healthy secondary; once the
// primary recovers (closed) and availability is refreshed, it is readmitted.
func TestFailoverPriorityExcludesOpen(t *testing.T) {
	ctx := context.Background()
	primary, secondary := uuid.New(), uuid.New()
	r, _ := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{failoverRoute(primary, secondary)}})

	avail := &fakeAvail{open: map[uuid.UUID]bool{primary: true}}
	if err := r.RefreshAvailability(ctx, avail); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, err := r.Resolve(ctx, "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != secondary {
		t.Errorf("open primary routed to %s, want the healthy secondary %s", got.ConnectorID, secondary)
	}

	// Primary recovers → readmitted at the next refresh.
	avail.open = map[uuid.UUID]bool{}
	if err := r.RefreshAvailability(ctx, avail); err != nil {
		t.Fatalf("refresh 2: %v", err)
	}
	got, err = r.Resolve(ctx, "+2250700000001")
	if err != nil {
		t.Fatalf("resolve 2: %v", err)
	}
	if got.ConnectorID != primary {
		t.Errorf("recovered primary routed to %s, want %s (readmitted)", got.ConnectorID, primary)
	}
}

// TestLeastLoadedExcludesOpen: the least-loaded connector is skipped when its breaker is open, even
// though it has the smaller load.
func TestLeastLoadedExcludesOpen(t *testing.T) {
	ctx := context.Background()
	busy, idle := uuid.New(), uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionLeastLoaded,
		MatchDestPattern: ptr("225"),
		Targets:          []cp.RouteTarget{{ConnectorID: busy, Weight: 1}, {ConnectorID: idle, Weight: 1}}}
	r, _ := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{route}})
	r.UseLoadReader(fakeLoad{busy: 100, idle: 3})
	if err := r.RefreshAvailability(ctx, &fakeAvail{open: map[uuid.UUID]bool{idle: true}}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	got, err := r.Resolve(ctx, "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != busy {
		t.Errorf("routed to %s, want the loaded-but-healthy connector %s (idle one is open)", got.ConnectorID, busy)
	}
}

// TestResolveDoesNotReadAvailabilityPerMessage: the reader is consulted only at RefreshAvailability, so
// many resolutions add zero reads — the hot path never touches Redis (§12).
func TestResolveDoesNotReadAvailabilityPerMessage(t *testing.T) {
	ctx := context.Background()
	primary, secondary := uuid.New(), uuid.New()
	r, _ := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{failoverRoute(primary, secondary)}})
	avail := &fakeAvail{open: map[uuid.UUID]bool{primary: true}}
	if err := r.RefreshAvailability(ctx, avail); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if avail.calls != 1 {
		t.Fatalf("reader calls after one refresh = %d, want 1", avail.calls)
	}

	for i := 0; i < 100; i++ {
		if _, err := r.Resolve(ctx, "+2250700000001"); err != nil {
			t.Fatalf("resolve %d: %v", i, err)
		}
	}
	if avail.calls != 1 {
		t.Errorf("reader calls after 100 resolutions = %d, want still 1 (no per-message read)", avail.calls)
	}
}

// TestAllTargetsOpenYieldsNoRoute: when every target is open and there is no fallback, the route retains
// no selectable connector — resolution fails (parking/reroute is a later step, 125/126).
func TestAllTargetsOpenYieldsNoRoute(t *testing.T) {
	ctx := context.Background()
	primary, secondary := uuid.New(), uuid.New()
	r, _ := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{failoverRoute(primary, secondary)}})
	if err := r.RefreshAvailability(ctx, &fakeAvail{open: map[uuid.UUID]bool{primary: true, secondary: true}}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	_, err := r.Resolve(ctx, "+2250700000001")
	if !errors.Is(err, errs.ErrNoRoute) {
		t.Errorf("all-open resolve error = %v, want ErrNoRoute", err)
	}
}

// TestReaderErrorKeepsLastAvailability: a refresh whose reader errors returns the error but leaves the
// previous availability intact — the fleet is not blackholed by a transient Redis blip.
func TestReaderErrorKeepsLastAvailability(t *testing.T) {
	ctx := context.Background()
	primary, secondary := uuid.New(), uuid.New()
	r, _ := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{failoverRoute(primary, secondary)}})
	if err := r.RefreshAvailability(ctx, &fakeAvail{open: map[uuid.UUID]bool{primary: true}}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if err := r.RefreshAvailability(ctx, errAvail{}); err == nil {
		t.Fatal("erroring refresh returned nil, want the reader error")
	}

	// Primary must still be excluded (previous availability preserved).
	got, err := r.Resolve(ctx, "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != secondary {
		t.Errorf("after a failed refresh routed to %s, want %s (last availability kept)", got.ConnectorID, secondary)
	}
}

// TestWeightedIgnoresBreaker: step-123 scopes breaker filtering to failover_priority/least_loaded; the
// deterministic-by-key strategies (weighted/hash_based) are unchanged, so an open connector can still be
// picked. This pins that scope decision.
func TestWeightedIgnoresBreaker(t *testing.T) {
	ctx := context.Background()
	only := uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionWeighted,
		MatchDestPattern: ptr("225"),
		Targets:          []cp.RouteTarget{{ConnectorID: only, Weight: 1}}}
	r, _ := routing.LoadSnapshot(ctx, fakeLister{routes: []cp.Route{route}})
	if err := r.RefreshAvailability(ctx, &fakeAvail{open: map[uuid.UUID]bool{only: true}}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	got, err := r.Resolve(ctx, "+2250700000001")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.ConnectorID != only {
		t.Errorf("weighted routed to %s, want %s (breaker filtering is out of scope for weighted)", got.ConnectorID, only)
	}
}
