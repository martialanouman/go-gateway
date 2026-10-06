package routing_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/routing/script"
)

// fakeConnectors serves the priority_tier of each connector; one absent from the map has tier 0.
type fakeConnectors struct{ tiers map[uuid.UUID]int }

func (f fakeConnectors) List(context.Context) ([]cp.Connector, error) {
	out := make([]cp.Connector, 0, len(f.tiers))
	for id, tier := range f.tiers {
		out = append(out, cp.Connector{ID: id, PriorityTier: tier})
	}
	return out, nil
}

const (
	rankMarketing     = 0
	rankTransactional = 1
	rankOTP           = 2
)

func tierResolver(t *testing.T, routes []cp.Route, tiers map[uuid.UUID]int) *routing.SnapshotResolver {
	t.Helper()
	r, err := routing.LoadSnapshot(context.Background(), fakeLister{routes: routes}, fakeConnectors{tiers: tiers})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return r
}

// TestTierSkipsAReservedStaticRouteForItsFallback: a static route to a connector reserved above the
// message's rank retains no target, so the route-level fallback takes it (ADR-0020 §4).
func TestTierSkipsAReservedStaticRouteForItsFallback(t *testing.T) {
	reserved, shared := uuid.New(), uuid.New()
	fallback := cp.Route{ID: uuid.New(), Priority: 200, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID: &shared}
	primary := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		MatchDestPattern: ptr("225"), TargetConnectorID: &reserved, FallbackRouteID: &fallback.ID}
	r := tierResolver(t, []cp.Route{primary, fallback}, map[uuid.UUID]int{reserved: rankOTP})

	for rank, want := range map[int]uuid.UUID{rankMarketing: shared, rankTransactional: shared, rankOTP: reserved} {
		got, err := r.Resolve(context.Background(), "+2250700000000", rank)
		if err != nil {
			t.Fatalf("rank %d: %v", rank, err)
		}
		if got.ConnectorID != want {
			t.Errorf("rank %d routed to %s, want %s", rank, got.ConnectorID, want)
		}
	}
}

// TestTierFiltersTheTargetsAndTheFallbackChain: a reserved target is skipped by the strategy, and the
// fallback_chain the pool reroutes through never offers it to a message below its tier.
func TestTierFiltersTheTargetsAndTheFallbackChain(t *testing.T) {
	reserved, shared := uuid.New(), uuid.New()
	r := tierResolver(t, []cp.Route{failoverRoute(reserved, shared)}, map[uuid.UUID]int{reserved: rankTransactional})

	got, err := r.Resolve(context.Background(), "+2250700000000", rankMarketing)
	if err != nil {
		t.Fatalf("marketing: %v", err)
	}
	if got.ConnectorID != shared || slices.Contains(got.FallbackChain, reserved) {
		t.Errorf("marketing routed to %s with chain %v, want %s without %s", got.ConnectorID, got.FallbackChain, shared, reserved)
	}

	got, err = r.Resolve(context.Background(), "+2250700000000", rankTransactional)
	if err != nil {
		t.Fatalf("transactional: %v", err)
	}
	if got.ConnectorID != reserved || !slices.Contains(got.FallbackChain, shared) {
		t.Errorf("transactional routed to %s with chain %v, want %s then %s", got.ConnectorID, got.FallbackChain, reserved, shared)
	}
}

// TestTierAppliesToEveryStrategy: a distribution strategy that does not read availability still skips a
// reserved target — the filter is the tier, not the breaker.
func TestTierAppliesToEveryStrategy(t *testing.T) {
	reserved, shared := uuid.New(), uuid.New()
	for _, strategy := range []cp.DistributionStrategy{cp.DistributionRoundRobin, cp.DistributionWeighted, cp.DistributionHashBased, cp.DistributionLeastLoaded} {
		route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: strategy,
			MatchDestPattern: ptr("225"), Targets: []cp.RouteTarget{
				{ConnectorID: reserved, Weight: 1}, {ConnectorID: shared, Weight: 1},
			}}
		r := tierResolver(t, []cp.Route{route}, map[uuid.UUID]int{reserved: rankOTP})
		for i := range 8 {
			got, err := r.Resolve(context.Background(), "+225070000000"+string(rune('0'+i)), rankMarketing)
			if err != nil {
				t.Fatalf("%s: %v", strategy, err)
			}
			if got.ConnectorID != shared || slices.Contains(got.FallbackChain, reserved) {
				t.Errorf("%s routed marketing to %s with chain %v, want %s without the reserved connector", strategy, got.ConnectorID, got.FallbackChain, shared)
			}
		}
	}
}

// TestTierLeavesNothingIsNoRoute: no new error code — a message every target refuses has no route.
func TestTierLeavesNothingIsNoRoute(t *testing.T) {
	reserved := uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		MatchDestPattern: ptr("225"), TargetConnectorID: &reserved}
	r := tierResolver(t, []cp.Route{route}, map[uuid.UUID]int{reserved: rankOTP})

	if _, err := r.Resolve(context.Background(), "+2250700000000", rankTransactional); !errors.Is(err, errs.ErrNoRoute) {
		t.Errorf("err = %v, want no_route", err)
	}
}

// TestTierMakesAnL0ConnectorFallThrough: an exact override to a reserved connector is skipped below its
// tier, and resolution falls through to the declarative level (ADR-0004), as for an unknown target.
func TestTierMakesAnL0ConnectorFallThrough(t *testing.T) {
	declRoute, declConn, reserved := uuid.New(), uuid.New(), uuid.New()
	ported := "2250700000001"
	decl := tierResolver(t, []cp.Route{{ID: declRoute, Priority: 100, DistributionStrategy: cp.DistributionStatic,
		Status: cp.RouteActive, MatchDestPattern: ptr("225"), TargetConnectorID: &declConn}},
		map[uuid.UUID]int{reserved: rankOTP})
	l0 := routing.NewL0Resolver(
		fakeExact{hits: map[string]exact.Target{ported: {Type: exact.TargetConnector, ID: reserved}}}, nil, decl)

	for rank, want := range map[int]uuid.UUID{rankTransactional: declConn, rankOTP: reserved} {
		got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{Dest: ported, Rank: rank})
		if err != nil {
			t.Fatalf("rank %d: %v", rank, err)
		}
		if got.ConnectorID != want {
			t.Errorf("rank %d routed to %s, want %s", rank, got.ConnectorID, want)
		}
	}
}

// TestTierReachesEveryResolutionLevel: the rank travels with the request past L0 — a script's route and
// the declarative match both skip a reserved connector below its tier, and take it at or above.
func TestTierReachesEveryResolutionLevel(t *testing.T) {
	reserved, shared := uuid.New(), uuid.New()
	fallback := cp.Route{ID: uuid.New(), Priority: 200, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID: &shared}
	reservedRoute := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		MatchDestPattern: ptr("225"), TargetConnectorID: &reserved, FallbackRouteID: &fallback.ID}
	decl := tierResolver(t, []cp.Route{reservedRoute, fallback}, map[uuid.UUID]int{reserved: rankOTP})
	scripts, err := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "p", Language: script.LanguageJS, Scope: script.ScopePlatform, Source: jsReturning(reservedRoute.ID)},
	}}, nil)
	if err != nil {
		t.Fatalf("BuildScriptSnapshot: %v", err)
	}
	for level, l0 := range map[string]*routing.L0Resolver{
		"script":      routing.NewL0Resolver(fakeExact{}, routing.NewScriptResolver(scripts, nil, nil), decl),
		"declarative": routing.NewL0Resolver(fakeExact{}, nil, decl),
	} {
		for rank, want := range map[int]uuid.UUID{rankTransactional: shared, rankOTP: reserved} {
			got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{Dest: "+2250700000000", Rank: rank, Segments: 1})
			if err != nil {
				t.Fatalf("%s rank %d: %v", level, rank, err)
			}
			if got.ConnectorID != want {
				t.Errorf("%s rank %d routed to %s, want %s", level, rank, got.ConnectorID, want)
			}
		}
	}
}

type loadByConnector map[uuid.UUID]int

func (l loadByConnector) InFlight(_ context.Context, id uuid.UUID) int { return l[id] }

// TestTierSkipsAnIdleReservedConnectorForLeastLoaded: the reserved connector is the least loaded, so only
// the tier keeps a marketing message off it.
func TestTierSkipsAnIdleReservedConnectorForLeastLoaded(t *testing.T) {
	reserved, shared := uuid.New(), uuid.New()
	route := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionLeastLoaded,
		MatchDestPattern: ptr("225"), Targets: []cp.RouteTarget{{ConnectorID: reserved}, {ConnectorID: shared}}}
	r := tierResolver(t, []cp.Route{route}, map[uuid.UUID]int{reserved: rankOTP})
	r.UseLoadReader(loadByConnector{shared: 100})

	got, err := r.Resolve(context.Background(), "+2250700000000", rankMarketing)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.ConnectorID != shared {
		t.Errorf("marketing routed to the idle reserved connector %s, want %s", got.ConnectorID, shared)
	}
}

// TestTierHoldsAlongTheRouteFallback: the route-level fallback is resolved at the message's rank too, so
// a fallback reserved for transactional still takes a transactional message.
func TestTierHoldsAlongTheRouteFallback(t *testing.T) {
	otpOnly, transactionalUp := uuid.New(), uuid.New()
	fallback := cp.Route{ID: uuid.New(), Priority: 200, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID: &transactionalUp}
	primary := cp.Route{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
		MatchDestPattern: ptr("225"), TargetConnectorID: &otpOnly, FallbackRouteID: &fallback.ID}
	r := tierResolver(t, []cp.Route{primary, fallback}, map[uuid.UUID]int{otpOnly: rankOTP, transactionalUp: rankTransactional})

	got, err := r.Resolve(context.Background(), "+2250700000000", rankTransactional)
	if err != nil || got.ConnectorID != transactionalUp {
		t.Errorf("transactional routed to %s (err %v), want the fallback %s", got.ConnectorID, err, transactionalUp)
	}
}
