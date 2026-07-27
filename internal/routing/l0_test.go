package routing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
)

// fakeExact is a scripted L0 lookup: it returns the target set for a MSISDN, or (false) for any other,
// or a transient error when errFor matches.
type fakeExact struct {
	hits   map[string]exact.Target
	errFor string
}

func (f fakeExact) Resolve(_ context.Context, msisdn string) (exact.Target, bool, error) {
	if msisdn == f.errFor {
		return exact.Target{}, false, errors.New("redis down")
	}
	t, ok := f.hits[msisdn]
	return t, ok, nil
}

// buildSnapshot compiles a one-route declarative snapshot (prefix 225 → declConn, route declRoute).
func buildSnapshot(t *testing.T, declRoute, declConn uuid.UUID) *routing.SnapshotResolver {
	t.Helper()
	snap, err := routing.LoadSnapshot(context.Background(), fakeLister{routes: []cp.Route{
		{ID: declRoute, Priority: 100, DistributionStrategy: cp.DistributionStatic, Status: cp.RouteActive,
			MatchDestPattern: ptr("225"), TargetConnectorID: &declConn},
	}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return snap
}

// TestL0ConnectorHitShortCircuits: an exact override to a connector routes straight to it, bypassing
// the declarative prefix match (invariant: L0 wins over L2).
func TestL0ConnectorHitShortCircuits(t *testing.T) {
	declRoute, declConn, portedConn := uuid.New(), uuid.New(), uuid.New()
	ported := "2250700000001"
	l0 := routing.NewL0Resolver(
		fakeExact{hits: map[string]exact.Target{ported: {Type: exact.TargetConnector, ID: portedConn}}},
		buildSnapshot(t, declRoute, declConn),
	)

	got, err := l0.Resolve(context.Background(), ported)
	if err != nil {
		t.Fatalf("Resolve(ported): %v", err)
	}
	if got.ConnectorID != portedConn {
		t.Errorf("ported routed to %s, want the L0 connector %s (not the declarative %s)", got.ConnectorID, portedConn, declConn)
	}
	if got.RouteID != nil {
		t.Errorf("connector override RouteID = %v, want nil (no route matched)", got.RouteID)
	}
}

// TestL0MissFallsBackToDeclarative: a number with no override resolves through the normal declarative
// chain unchanged.
func TestL0MissFallsBackToDeclarative(t *testing.T) {
	declRoute, declConn := uuid.New(), uuid.New()
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, buildSnapshot(t, declRoute, declConn))

	got, err := l0.Resolve(context.Background(), "2250700000002")
	if err != nil {
		t.Fatalf("Resolve(no override): %v", err)
	}
	if got.ConnectorID != declConn || got.RouteID == nil || *got.RouteID != declRoute {
		t.Errorf("no-override routed to {%s, %v}, want the declarative {%s, %s}", got.ConnectorID, got.RouteID, declConn, declRoute)
	}
}

// TestL0RouteTargetResolvesToConnector: an override to a route resolves to that route's connector via
// the snapshot (target_type=route).
func TestL0RouteTargetResolvesToConnector(t *testing.T) {
	declRoute, declConn := uuid.New(), uuid.New()
	ported := "2250700000003"
	l0 := routing.NewL0Resolver(
		fakeExact{hits: map[string]exact.Target{ported: {Type: exact.TargetRoute, ID: declRoute}}},
		buildSnapshot(t, declRoute, declConn),
	)

	got, err := l0.Resolve(context.Background(), ported)
	if err != nil {
		t.Fatalf("Resolve(route override): %v", err)
	}
	if got.ConnectorID != declConn || got.RouteID == nil || *got.RouteID != declRoute {
		t.Errorf("route override resolved to {%s, %v}, want {%s, %s}", got.ConnectorID, got.RouteID, declConn, declRoute)
	}
}

// TestL0UnresolvableRouteTargetFallsBack: an override to a route absent from the snapshot (deleted /
// disabled) falls back to the declarative chain rather than dropping the message (spec §6.1).
func TestL0UnresolvableRouteTargetFallsBack(t *testing.T) {
	declRoute, declConn := uuid.New(), uuid.New()
	ported := "2250700000004"
	l0 := routing.NewL0Resolver(
		fakeExact{hits: map[string]exact.Target{ported: {Type: exact.TargetRoute, ID: uuid.New()}}}, // unknown route id
		buildSnapshot(t, declRoute, declConn),
	)

	got, err := l0.Resolve(context.Background(), ported)
	if err != nil {
		t.Fatalf("Resolve(dangling route override): %v", err)
	}
	if got.ConnectorID != declConn {
		t.Errorf("dangling override routed to %s, want the declarative fallback %s", got.ConnectorID, declConn)
	}
}

// TestL0LookupFaultSurfaces: a transient exact-lookup fault is returned, not swallowed into a fallback,
// so a message is retried rather than routed on stale assumptions.
func TestL0LookupFaultSurfaces(t *testing.T) {
	declRoute, declConn := uuid.New(), uuid.New()
	bad := "2250700000005"
	l0 := routing.NewL0Resolver(fakeExact{errFor: bad}, buildSnapshot(t, declRoute, declConn))

	if _, err := l0.Resolve(context.Background(), bad); err == nil {
		t.Fatal("Resolve(lookup fault) = nil error, want the fault surfaced")
	}
}
