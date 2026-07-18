package routing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/routing"
)

type fakeLister struct {
	routes []cp.Route
}

func (f fakeLister) List(context.Context) ([]cp.Route, error) { return f.routes, nil }

func ptr[T any](v T) *T { return &v }

func TestSnapshotLongestPrefixWins(t *testing.T) {
	catchAll := uuid.New()
	specific := uuid.New()
	disabledConn := uuid.New()
	nonStaticConn := uuid.New()

	lister := fakeLister{routes: []cp.Route{
		{ID: uuid.New(), Priority: 100, DistributionStrategy: cp.DistributionStatic, Status: cp.RouteActive,
			TargetConnectorID: &catchAll}, // catch-all (nil dest pattern)
		{ID: uuid.New(), Priority: 100, DistributionStrategy: cp.DistributionStatic, Status: cp.RouteActive,
			MatchDestPattern: ptr("22507"), TargetConnectorID: &specific},
		{ID: uuid.New(), Priority: 1, DistributionStrategy: cp.DistributionStatic, Status: cp.RouteDisabled,
			MatchDestPattern: ptr("225"), TargetConnectorID: &disabledConn}, // disabled: ignored
		{ID: uuid.New(), Priority: 1, DistributionStrategy: cp.DistributionRoundRobin, Status: cp.RouteActive,
			MatchDestPattern: ptr("2250"), TargetConnectorID: &nonStaticConn}, // non-static: ignored in M2
	}}

	resolver, err := routing.LoadSnapshot(context.Background(), lister)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	// A number under the specific prefix routes to the specific connector.
	got, err := resolver.Resolve(context.Background(), "+22507000000")
	if err != nil {
		t.Fatalf("resolve specific: %v", err)
	}
	if got.ConnectorID != specific {
		t.Errorf("specific: got %s want %s", got.ConnectorID, specific)
	}

	// A number outside the specific prefix falls through to the catch-all.
	got, err = resolver.Resolve(context.Background(), "+22501000000")
	if err != nil {
		t.Fatalf("resolve catch-all: %v", err)
	}
	if got.ConnectorID != catchAll {
		t.Errorf("catch-all: got %s want %s", got.ConnectorID, catchAll)
	}
}

func TestSnapshotNoRouteWhenNoCatchAll(t *testing.T) {
	conn := uuid.New()
	lister := fakeLister{routes: []cp.Route{
		{ID: uuid.New(), Priority: 100, DistributionStrategy: cp.DistributionStatic, Status: cp.RouteActive,
			MatchDestPattern: ptr("225"), TargetConnectorID: &conn},
	}}
	resolver, err := routing.LoadSnapshot(context.Background(), lister)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if _, err := resolver.Resolve(context.Background(), "+33123456789"); err != errs.ErrNoRoute {
		t.Fatalf("got %v, want ErrNoRoute", err)
	}
}
