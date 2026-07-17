package postgres_test

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestRouteRepoWritesTargetsInOneTransaction proves a non-static route and its targets are persisted
// together and read back intact.
func TestRouteRepoWritesTargetsInOneTransaction(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	connectors := postgres.NewConnectorRepo(pool)
	routes := postgres.NewRouteRepo(pool)

	c1, err := connectors.Create(ctx, cp.NewConnector{Name: "route-c1", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s1", PasswordHash: "h"})
	if err != nil {
		t.Fatalf("create connector 1: %v", err)
	}
	c2, err := connectors.Create(ctx, cp.NewConnector{Name: "route-c2", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s2", PasswordHash: "h"})
	if err != nil {
		t.Fatalf("create connector 2: %v", err)
	}

	created, err := routes.Create(ctx, cp.NewRoute{
		Name:                 "balanced",
		DistributionStrategy: cp.DistributionWeighted,
		Targets: []cp.RouteTarget{
			{ConnectorID: c1.ID, Weight: 3, Priority: 0},
			{ConnectorID: c2.ID, Weight: 1, Priority: 1},
		},
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if len(created.Targets) != 2 {
		t.Fatalf("created targets = %d, want 2", len(created.Targets))
	}

	got, err := routes.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if len(got.Targets) != 2 {
		t.Errorf("read targets = %d, want 2 (targets did not persist in the transaction)", len(got.Targets))
	}
	if got.Priority != 100 {
		t.Errorf("priority = %d, want 100 (the schema default)", got.Priority)
	}
}

// TestRouteRepoStaticRouteRoundTrips: a static route names one connector and has no targets.
func TestRouteRepoStaticRouteRoundTrips(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	connectors := postgres.NewConnectorRepo(pool)
	routes := postgres.NewRouteRepo(pool)

	c, err := connectors.Create(ctx, cp.NewConnector{Name: "route-static-c", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s", PasswordHash: "h"})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}

	created, err := routes.Create(ctx, cp.NewRoute{
		Name:                 "direct",
		DistributionStrategy: cp.DistributionStatic,
		TargetConnectorID:    &c.ID,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.TargetConnectorID == nil || *created.TargetConnectorID != c.ID {
		t.Errorf("target_connector_id = %v, want %s", created.TargetConnectorID, c.ID)
	}
	if len(created.Targets) != 0 {
		t.Errorf("static route targets = %d, want 0", len(created.Targets))
	}
}
