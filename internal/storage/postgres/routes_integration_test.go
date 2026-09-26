package postgres_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
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

	c1, err := connectors.Create(ctx, cp.NewConnector{Name: "route-c1", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s1", Password: cp.SealedSecret{Sealed: []byte("h"), KMSKeyRef: "test/v1"}})
	if err != nil {
		t.Fatalf("create connector 1: %v", err)
	}
	c2, err := connectors.Create(ctx, cp.NewConnector{Name: "route-c2", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s2", Password: cp.SealedSecret{Sealed: []byte("h"), KMSKeyRef: "test/v1"}})
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

	c, err := connectors.Create(ctx, cp.NewConnector{Name: "route-static-c", Host: "h", Port: 2775, BindType: cp.BindTRX, SystemID: "s", Password: cp.SealedSecret{Sealed: []byte("h"), KMSKeyRef: "test/v1"}})
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

func routeIDs(rs []cp.Route) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.ID)
	}
	return out
}

// TestRouteRepoReorderIsAllOrNothing: the router reads a snapshot at any moment, so a reorder is one
// statement — a reader sees the whole old order or the whole new one — and a refused list moves nothing.
func TestRouteRepoReorderIsAllOrNothing(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	routes := postgres.NewRouteRepo(pool)
	for range 3 {
		if _, err := routes.Create(ctx, cp.NewRoute{Name: "reorder-" + uuid.NewString(), DistributionStrategy: cp.DistributionWeighted}); err != nil {
			t.Fatalf("create route: %v", err)
		}
	}
	all, err := routes.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	reversed := routeIDs(all)
	slices.Reverse(reversed)

	got, err := routes.Reorder(ctx, reversed)
	if err != nil {
		t.Fatalf("Reorder: %v", err)
	}
	if !slices.Equal(routeIDs(got), reversed) {
		t.Fatalf("order = %v, want %v", routeIDs(got), reversed)
	}
	for i, r := range got {
		if r.Priority != (i+1)*10 {
			t.Fatalf("route %d priority = %d, want %d", i, r.Priority, (i+1)*10)
		}
	}

	for name, ids := range map[string][]uuid.UUID{
		"incomplete": reversed[1:],
		"duplicate":  append([]uuid.UUID{reversed[0]}, reversed[:len(reversed)-1]...),
		"unknown":    append([]uuid.UUID{uuid.New()}, reversed[1:]...),
		"empty":      {},
	} {
		if _, err := routes.Reorder(ctx, ids); !errors.Is(err, errs.ErrValidation) {
			t.Errorf("%s list: err = %v, want ErrValidation", name, err)
		}
		if now, _ := routes.List(ctx); !slices.Equal(routeIDs(now), reversed) {
			t.Fatalf("%s list moved routes: order = %v, want still %v", name, routeIDs(now), reversed)
		}
	}

	original := routeIDs(all)
	writing, done := make(chan struct{}), make(chan struct{})
	var mixed []uuid.UUID
	var readErr error
	seen := map[bool]bool{}
	go func() {
		defer close(done)
		for {
			select {
			case <-writing:
				return
			default:
			}
			now, err := routes.List(ctx)
			if err != nil {
				readErr = err
				return
			}
			ids := routeIDs(now)
			if !slices.Equal(ids, original) && !slices.Equal(ids, reversed) {
				mixed = ids
				return
			}
			seen[slices.Equal(ids, original)] = true
		}
	}()
	for i := range 200 {
		order := reversed
		if i%2 == 0 {
			order = original
		}
		if _, err := routes.Reorder(ctx, order); err != nil {
			t.Fatalf("Reorder %d: %v", i, err)
		}
	}
	close(writing)
	<-done
	if readErr != nil || mixed != nil {
		t.Fatalf("concurrent reader: err=%v, saw %v — neither the old order nor the new one", readErr, mixed)
	}
	if !seen[true] || !seen[false] {
		t.Fatalf("the reader saw only one of the two orders (%v): it proved nothing about the switch", seen)
	}
}

// TestRouteRepoReorderRefusedByAConcurrentDeleteMovesNothing: the guard reads the routes when the
// statement starts; a route deleted while the UPDATE waits on its row lock is skipped. Without a rollback
// the other routes would stay renumbered behind a 422 — and a refused request publishes no invalidation.
func TestRouteRepoReorderRefusedByAConcurrentDeleteMovesNothing(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	routes := postgres.NewRouteRepo(pool)
	doomed, err := routes.Create(ctx, cp.NewRoute{Name: "doomed-" + uuid.NewString(), DistributionStrategy: cp.DistributionWeighted})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	before, err := routes.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	order := routeIDs(before)
	slices.Reverse(order)

	deleter, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := deleter.Exec(ctx, `DELETE FROM control_plane.routes WHERE id = $1`, doomed.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := routes.Reorder(ctx, order)
		result <- err
	}()
	time.Sleep(500 * time.Millisecond) // the reorder now waits on the doomed row's lock
	if err := deleter.Commit(ctx); err != nil {
		t.Fatalf("commit delete: %v", err)
	}

	if err := <-result; !errors.Is(err, errs.ErrValidation) {
		t.Fatalf("reorder across a concurrent delete: err = %v, want ErrValidation", err)
	}
	after, err := routes.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, r := range after {
		for _, b := range before {
			if r.ID == b.ID && r.Priority != b.Priority {
				t.Fatalf("route %s moved from %d to %d behind a refused reorder", r.ID, b.Priority, r.Priority)
			}
		}
	}
}
