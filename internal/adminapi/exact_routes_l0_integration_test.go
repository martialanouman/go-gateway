package adminapi_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/async"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// routeLister feeds LoadSnapshot the one declarative route this test needs.
type routeLister struct{ routes []cp.Route }

func (l routeLister) List(context.Context) ([]cp.Route, error) { return l.routes, nil }

// TestExactRouteCreatedByAdminResolvesThroughL0 is the acceptance test of step-250e, and the one that
// would have caught the defect: it drives the WHOLE chain — Admin API, Postgres, the Bloom rebuilt from
// Postgres, Redis, and the three-level resolver — with no double standing in for the exact package.
//
// That is the point. Every existing L0 test injects a fakeExact, which replaces the package wholesale;
// every exact-package test seeds the Redis key itself. Both pass against a gateway in which nothing
// ever writes that key — which is exactly the gateway that shipped. Number portability was broken in
// production while two layers of green tests described it working.
//
// The declarative route deliberately matches the same number and points ELSEWHERE. Without that, the
// assertion would hold for a resolver that ignored L0 entirely, and the test would be as hollow as the
// ones it replaces: on the pre-step code the number resolves to declConn, and this fails.
func TestExactRouteCreatedByAdminResolvesThroughL0(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	ctx := context.Background()

	repo := postgres.NewExactRouteRepo(pool)
	api := newTestAPIWith(t, adminapi.Deps{
		ExactRoutes:     repo,
		ExactRouteCache: exact.NewInvalidator(rdb),
	})

	// Unique per run: both containers are shared across this package's tests.
	ported := uniqueMSISDN()
	portedConn, declConn, declRoute := uuid.New(), uuid.New(), uuid.New()

	// The operator publishes the MNP override.
	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"msisdn":%q,"target_type":"connector","target_id":%q}`, ported, portedConn)
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", w.Code, w.Body)
	}

	// The data plane learns of it the way it does in production: router-svc rebuilds the Bloom from
	// Postgres on the config-change notification.
	l0 := newL0(ctx, t, rdb, repo, declRoute, declConn)

	got, err := l0.Resolve(ctx, pipeline.RouteRequest{Dest: ported})
	if err != nil {
		t.Fatalf("Resolve(%s): %v", ported, err)
	}
	if got.ConnectorID != portedConn {
		t.Fatalf("ported number routed to %s, want the L0 target %s; routing to the declarative %s is "+
			"precisely the production defect — a ported number sent to its former operator",
			got.ConnectorID, portedConn, declConn)
	}

	// The read-through must also have populated the cache, or every message to this number would keep
	// paying a Postgres lookup.
	if _, err := rdb.Get(ctx, "exactroute:{"+ported+"}").Result(); err != nil {
		t.Errorf("cache key after the first resolve: %v, want it populated", err)
	}

	// Deleting through the Admin API must clear the cache. The Bloom is deliberately NOT rebuilt here:
	// it still admits the number, which is the realistic case (the Bloom lags a mutation by up to two
	// 250ms coalescing windows). Only the DEL stands between a deleted route and a message still being
	// sent to the target the operator just removed.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/exact-routes/"+ported, ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204; body=%s", w.Code, w.Body)
	}
	if err := rdb.Get(ctx, "exactroute:{"+ported+"}").Err(); err != goredis.Nil {
		t.Errorf("cache key after delete: err=%v, want redis.Nil — a key left behind keeps routing to a "+
			"target the operator removed, and it carries no TTL from the admin side", err)
	}

	got, err = l0.Resolve(ctx, pipeline.RouteRequest{Dest: ported})
	if err != nil {
		t.Fatalf("Resolve after delete: %v", err)
	}
	if got.ConnectorID != declConn {
		t.Errorf("after delete the number routed to %s, want the declarative %s", got.ConnectorID, declConn)
	}
}

// TestExactRouteUpdatedByAdminResolvesToTheNewTarget: a re-ported number must follow its new carrier
// on the next message, not when a TTL happens to expire. Same stale-Bloom setup as the delete case —
// the Bloom is unchanged by an update anyway, since the msisdn set did not move.
func TestExactRouteUpdatedByAdminResolvesToTheNewTarget(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	ctx := context.Background()

	repo := postgres.NewExactRouteRepo(pool)
	api := newTestAPIWith(t, adminapi.Deps{
		ExactRoutes:     repo,
		ExactRouteCache: exact.NewInvalidator(rdb),
	})

	ported := uniqueMSISDN()
	firstConn, secondConn := uuid.New(), uuid.New()
	declConn, declRoute := uuid.New(), uuid.New()

	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"msisdn":%q,"target_type":"connector","target_id":%q}`, ported, firstConn)
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", w.Code, w.Body)
	}

	l0 := newL0(ctx, t, rdb, repo, declRoute, declConn)
	if got, err := l0.Resolve(ctx, pipeline.RouteRequest{Dest: ported}); err != nil || got.ConnectorID != firstConn {
		t.Fatalf("first resolve = (%v, %v), want the first carrier %s", got.ConnectorID, err, firstConn)
	}
	// The cache now holds firstConn. Without the invalidation below, that is what the next resolve
	// returns — for a full TTL.

	w = httptest.NewRecorder()
	body = fmt.Sprintf(`{"target_type":"connector","target_id":%q}`, secondConn)
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/exact-routes/"+ported, body))
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
	}

	got, err := l0.Resolve(ctx, pipeline.RouteRequest{Dest: ported})
	if err != nil {
		t.Fatalf("resolve after update: %v", err)
	}
	if got.ConnectorID != secondConn {
		t.Errorf("after re-porting, routed to %s, want the new carrier %s", got.ConnectorID, secondConn)
	}
}

// TestExactRouteBulkImportIsReflectedByTheBloomAndResolvesThroughL0 is the DoD case the fiche states as
// "l'import de masse remplit la table (et le Bloom la reflète)". It runs against the REAL async runner,
// because the defect it guards lives in the asynchrony: the config-change announcement used to fire when
// the handler returned 202, before BulkUpsert had committed, so the router rebuilt its Bloom from a table
// that did not hold the rows. A syncRunner collapses exactly the gap that made that possible.
func TestExactRouteBulkImportIsReflectedByTheBloomAndResolvesThroughL0(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	ctx := context.Background()

	repo := postgres.NewExactRouteRepo(pool)
	runner := async.New(1, nil)
	// "Aucune goroutine sans condition d'arrêt": the job outlives the request, so the runner has to be
	// drained before the test returns.
	t.Cleanup(func() { _ = runner.Close(context.Background()) })
	// The announcement is what makes the fleet rebuild, so the double rebuilds AT that instant — which
	// is the whole assertion. Capturing the Bloom afterwards instead would let the commit land in
	// between, and the test would pass against an announcement fired before BulkUpsert ran (verified by
	// mutation: it did).
	rebuilt := make(chan *exact.Bloom, 1)
	api := newTestAPIWith(t, adminapi.Deps{
		ExactRoutes:     repo,
		ExactRouteCache: exact.NewInvalidator(rdb),
		ConfigChanges:   bloomRebuilder{repo: repo, out: rebuilt},
		Imports:         runner,
	})

	ported, other := uniqueMSISDN(), uniqueMSISDN()
	portedConn, otherConn := uuid.New(), uuid.New()
	declConn, declRoute := uuid.New(), uuid.New()

	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"source":"mnp_import","rows":[
		{"msisdn":%q,"target_type":"connector","target_id":%q},
		{"msisdn":%q,"target_type":"connector","target_id":%q}]}`,
		ported, portedConn, other, otherConn)
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes/import", body))
	if w.Code != http.StatusAccepted {
		t.Fatalf("import status = %d, want 202; body=%s", w.Code, w.Body)
	}

	var bloom *exact.Bloom
	select {
	case bloom = <-rebuilt:
	case <-time.After(10 * time.Second):
		t.Fatal("the import never announced its config change; the fleet would rebuild its Bloom from a " +
			"table without the imported rows, and L0 would never resolve for them")
	}

	// The filter built at announcement time must already know the imported numbers. This is the DoD
	// case "l'import de masse remplit la table (et le Bloom la reflète)": a Bloom that answers "no" is
	// definitive and costs no network call, so neither the cache nor the durable read is ever reached.
	for _, msisdn := range []string{ported, other} {
		if !bloom.MightContain(msisdn) {
			t.Fatalf("the Bloom rebuilt on the import's announcement does not know %s: the announcement "+
				"outran the commit, and L0 will never resolve for the imported numbers", msisdn)
		}
	}

	l0 := newL0WithBloom(ctx, t, bloom, rdb, repo, declRoute, declConn)
	for _, tc := range []struct {
		msisdn string
		want   uuid.UUID
	}{{ported, portedConn}, {other, otherConn}} {
		got, err := l0.Resolve(ctx, pipeline.RouteRequest{Dest: tc.msisdn})
		if err != nil {
			t.Fatalf("Resolve(%s): %v", tc.msisdn, err)
		}
		if got.ConnectorID != tc.want {
			t.Errorf("imported number %s routed to %s, want %s (declarative is %s)",
				tc.msisdn, got.ConnectorID, tc.want, declConn)
		}
	}
}

// bloomRebuilder models what the announcement actually causes: router-svc reloads its Bloom from
// Postgres. Building it here, inside Announce, is what makes the ordering observable.
type bloomRebuilder struct {
	repo *postgres.ExactRouteRepo
	out  chan *exact.Bloom
}

func (b bloomRebuilder) Publish(ctx context.Context, _ string, _ []byte) error {
	// Derived from the announcement's context but WITHOUT its deadline. In production the announcement
	// is a PUBLISH bounded at five seconds, and the rebuild it triggers runs elsewhere on router-svc's
	// own clock; paginating the whole shared table under the publish budget would make a loaded CI fail
	// with a message about an announcement that did in fact happen.
	bloom, err := exact.LoadBloom(context.WithoutCancel(ctx), b.repo)
	if err != nil {
		return err
	}
	select {
	case b.out <- bloom:
	default:
	}
	return nil
}

// newL0 builds the production three-level resolver: a Bloom loaded from Postgres exactly as router-svc
// loads it, the real read-through resolver over the real Redis, and a declarative route that matches
// the same 225 prefix but points at declConn.
func newL0(ctx context.Context, t *testing.T, rdb *goredis.Client, repo *postgres.ExactRouteRepo,
	declRoute, declConn uuid.UUID,
) *routing.L0Resolver {
	t.Helper()

	bloom, err := exact.LoadBloom(ctx, repo)
	if err != nil {
		t.Fatalf("LoadBloom: %v", err)
	}
	return newL0WithBloom(ctx, t, bloom, rdb, repo, declRoute, declConn)
}

// newL0WithBloom is newL0 over a filter the caller already holds — the case where the moment the Bloom
// was built is itself the thing under test.
func newL0WithBloom(ctx context.Context, t *testing.T, bloom *exact.Bloom, rdb *goredis.Client,
	repo *postgres.ExactRouteRepo, declRoute, declConn uuid.UUID,
) *routing.L0Resolver {
	t.Helper()

	decl, err := routing.LoadSnapshot(ctx, routeLister{routes: []cp.Route{{
		ID: declRoute, Priority: 100, DistributionStrategy: cp.DistributionStatic, Status: cp.RouteActive,
		MatchDestPattern: strptr("225"), TargetConnectorID: &declConn,
	}}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return routing.NewL0Resolver(exact.NewResolver(bloom, rdb, repo, time.Hour), nil, decl)
}

func strptr(s string) *string { return &s }

// uniqueMSISDN returns a digits-only E.164 number under the 225 prefix the declarative route matches.
// It must be digits: the Admin API normalizes and validates, unlike the Bloom and Redis, where the
// package's own tests get away with hex-flavoured MSISDNs.
// A Cote dIvoire mobile number is 225 + 10 digits, and phonenumbers.IsValidNumber enforces that
// length: a 14-digit number is rejected as surely as a lettered one.
func uniqueMSISDN() string { return fmt.Sprintf("22507%08d", uuid.New().ID()%100_000_000) }
