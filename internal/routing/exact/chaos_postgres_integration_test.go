package exact_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestExactRouteFailsClosedWhenPostgresIsCut is the acceptance test for the durable leg step-250e adds
// to the L0 row of the failure-policy matrix (guide de codage §16).
//
// Until this step the L0 read had one leg, Redis, and step-250d proved its policy on a genuinely cut
// Redis. The read-through gives it a second: a Bloom possible-hit that the cache does not hold now asks
// Postgres. The policy must be the same, for the same reason — an exact route that cannot be reached
// must not degrade into default routing, which for a ported number means its former operator. The
// repo's own rule is that nothing is written into §16 before it has its test; this is that test.
//
// Uncoded is the load-bearing half of the assertion. router.handle commits the Kafka offset for a coded
// error (a definitive reject, message buried) and leaves it uncommitted for an uncoded one, which is
// what buys the redelivery.
//
// This test lives in the external test package on purpose: internal/storage/postgres imports this one,
// so an in-package test could not reach the repo without an import cycle.
func TestExactRouteFailsClosedWhenPostgresIsCut(t *testing.T) {
	ctx := context.Background()
	rdb := redistest.Client(t)

	// Seed and read back durable state through an UNCUT pool: through the cut one the verification
	// would die with the dependency and pass by observing nothing.
	seedRepo := postgres.NewExactRouteRepo(pgtest.Pool(t))
	ported := fmt.Sprintf("22507%08d", uuid.New().ID()%100_000_000)
	want := exact.Target{Type: exact.TargetConnector, ID: uuid.New()}
	if _, err := seedRepo.Upsert(ctx, exact.Route{
		MSISDN: ported, Target: want, Source: exact.SourceMNPImport,
	}); err != nil {
		t.Fatalf("seed the exact route: %v", err)
	}

	// The Bloom is built while the link is up, from the uncut pool, exactly as router-svc builds it at
	// boot. A number the Bloom rejects is answered with no network call at all, so it would make the cut
	// invisible and this test would pass against a healthy Postgres.
	bloom, err := exact.LoadBloom(ctx, seedRepo)
	if err != nil {
		t.Fatalf("LoadBloom: %v", err)
	}

	cutPool, proxy := pgtest.Cuttable(t)
	resolver := exact.NewResolver(bloom, rdb, postgres.NewExactRouteRepo(cutPool), exact.DefaultCacheTTL)
	cacheKey := "exactroute:{" + ported + "}"
	clearCache := func() {
		if err := rdb.Del(ctx, cacheKey).Err(); err != nil {
			t.Fatalf("clear the cache key: %v", err)
		}
	}

	// Control, with Postgres up: the cold lookup resolves off the durable table. Without it we could not
	// tell a fail-closed resolver from a harness that never reached Postgres in the first place.
	clearCache()
	got, ok, err := resolver.Resolve(ctx, ported)
	if err != nil || !ok || got != want {
		t.Fatalf("with postgres up Resolve(%s) = (%v, %v, %v), want the seeded target", ported, got, ok, err)
	}

	// That control just populated the cache. Leaving it would let the next resolve answer from Redis and
	// never reach the cut dependency — the test would pass while proving nothing.
	clearCache()

	proxy.Cut()
	_, ok, err = resolver.Resolve(ctx, ported)
	if err == nil || ok {
		t.Fatalf("with postgres cut Resolve(%s) = (ok=%v, err=%v), want a transient fault; answering "+
			"(false, nil) is indistinguishable from \"no override\" and would route a ported number "+
			"by prefix, to its former operator", ported, ok, err)
	}
	if _, coded := errs.CodeOf(err); coded {
		t.Errorf("Resolve error is coded (%v); a coded error commits the offset and buries the message, "+
			"but an unreachable exact route is transient and must be redelivered", err)
	}

	// No latch: the redelivery bought by that uncoded error must succeed once the link is back.
	proxy.Resume()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, ok, err = resolver.Resolve(ctx, ported)
		if err == nil && ok && got == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after resume Resolve(%s) = (%v, %v, %v), want the seeded target — the resolver "+
				"latched on the outage", ported, got, ok, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
