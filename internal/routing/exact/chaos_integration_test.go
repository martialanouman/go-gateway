package exact

import (
	"context"
	"testing"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestExactRouteFailsClosedWhenRedisIsCut is the step-250d acceptance test for the sixth row of the
// failure-policy matrix (guide de codage §16): "Redis (routage L0, numéro exact) -> fail-closed en
// rejeu : erreur non codée → offset non commité → redélivrance".
//
// The row was written in step-250 and tested by nobody. What stood in for it was a fake whose Resolve
// returned errors.New("redis down") (l0_test.go:23) — and that fake replaces the whole exact package,
// so neither the Bloom gate, nor redisKey, nor the goredis.Nil discrimination, nor the %w wrapping on
// resolver.go:49 was ever exercised. Same shape as an outage, none of its contract.
//
// The seeding is not scaffolding, it is the test. A Bloom miss is a definitive "no override" answered
// with no network call at all (resolver.go:41-43), so an MSISDN outside the filter makes the cut
// invisible and the test would pass identically against a healthy Redis. Only a number the Bloom
// admits reaches Redis, and only that number can prove anything here.
func TestExactRouteFailsClosedWhenRedisIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	ctx := context.Background()

	// A unique number per run: the container is shared across this package's tests.
	ported := "22507" + uuid.NewString()[:8]
	unknown := "22508" + uuid.NewString()[:8] // deliberately NOT in the Bloom
	want := Target{Type: TargetConnector, ID: uuid.New()}

	if err := rdb.Set(ctx, redisKey(ported), EncodeTarget(want), 0).Err(); err != nil {
		t.Fatalf("seed the exact route: %v", err)
	}
	resolver := NewResolver(newBloom([]string{ported}), rdb)

	// Control, with Redis up: the override resolves. Without it we could not tell a fail-closed
	// resolver from a harness that never reached Redis in the first place.
	got, ok, err := resolver.Resolve(ctx, ported)
	if err != nil || !ok {
		t.Fatalf("with redis up Resolve(%s) = (%v, %v, %v), want the seeded target", ported, got, ok, err)
	}
	if got != want {
		t.Fatalf("resolved target = %v, want %v", got, want)
	}

	proxy.Cut()

	// The outage must SURFACE. The failure to guard against is not a wrong answer, it is a silent
	// (_, false, nil) — indistinguishable from "no override", which sends the caller down declarative
	// prefix routing. For a ported number that is the wrong operator, which is the entire reason the
	// L0 short-circuit exists (spec §6.1).
	_, ok, err = resolver.Resolve(ctx, ported)
	if err == nil {
		t.Fatalf("with redis cut Resolve returned (ok=%v, nil): an unreachable lookup read as \"no "+
			"override\" degrades a ported number to prefix routing, and prefix routing sends it to the "+
			"operator the number used to belong to", ok)
	}

	// The assertion that decides the message's fate, and the reason §16 says "en rejeu". router.handle
	// branches on exactly this: a coded error becomes a `rejected` CDR row and COMMITS the offset —
	// the message is deliberately turned away — while an uncoded one is treated as a transient fault,
	// leaves the offset uncommitted and is redelivered. Give this failure a code and every message in
	// flight during a Redis blip is permanently rejected instead of retried. The other half of that
	// branch is already pinned by TestRouterRetriesOnBillingTransportError; what this owes is that the
	// error ARRIVES uncoded.
	if code, coded := errs.CodeOf(err); coded {
		t.Errorf("the outage error carries the platform code %q: router.handle turns a coded error into "+
			"a rejected CDR and commits the offset, so every message needing an exact-route lookup during "+
			"a redis blip would be permanently rejected instead of retried", code)
	}

	// And the other ~99% of traffic keeps flowing. A Bloom miss answers without touching Redis, so an
	// exact-route outage must NOT take routing down wholesale — only the numbers that actually need a
	// lookup are held back.
	if _, ok, err := resolver.Resolve(ctx, unknown); err != nil || ok {
		t.Errorf("Resolve(%s) during the outage = (%v, %v), want (false, nil): a number the Bloom "+
			"rejects is a definitive miss that never reads Redis, so the outage must not touch it",
			unknown, ok, err)
	}

	proxy.Resume()

	// Recovery: the redelivery the uncoded error bought now resolves. A resolver that latched would
	// keep sending ported numbers to the wrong operator long after the outage ended.
	got, ok, err = resolver.Resolve(ctx, ported)
	if err != nil || !ok || got != want {
		t.Fatalf("after redis came back Resolve(%s) = (%v, %v, %v), want the seeded target",
			ported, got, ok, err)
	}
}
