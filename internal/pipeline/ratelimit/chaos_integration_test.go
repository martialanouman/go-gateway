package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestRateLimitFallsBackToTheLocalCeilingWhenRedisIsCut is the step-250 acceptance test for the first
// row of the failure-policy matrix (guide de codage §16): "Redis (rate-limit) -> fail-closed: plafond
// technique statique local du connecteur".
//
// It is the FIRST test in the repo to cut a real Redis. Every prior "redis down" test injects a fake
// that returns errors.New("redis down") (ratelimit_test.go:111 and eight siblings) — which proves the
// branch is reachable, not that the policy holds when the server actually goes away. Here go-redis
// meets a genuinely dead socket, with the production dial/read/write timeouts.
//
// The load-bearing assertion is NOT that the limiter keeps answering: it is that the answer stays
// BOUNDED. "Fail-closed" would be worthless if the fallback admitted everything, and an unbounded
// connector is precisely what the policy exists to prevent. So the outage half spends the local
// bucket to exhaustion and demands a refusal.
func TestRateLimitFallsBackToTheLocalCeilingWhenRedisIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	frozen := time.Now()
	lim := ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return frozen }))

	ctx := context.Background()
	const capacity = 5
	entityID := uuid.NewString()

	// Control: while Redis is up the decision comes from the SHARED bucket. Without this the test could
	// not tell a working fallback from a limiter that never reached Redis in the first place.
	if d := lim.Allow(ctx, "connector", entityID, "sec", 0, capacity, 1); !d.Allowed || d.FailClosed {
		t.Fatalf("with redis up the shared bucket must decide, got allowed=%v fail_closed=%v", d.Allowed, d.FailClosed)
	}

	proxy.Cut()

	// The per-pod ceiling starts full, so it admits exactly `capacity` unit-cost calls with the clock
	// frozen (no refill) and must then refuse. Both halves matter: the admissions prove the limiter did
	// not simply start erroring, the refusal proves the fallback is a CEILING and not a rubber stamp.
	for i := 0; i < capacity; i++ {
		d := lim.Allow(ctx, "connector", entityID, "sec", 0, capacity, 1)
		if !d.Allowed {
			t.Fatalf("call %d: the local ceiling must admit its first %d tokens, got a refusal", i, capacity)
		}
		if !d.FailClosed {
			t.Fatalf("call %d: a decision taken with redis cut must be marked fail-closed", i)
		}
	}
	if d := lim.Allow(ctx, "connector", entityID, "sec", 0, capacity, 1); d.Allowed {
		t.Error("the local ceiling admitted a call beyond its capacity: the outage fallback is unbounded, " +
			"which is the exact failure the fail-closed policy exists to prevent")
	}

	proxy.Resume()

	// Recovery: the shared bucket decides again. A fallback that latched would silently keep every pod
	// on its own ceiling long after the outage, multiplying the real limit by the pod count.
	if d := lim.Allow(ctx, "connector", entityID, "sec", 0, capacity, 1); d.FailClosed {
		t.Error("after redis came back the limiter must return to the shared bucket, still fail-closed")
	}
}
