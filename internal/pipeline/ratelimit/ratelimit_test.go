package ratelimit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestLimiterAtomicUnderConcurrency is the core guarantee: with the clock frozen (no refill), a bucket
// of capacity K admits EXACTLY K unit-cost calls no matter how many goroutines race for them — the Lua
// script serialises the refill-and-consume, so two callers can never both spend the last token. Run
// under -race, it also proves no data race in the limiter itself.
func TestLimiterAtomicUnderConcurrency(t *testing.T) {
	rdb := redistest.Client(t)
	frozen := time.Now()
	lim := ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return frozen }))

	const capacity, goroutines = 50, 200
	entityID := uuid.NewString()
	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lim.Allow(context.Background(), "connector", entityID, "sec", 1000, capacity, 1).Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()

	if allowed != capacity {
		t.Errorf("allowed = %d, want exactly the capacity %d (atomic bucket must not over-admit under concurrency)", allowed, capacity)
	}
}

// TestLimiterRefillsOverTime: an exhausted bucket admits again once enough time has elapsed for the
// refill (rate tokens/second). The clock is injected, so the refill is driven deterministically.
func TestLimiterRefillsOverTime(t *testing.T) {
	rdb := redistest.Client(t)
	now := time.Now()
	lim := ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return now }))

	entityID := uuid.NewString()
	// Capacity 2, rate 10/sec. Drain the two tokens.
	for i := 0; i < 2; i++ {
		if !lim.Allow(context.Background(), "connector", entityID, "sec", 10, 2, 1).Allowed {
			t.Fatalf("call %d should be admitted from a full bucket", i+1)
		}
	}
	if d := lim.Allow(context.Background(), "connector", entityID, "sec", 10, 2, 1); d.Allowed || d.FailClosed {
		t.Fatalf("a drained bucket must deny WITHOUT failing closed (Redis is up): %+v", d)
	}
	// 100ms at 10 tokens/sec refills exactly one token.
	now = now.Add(100 * time.Millisecond)
	if !lim.Allow(context.Background(), "connector", entityID, "sec", 10, 2, 1).Allowed {
		t.Error("after a refill interval the bucket must admit again")
	}
}

// TestLimiterCostIsSegments: a message occupying several segments consumes that many tokens at once, so
// a long message counts as the several SMS it really is (step-082 cost).
func TestLimiterCostIsSegments(t *testing.T) {
	rdb := redistest.Client(t)
	frozen := time.Now()
	lim := ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return frozen }))

	entityID := uuid.NewString()
	// Capacity 5. A 3-segment message leaves 2 tokens; a following 3-segment message is then denied.
	d := lim.Allow(context.Background(), "connector", entityID, "sec", 100, 5, 3)
	if !d.Allowed || d.Remaining != 2 {
		t.Fatalf("3-segment call = %+v, want allowed with 2 tokens left", d)
	}
	if lim.Allow(context.Background(), "connector", entityID, "sec", 100, 5, 3).Allowed {
		t.Error("a second 3-segment message must be denied — only 2 tokens remain")
	}
}

// TestLimiterFailsClosedWhenRedisDown: with Redis unreachable, the limiter must NOT fail open — it falls
// back to a per-pod static ceiling that still bounds admissions, and marks the decision.
func TestLimiterFailsClosedWhenRedisDown(t *testing.T) {
	// A client pointed at a dead address: every command errors with a dial failure. Retries are off and
	// the dial timeout short so the test does not spend seconds in go-redis backoff.
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = dead.Close() })
	frozen := time.Now()
	lim := ratelimit.NewLimiter(dead, ratelimit.WithClock(func() time.Time { return frozen }))

	const capacity, calls = 5, 20
	entityID := uuid.NewString()
	allowed := 0
	for i := 0; i < calls; i++ {
		d := lim.Allow(context.Background(), "connector", entityID, "sec", 1000, capacity, 1)
		if !d.FailClosed {
			t.Fatalf("call %d: decision must be marked fail-closed when Redis is down", i+1)
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed == calls {
		t.Fatal("the limiter failed OPEN under a Redis outage — every call was admitted")
	}
	if allowed != capacity {
		t.Errorf("fail-closed admitted %d, want the local ceiling %d (frozen clock, no refill)", allowed, capacity)
	}
}

// TestLimiterEdgeCapacities: a message larger than the burst capacity, and a zero capacity, both deny
// permanently — never a partial consume, never fail-open (Redis is up, so no fail-closed either).
func TestLimiterEdgeCapacities(t *testing.T) {
	rdb := redistest.Client(t)
	frozen := time.Now()
	lim := ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return frozen }))

	// cost > capacity: a 5-segment message against a burst of 3 can never be admitted.
	if d := lim.Allow(context.Background(), "connector", uuid.NewString(), "sec", 100, 3, 5); d.Allowed || d.FailClosed {
		t.Errorf("cost>capacity = %+v, want a plain deny (no partial consume, no fail-closed)", d)
	}
	// capacity 0: the bucket caps at 0 tokens, so every message denies.
	if d := lim.Allow(context.Background(), "connector", uuid.NewString(), "sec", 100, 0, 1); d.Allowed || d.FailClosed {
		t.Errorf("capacity=0 = %+v, want a plain deny", d)
	}
}

// TestLimiterWindowsAreIsolated: two windows for the same entity are INDEPENDENT buckets — exhausting
// the query_sm budget leaves the submit_sm (sec) budget untouched (step-087 isolation).
func TestLimiterWindowsAreIsolated(t *testing.T) {
	rdb := redistest.Client(t)
	frozen := time.Now()
	lim := ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return frozen }))
	account := uuid.NewString()

	// Drain the query_sm bucket (capacity 2).
	for i := 0; i < 2; i++ {
		if !lim.Allow(context.Background(), "smpp_account", account, "query_sm", 2, 2, 1).Allowed {
			t.Fatalf("query_sm call %d should be admitted from a full bucket", i+1)
		}
	}
	if lim.Allow(context.Background(), "smpp_account", account, "query_sm", 2, 2, 1).Allowed {
		t.Fatal("the query_sm bucket should be exhausted")
	}
	// The submit (sec) bucket for the SAME account is untouched — a query flood cannot eat the send budget.
	if !lim.Allow(context.Background(), "smpp_account", account, "sec", 2, 2, 1).Allowed {
		t.Error("the submit_sm (sec) bucket must be independent of the exhausted query_sm bucket")
	}
}
