package exact

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// fakeStore stands in for the durable exact-route table behind the cache. gets counts calls so a test
// can prove a Redis hit never reaches Postgres — the whole point of the cache.
type fakeStore struct {
	rows map[string]Route
	err  error // when set, every Get returns this transient fault
	gets int
}

func (f *fakeStore) Get(_ context.Context, msisdn string) (Route, bool, error) {
	f.gets++
	if f.err != nil {
		return Route{}, false, f.err
	}
	r, ok := f.rows[msisdn]
	return r, ok, nil
}

// TestResolveRedisMissLoadsFromPostgresAndCaches is the step-250e defect, stated as a test: a number
// present in the durable table must resolve through L0 even though nobody ever wrote its Redis key.
//
// Before this step the key had no writer at all, so a Bloom possible-hit read a missing key, Resolve
// answered (zero, false, nil) — indistinguishable from "no override" — and the message fell back to
// prefix routing. For a ported number that means its former operator.
func TestResolveRedisMissLoadsFromPostgresAndCaches(t *testing.T) {
	msisdn := "2250700000001"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	cache := &fakeRedis{vals: map[string]string{}} // nothing has ever written this key
	store := &fakeStore{rows: map[string]Route{msisdn: {MSISDN: msisdn, Target: want}}}
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour)

	got, ok, err := r.Resolve(context.Background(), msisdn)
	if err != nil || !ok {
		t.Fatalf("Resolve(cold) = (%+v, ok=%v, err=%v), want the durable target", got, ok, err)
	}
	if got != want {
		t.Errorf("target = %+v, want %+v", got, want)
	}

	// The lookup must also have populated the cache, or every message to a ported number would pay a
	// Postgres round-trip forever.
	if cached, seen := cache.vals[redisKey(msisdn)]; !seen || cached != EncodeTarget(want) {
		t.Errorf("cache holds %q (present=%v), want %q", cached, seen, EncodeTarget(want))
	}
}

// TestResolveRedisHitDoesNotTouchPostgres: once cached, the durable store is out of the hot path. The
// counter is the assertion — without it a resolver that read Postgres on every message would pass.
func TestResolveRedisHitDoesNotTouchPostgres(t *testing.T) {
	msisdn := "2250700000001"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	cache := &fakeRedis{vals: map[string]string{redisKey(msisdn): EncodeTarget(want)}}
	store := &fakeStore{rows: map[string]Route{msisdn: {MSISDN: msisdn, Target: want}}}
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour)

	if got, ok, err := r.Resolve(context.Background(), msisdn); err != nil || !ok || got != want {
		t.Fatalf("Resolve(warm) = (%+v, ok=%v, err=%v), want the cached target", got, ok, err)
	}
	if store.gets != 0 {
		t.Errorf("cache hit did %d Postgres lookup(s), want 0", store.gets)
	}
}

// TestResolveBloomFalsePositiveIsNotCached: a Bloom possible-hit with no durable row is a false
// positive. It must fall back to L1/L2 without an error and WITHOUT writing a negative cache entry —
// this step ships no negative caching, and a key written here would be a route to nowhere.
func TestResolveBloomFalsePositiveIsNotCached(t *testing.T) {
	msisdn := "2250700000001"
	cache := &fakeRedis{vals: map[string]string{}}
	store := &fakeStore{rows: map[string]Route{}} // the Bloom admits it; the table does not have it
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour)

	got, ok, err := r.Resolve(context.Background(), msisdn)
	if err != nil || ok {
		t.Fatalf("Resolve(false positive) = (%+v, ok=%v, err=%v), want (zero, false, nil)", got, ok, err)
	}
	if len(cache.vals) != 0 {
		t.Errorf("cache wrote %d key(s) for a false positive, want 0", len(cache.vals))
	}
}

// TestResolvePostgresFaultSurfacesUncoded extends the §16 L0 row to the durable leg the cache now has.
// The reason is the one step-250d recorded for Redis: an exact route that cannot be reached must not
// degrade into default routing, which would send a ported number to the wrong operator. Uncoded is the
// load-bearing half — router.handle commits the Kafka offset for a coded error (a definitive reject)
// and leaves it uncommitted for an uncoded one (redelivery).
func TestResolvePostgresFaultSurfacesUncoded(t *testing.T) {
	msisdn := "2250700000001"
	cache := &fakeRedis{vals: map[string]string{}}
	// The fault must be what the repository ACTUALLY returns: postgres.translate wraps every
	// infrastructure failure in errs.ErrInternal, so the error reaching the resolver is CODED. A bare
	// errors.New here has the shape of an outage and none of its contract — and passes against a
	// resolver that propagates the code, which is the defect the Postgres chaos test caught.
	store := &fakeStore{err: fmt.Errorf("get exact route: %w",
		errors.Join(errors.New("unexpected EOF"), errs.ErrInternal))}
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour)

	_, ok, err := r.Resolve(context.Background(), msisdn)
	if err == nil || ok {
		t.Fatalf("Resolve(postgres fault) = (ok=%v, err=%v), want (false, non-nil)", ok, err)
	}
	if _, coded := errs.CodeOf(err); coded {
		t.Errorf("Resolve error is coded (%v); a coded error commits the offset and buries the message, "+
			"but an unreachable exact route is transient and must be redelivered", err)
	}
}

// TestResolveRedisFaultDoesNotFallThroughToPostgres pins the decision that keeps the step-250d chaos
// test meaningful: only a MISSING key (goredis.Nil) reaches the durable store. A Redis *fault* is
// returned as-is.
//
// Falling through on a fault looks like resilience and is a trap: it would silently move the hot path
// onto the control-plane database during a Redis outage, at the full message rate, and it would turn
// the fail-closed policy §16 proves into a policy nothing exercises. Whether to degrade that way is an
// open question recorded in the step; it is not this PR's to answer silently.
func TestResolveRedisFaultDoesNotFallThroughToPostgres(t *testing.T) {
	msisdn := "2250700000001"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	cache := &fakeRedis{err: context.DeadlineExceeded}
	// The store COULD answer — that is what makes this test non-hollow. With an empty store the
	// resolver would fail for want of a row rather than for want of a policy.
	store := &fakeStore{rows: map[string]Route{msisdn: {MSISDN: msisdn, Target: want}}}
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour)

	if _, ok, err := r.Resolve(context.Background(), msisdn); err == nil || ok {
		t.Fatalf("Resolve(redis fault) = (ok=%v, err=%v), want (false, non-nil)", ok, err)
	}
	if store.gets != 0 {
		t.Errorf("a Redis fault fell through to Postgres (%d lookup(s)); only a missing key may", store.gets)
	}
}

// TestCacheTTLJitterStaysInBand: the populated key must not expire in lockstep across a burst warmed
// at the same instant, so the TTL carries ±10% jitter. The band is asserted over many draws, and that
// they are not all identical — a constant would satisfy a band check alone.
func TestCacheTTLJitterStaysInBand(t *testing.T) {
	const base = time.Hour
	low, high := base-base/10, base+base/10

	seen := make(map[time.Duration]struct{})
	for range 1000 {
		got := jitterTTL(base)
		if got < low || got > high {
			t.Fatalf("jitterTTL(%v) = %v, want within [%v, %v]", base, got, low, high)
		}
		seen[got] = struct{}{}
	}
	if len(seen) < 2 {
		t.Errorf("jitterTTL produced %d distinct value(s) over 1000 draws; a constant TTL expires a "+
			"cold-warmed burst in lockstep, which is what the jitter exists to prevent", len(seen))
	}
}

// fakeMeter records the outcome of each lookup.
type fakeMeter struct{ seen []string }

func (m *fakeMeter) Observe(outcome string) { m.seen = append(m.seen, outcome) }

// TestResolveCountsEveryLookupOutcome: the four paths through Resolve must be distinguishable in
// metrics, because the follow-ups this step defers are not decidable without them — whether a negative
// cache is worth adding depends on the pg_miss rate, and the TTL on the pg_hit rate. Shipping the
// decision "measure first" without the measurement would leave both open forever.
func TestResolveCountsEveryLookupOutcome(t *testing.T) {
	cached, cold, absent := "2250700000001", "2250700000002", "2250700000003"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	meter := &fakeMeter{}

	r := NewResolver(
		newBloom([]string{cached, cold, absent}),
		&fakeRedis{vals: map[string]string{redisKey(cached): EncodeTarget(want)}},
		&fakeStore{rows: map[string]Route{cold: {MSISDN: cold, Target: want}}},
		time.Hour,
		WithLookupMeter(meter),
	)

	for _, msisdn := range []string{"2250799999999", cached, cold, absent} {
		if _, _, err := r.Resolve(context.Background(), msisdn); err != nil {
			t.Fatalf("Resolve(%s): %v", msisdn, err)
		}
	}

	want4 := []string{"bloom_miss", "redis_hit", "pg_hit", "pg_miss"}
	if len(meter.seen) != len(want4) {
		t.Fatalf("outcomes = %v, want %v", meter.seen, want4)
	}
	for i := range want4 {
		if meter.seen[i] != want4[i] {
			t.Errorf("outcome[%d] = %q, want %q", i, meter.seen[i], want4[i])
		}
	}
}

// TestResolveCorruptCacheValueHealsFromTheDurableTable: an unreadable cached value must not wedge the
// lane.
//
// Returning the parse error is not enough. The resolver refuses to descend to the store, so the
// redelivery it buys lands on the same key and fails again — and since the lane IS the Kafka partition,
// that is the whole partition stalled until the TTL expires, up to six hours, for one bad key. Yet the
// durable truth is right there: treat an illegible value as a miss, and the read-through rewrites a
// healthy one. The outcome is counted so a drift still shows up rather than healing invisibly.
func TestResolveCorruptCacheValueHealsFromTheDurableTable(t *testing.T) {
	msisdn := "2250700000001"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	cache := &fakeRedis{vals: map[string]string{redisKey(msisdn): "not-a-target"}}
	store := &fakeStore{rows: map[string]Route{msisdn: {MSISDN: msisdn, Target: want}}}
	meter := &fakeMeter{}
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour, WithLookupMeter(meter))

	got, ok, err := r.Resolve(context.Background(), msisdn)
	if err != nil || !ok || got != want {
		t.Fatalf("Resolve(corrupt) = (%+v, ok=%v, err=%v), want the durable target", got, ok, err)
	}
	if cached := cache.vals[redisKey(msisdn)]; cached != EncodeTarget(want) {
		t.Errorf("cache still holds %q, want it healed to %q", cached, EncodeTarget(want))
	}
	// Exactly one observation, like every other path: a healed value did perform a durable read, but
	// counting it as pg_hit as well would inflate the ratios ADR-0015 defers two decisions to, and would
	// stop sum(exact_route_lookups_total) from being a lookup count.
	if len(meter.seen) != 1 || meter.seen[0] != "cache_corrupt" {
		t.Errorf("outcomes = %v, want exactly [cache_corrupt] — one observation per resolution, and a "+
			"value that healed invisibly is a drift nobody can see", meter.seen)
	}
}

// TestResolveCachesWithTheConfiguredTTL: the TTL is the ONLY bound on a lost invalidation, and the
// fiche names "a key written without TTL" as the defect it refused to ship. In go-redis an expiration
// of 0 means NO expiry, so a resolver that dropped the TTL would produce exactly that — silently.
func TestResolveCachesWithTheConfiguredTTL(t *testing.T) {
	msisdn := "2250700000001"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	cache := &fakeRedis{vals: map[string]string{}}
	store := &fakeStore{rows: map[string]Route{msisdn: {MSISDN: msisdn, Target: want}}}
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour)

	if _, ok, err := r.Resolve(context.Background(), msisdn); err != nil || !ok {
		t.Fatalf("Resolve = (ok=%v, err=%v), want a hit", ok, err)
	}
	if cache.sets != 1 {
		t.Fatalf("cache Set called %d time(s), want 1", cache.sets)
	}
	low, high := time.Hour-time.Hour/10, time.Hour+time.Hour/10
	if cache.lastTTL < low || cache.lastTTL > high {
		t.Errorf("cache TTL = %v, want within [%v, %v]; 0 means NO EXPIRY in go-redis, which is the "+
			"unbounded-staleness state this step exists to avoid", cache.lastTTL, low, high)
	}
}

// TestResolveSurvivesACacheWriteFailure: the target is already known and correct, so a Redis write blip
// must not fail the message — the next one simply pays another lookup. Only the READ legs are
// fail-closed.
func TestResolveSurvivesACacheWriteFailure(t *testing.T) {
	msisdn := "2250700000001"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	cache := &fakeRedis{vals: map[string]string{}, setErr: errors.New("READONLY replica")}
	store := &fakeStore{rows: map[string]Route{msisdn: {MSISDN: msisdn, Target: want}}}
	r := NewResolver(newBloom([]string{msisdn}), cache, store, time.Hour)

	got, ok, err := r.Resolve(context.Background(), msisdn)
	if err != nil || !ok || got != want {
		t.Fatalf("Resolve(cache write fails) = (%+v, ok=%v, err=%v), want the durable target", got, ok, err)
	}
}

// TestResolveBoundsTheDurableLookup: a Postgres that ABSORBS without answering — packet loss, a
// failover, a locked table — must still end in an error, because "fail-closed en rejeu" (§16) buys
// nothing if the call never returns.
//
// The Redis leg already has this bound (redis client ReadTimeout); the durable leg had none: the pool
// sets only a connect timeout, the pipeline stage adds no deadline, and router.handle's context is the
// consumer's long-lived one. The lane — which IS the partition — would hang silently, with no error, no
// redelivery and no metric. The chaos test cannot catch it: a cut proxy resets the connection, which is
// the "loud" outage, not this one.
func TestResolveBoundsTheDurableLookup(t *testing.T) {
	msisdn := "2250700000001"
	r := NewResolver(
		newBloom([]string{msisdn}),
		&fakeRedis{vals: map[string]string{}},
		blackHoleStore{},
		time.Hour,
		WithLookupTimeout(150*time.Millisecond),
	)

	done := make(chan error, 1)
	go func() {
		_, _, err := r.Resolve(context.Background(), msisdn)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Resolve(black-hole postgres) = nil error, want a transient fault")
		}
		if _, coded := errs.CodeOf(err); coded {
			t.Errorf("timeout error is coded (%v); it must be transient so the offset is not committed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Resolve never returned against a Postgres that absorbs without answering; the lane " +
			"(and so the whole partition) would hang with no error, no redelivery and no metric")
	}
}

// blackHoleStore never answers on its own — it returns only when the caller's context gives up. That is
// the failure mode a cut connection does NOT reproduce.
type blackHoleStore struct{}

func (blackHoleStore) Get(ctx context.Context, _ string) (Route, bool, error) {
	<-ctx.Done()
	return Route{}, false, ctx.Err()
}

// slowStore answers only after burning most of the lookup budget, the way a loaded Postgres does.
type slowStore struct {
	delay time.Duration
	route Route
}

func (s slowStore) Get(ctx context.Context, _ string) (Route, bool, error) {
	select {
	case <-time.After(s.delay):
		return s.route, true, nil
	case <-ctx.Done():
		return Route{}, false, ctx.Err()
	}
}

// TestCachePopulateDoesNotInheritTheLookupBudget: the lookup deadline bounds the DURABLE READ, and
// nothing else.
//
// Sharing one deadline with the write that follows is not a crash, which is what makes it easy to ship:
// if store.Get returned at all, the context had not expired. But what is left of the budget can be a
// sliver — a Postgres that burned 1.9s of 2s leaves the populate 100ms — so the cache write starts
// failing exactly while the system is slow, and every following message pays another slow lookup. The
// cache would stop working precisely when it is most needed.
//
// The assertion is structural rather than timing-based, because the symptom is a degradation and not a
// deterministic failure: the write must simply not carry the lookup's deadline.
func TestCachePopulateDoesNotInheritTheLookupBudget(t *testing.T) {
	msisdn := "2250700000001"
	want := Target{Type: TargetConnector, ID: uuid.New()}
	cache := &fakeRedis{vals: map[string]string{}}
	r := NewResolver(
		newBloom([]string{msisdn}), cache,
		slowStore{delay: 20 * time.Millisecond, route: Route{MSISDN: msisdn, Target: want}},
		time.Hour,
		WithLookupTimeout(150*time.Millisecond),
	)

	if _, ok, err := r.Resolve(context.Background(), msisdn); err != nil || !ok {
		t.Fatalf("Resolve = (ok=%v, err=%v), want a hit", ok, err)
	}
	if cache.sets != 1 {
		t.Fatalf("cache Set called %d time(s), want 1", cache.sets)
	}
	if cache.setHadDL && time.Until(cache.setDeadline) < 130*time.Millisecond {
		t.Errorf("the cache write carried a deadline %v away — the durable lookup's leftover budget, "+
			"not its own. A slow lookup would then starve the populate",
			time.Until(cache.setDeadline).Round(time.Millisecond))
	}
}
