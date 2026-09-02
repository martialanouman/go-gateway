package exact

import (
	"context"
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
	store := &fakeStore{err: context.DeadlineExceeded}
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
