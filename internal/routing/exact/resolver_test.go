package exact

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// fakeRedis is an in-memory stand-in for the resolver's cache: a missing key returns goredis.Nil,
// matching the real client, so the cache-miss path is exercised without a live Redis. gets counts calls
// so a test can prove the definitive-miss short-cut never touches Redis; lastTTL records what a
// populating Set asked for.
type fakeRedis struct {
	vals    map[string]string
	err     error // when set, every command returns this transient fault
	gets    int   // number of Get calls
	sets    int   // number of Set calls
	lastTTL time.Duration
}

func (f *fakeRedis) Get(_ context.Context, key string) *goredis.StringCmd {
	f.gets++
	if f.err != nil {
		return goredis.NewStringResult("", f.err)
	}
	v, ok := f.vals[key]
	if !ok {
		return goredis.NewStringResult("", goredis.Nil)
	}
	return goredis.NewStringResult(v, nil)
}

func (f *fakeRedis) Set(_ context.Context, key string, value any, ttl time.Duration) *goredis.StatusCmd {
	if f.err != nil {
		return goredis.NewStatusResult("", f.err)
	}
	if f.vals == nil {
		f.vals = map[string]string{}
	}
	f.sets++
	f.lastTTL = ttl
	f.vals[key] = fmt.Sprint(value)
	return goredis.NewStatusResult("OK", nil)
}

// TestResolveMissSkipsRedis: a number the Bloom does not know is a definitive miss — ok=false, and the
// resolver must not read Redis at all (the counter proves the short-cut, so a regression that drops it
// fails loudly).
func TestResolveMissSkipsRedis(t *testing.T) {
	bloom := newBloom([]string{"2250700000001"})
	redis := &fakeRedis{}
	r := NewResolver(bloom, redis, &fakeStore{}, time.Hour)

	target, ok, err := r.Resolve(context.Background(), "2250799999999")
	if err != nil || ok {
		t.Fatalf("Resolve(absent) = (%+v, ok=%v, err=%v), want (zero, false, nil)", target, ok, err)
	}
	if redis.gets != 0 {
		t.Errorf("Bloom miss did %d Redis Get(s), want 0 (definitive miss must not touch Redis)", redis.gets)
	}
}

// TestResolveHitReturnsTarget: a configured number present in both the Bloom and Redis resolves to its
// decoded target.
func TestResolveHitReturnsTarget(t *testing.T) {
	msisdn := "2250700000001"
	connID := uuid.New()
	bloom := newBloom([]string{msisdn})
	r := NewResolver(bloom, &fakeRedis{vals: map[string]string{
		redisKey(msisdn): EncodeTarget(Target{Type: TargetConnector, ID: connID}),
	}}, &fakeStore{}, time.Hour)

	target, ok, err := r.Resolve(context.Background(), msisdn)
	if err != nil || !ok {
		t.Fatalf("Resolve(hit) = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	if target.Type != TargetConnector || target.ID != connID {
		t.Errorf("target = %+v, want {connector %s}", target, connID)
	}
}

// TestResolveBloomHitButRedisAbsent: a Bloom possible-hit that neither the cache nor the durable table
// knows is a false positive. It falls back — ok=false, no error — so a false positive never mis-routes.
func TestResolveBloomHitButRedisAbsent(t *testing.T) {
	msisdn := "2250700000001"
	bloom := newBloom([]string{msisdn})                                                     // msisdn IS in the Bloom...
	r := NewResolver(bloom, &fakeRedis{vals: map[string]string{}}, &fakeStore{}, time.Hour) // ...and absent from both.

	target, ok, err := r.Resolve(context.Background(), msisdn)
	if err != nil || ok {
		t.Fatalf("Resolve(bloom hit, redis miss) = (%+v, ok=%v, err=%v), want (zero, false, nil)", target, ok, err)
	}
}

// TestResolveRedisFaultSurfaces: a transient Redis error is returned, not swallowed as a miss, so the
// caller retries rather than silently skipping a real override.
func TestResolveRedisFaultSurfaces(t *testing.T) {
	msisdn := "2250700000001"
	bloom := newBloom([]string{msisdn})
	r := NewResolver(bloom, &fakeRedis{err: context.DeadlineExceeded}, &fakeStore{}, time.Hour)

	if _, ok, err := r.Resolve(context.Background(), msisdn); err == nil || ok {
		t.Fatalf("Resolve(redis fault) = (ok=%v, err=%v), want (false, non-nil)", ok, err)
	}
}

// TestParseTargetRoundTrip: EncodeTarget/parseTarget are inverses for both kinds, and malformed values
// are rejected rather than mis-decoded.
func TestParseTargetRoundTrip(t *testing.T) {
	for _, tt := range []TargetType{TargetConnector, TargetRoute} {
		want := Target{Type: tt, ID: uuid.New()}
		got, err := parseTarget(EncodeTarget(want))
		if err != nil || got != want {
			t.Errorf("round-trip %s = (%+v, %v), want (%+v, nil)", tt, got, err, want)
		}
	}
	for _, bad := range []string{"", "connector", "bogus:" + uuid.NewString(), "connector:not-a-uuid"} {
		if _, err := parseTarget(bad); err == nil {
			t.Errorf("parseTarget(%q) = nil error, want rejection", bad)
		}
	}
}

// TestMightContainNoFalseNegative: every configured MSISDN is reported as a possible member — the
// Bloom invariant the L0 short-cut relies on (absent ⇒ certainly no override).
func TestMightContainNoFalseNegative(t *testing.T) {
	msisdns := make([]string, 500)
	for i := range msisdns {
		msisdns[i] = "22507" + uuid.NewString()[:8]
	}
	b := newBloom(msisdns)
	for _, m := range msisdns {
		if !b.MightContain(m) {
			t.Fatalf("MightContain(%q) = false, want true (Bloom must never yield a false negative)", m)
		}
	}
}

// TestExactRouteRedisEncodingIsPinned pins the wire form of an exact route — key AND value — against
// literals, rather than against the functions that produce them.
//
// Everything else in this package, the chaos test included, seeds through redisKey/EncodeTarget and
// reads back through the resolver, so the encoding is only ever checked against itself: drop the {}
// hash tag and the entire suite stays green (verified by mutation). The tag is not cosmetic — it is what
// pins a number's key to one cluster slot — and step-250e is about to add the writer this key has never
// had. Two components agreeing on a format that nothing anchors is how they drift apart in silence.
func TestExactRouteRedisEncodingIsPinned(t *testing.T) {
	if got := redisKey("2250700000001"); got != "exactroute:{2250700000001}" {
		t.Errorf("redisKey = %q, want exactroute:{2250700000001}: without the hash tag a number's key is "+
			"no longer pinned to a single cluster slot", got)
	}

	id := uuid.MustParse("3f2504e0-4f89-11d3-9a0c-0305e82c3301")
	for _, tc := range []struct {
		target Target
		want   string
	}{
		{Target{Type: TargetConnector, ID: id}, "connector:3f2504e0-4f89-11d3-9a0c-0305e82c3301"},
		{Target{Type: TargetRoute, ID: id}, "route:3f2504e0-4f89-11d3-9a0c-0305e82c3301"},
	} {
		if got := EncodeTarget(tc.target); got != tc.want {
			t.Errorf("EncodeTarget(%+v) = %q, want %q", tc.target, got, tc.want)
		}
	}
}
