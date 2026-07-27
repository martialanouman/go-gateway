package exact

import (
	"context"
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// fakeRedis is a Get-only stand-in for the resolver: a missing key returns goredis.Nil, matching the
// real client, so the Bloom-hit-but-absent path is exercised without a live Redis. gets counts calls so
// a test can prove the definitive-miss short-cut never touches Redis.
type fakeRedis struct {
	vals map[string]string
	err  error // when set, every Get returns this transient fault
	gets int   // number of Get calls
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

// TestResolveMissSkipsRedis: a number the Bloom does not know is a definitive miss — ok=false, and the
// resolver must not read Redis at all (the counter proves the short-cut, so a regression that drops it
// fails loudly).
func TestResolveMissSkipsRedis(t *testing.T) {
	bloom := newBloom([]string{"2250700000001"})
	redis := &fakeRedis{}
	r := NewResolver(bloom, redis)

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
	}})

	target, ok, err := r.Resolve(context.Background(), msisdn)
	if err != nil || !ok {
		t.Fatalf("Resolve(hit) = (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	if target.Type != TargetConnector || target.ID != connID {
		t.Errorf("target = %+v, want {connector %s}", target, connID)
	}
}

// TestResolveBloomHitButRedisAbsent: a Bloom possible-hit with no Redis entry (a false positive, or a
// number not yet synced) falls back — ok=false, no error — so a false positive never mis-routes.
func TestResolveBloomHitButRedisAbsent(t *testing.T) {
	msisdn := "2250700000001"
	bloom := newBloom([]string{msisdn})                            // msisdn IS in the Bloom...
	r := NewResolver(bloom, &fakeRedis{vals: map[string]string{}}) // ...but absent from Redis.

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
	r := NewResolver(bloom, &fakeRedis{err: context.DeadlineExceeded})

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
