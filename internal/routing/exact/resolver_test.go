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
	vals map[string]string
	err  error // when set, every Get returns this transient fault
	// setErr is separate from err on purpose: with one field, "the key is absent AND the populating
	// write fails" — the exact case the read-through has to survive — could not be expressed at all.
	setErr error
	// setDeadline records the deadline the populating write was handed, so a test can prove the write
	// does not inherit the durable lookup's budget.
	setDeadline time.Time
	setHadDL    bool
	gets        int // number of Get calls
	sets        int // number of Set calls
	lastTTL     time.Duration
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

func (f *fakeRedis) Set(ctx context.Context, key string, value any, ttl time.Duration) *goredis.StatusCmd {
	// The real client checks the context before issuing a command; a double that ignores it cannot show
	// a populate starved by a deadline meant for someone else.
	if err := ctx.Err(); err != nil {
		return goredis.NewStatusResult("", err)
	}
	f.setDeadline, f.setHadDL = ctx.Deadline()
	if f.setErr != nil {
		return goredis.NewStatusResult("", f.setErr)
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
		redisKey(msisdn): encodeTarget(Target{Type: TargetConnector, ID: connID}),
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

// TestParseTargetRoundTrip: encodeTarget/parseTarget are inverses for both kinds, and malformed values
// are rejected rather than mis-decoded.
func TestParseTargetRoundTrip(t *testing.T) {
	for _, tt := range []TargetType{TargetConnector, TargetRoute} {
		want := Target{Type: tt, ID: uuid.New()}
		got, err := parseTarget(encodeTarget(want))
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
// Everything else in this package, the chaos test included, seeds through redisKey/encodeTarget and
// reads back through the resolver, so the encoding is only ever checked against itself: drop the {}
// hash tag and the entire suite stays green (verified by mutation). The tag is not cosmetic — it is what
// pins a number's key to one cluster slot — and step-250e gave the key the writer it never had: the
// resolver populates it, the Admin API deletes it. Two components agreeing on a format that nothing
// anchors is how they drift apart in silence.
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
		if got := encodeTarget(tc.target); got != tc.want {
			t.Errorf("encodeTarget(%+v) = %q, want %q", tc.target, got, tc.want)
		}
	}
}

// respError is a server error as go-redis models one: the RedisError marker is what redis.HasErrorPrefix
// tests with errors.As, and without it the prefix is never even compared. Every existing fault test here
// uses a transport error (a deadline, an i/o timeout) — none of which carries that marker — so none of
// them can say anything about how a RESP error code is classified.
type respError string

func (e respError) Error() string { return string(e) }
func (respError) RedisError()     {}

// TestResolveClassifiesServerErrorsByTheirCode covers the fork the WRONGTYPE heal introduced. Only
// WRONGTYPE may fall through to the durable table; every other RESP error code must stay a fault.
//
// The second case is the load-bearing one. Widening the prefix — to "", to "W" — would send LOADING,
// READONLY, OOM, CLUSTERDOWN and MASTERDOWN down the healing path too, which is the exact fail-open the
// resolver's godoc forbids: the whole MT hot path onto the control-plane database, at full message rate,
// during a Redis outage.
func TestResolveClassifiesServerErrorsByTheirCode(t *testing.T) {
	msisdn := "2250700000042"
	want := Target{Type: TargetConnector, ID: uuid.New()}

	for _, tc := range []struct {
		name        string
		err         error
		wantOutcome string
		wantErr     bool
		wantCorrupt int
	}{
		{
			name:        "WRONGTYPE heals from the durable table",
			err:         respError("WRONGTYPE Operation against a key holding the wrong kind of value"),
			wantOutcome: outcomePgHit,
			wantCorrupt: 1,
		},
		{
			name:        "LOADING stays a fault",
			err:         respError("LOADING Redis is loading the dataset in memory"),
			wantOutcome: outcomeRedisError,
			wantErr:     true,
		},
		{
			name:        "READONLY stays a fault",
			err:         respError("READONLY You can't write against a read only replica"),
			wantOutcome: outcomeRedisError,
			wantErr:     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meter := &fakeMeter{}
			corrupt := &countingCorruption{}
			store := &fakeStore{rows: map[string]Route{msisdn: {MSISDN: msisdn, Target: want}}}
			r := NewResolver(newBloom([]string{msisdn}), &fakeRedis{err: tc.err}, store, time.Hour,
				WithLookupMeter(meter), WithCorruptionMeter(corrupt))

			got, ok, err := r.Resolve(context.Background(), msisdn)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Resolve() = %+v, %v, nil; want the fault returned, not a fall-through to Postgres",
						got, ok)
				}
				if store.gets != 0 {
					t.Errorf("durable lookups = %d, want 0: a Redis outage must not move the hot path "+
						"onto the control-plane database", store.gets)
				}
			} else if err != nil || !ok || got != want {
				t.Fatalf("Resolve() = %+v, %v, %v; want %+v, true, nil", got, ok, err, want)
			}
			if corrupt.n != tc.wantCorrupt {
				t.Errorf("corruption counter = %d, want %d", corrupt.n, tc.wantCorrupt)
			}
			if len(meter.seen) != 1 || meter.seen[0] != tc.wantOutcome {
				t.Errorf("outcomes = %v, want exactly [%s]", meter.seen, tc.wantOutcome)
			}
		})
	}
}

// countingCorruption counts the cache values the resolver could not use.
type countingCorruption struct{ n int }

func (c *countingCorruption) Inc() { c.n++ }
