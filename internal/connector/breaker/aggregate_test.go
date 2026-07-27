package breaker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// capturePub records every publish so a test can assert the invalidation fan-out.
type capturePub struct {
	mu       sync.Mutex
	channels []string
	payloads []string
}

func (p *capturePub) Publish(_ context.Context, channel string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.channels = append(p.channels, channel)
	p.payloads = append(p.payloads, string(payload))
	return nil
}

func (p *capturePub) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.payloads)
}

// mustReport reports a state and fails the test on error (setup steps whose result is not asserted).
func mustReport(t *testing.T, a *breaker.Aggregator, connectorID string, s breaker.State) {
	t.Helper()
	if _, err := a.Report(context.Background(), connectorID, 0, s); err != nil {
		t.Fatalf("report: %v", err)
	}
}

// TestParseState round-trips the schema tokens; an unknown token is not ok.
func TestParseState(t *testing.T) {
	for _, s := range []breaker.State{breaker.Closed, breaker.Open, breaker.HalfOpen} {
		got, ok := breaker.ParseState(s.String())
		if !ok || got != s {
			t.Errorf("ParseState(%q) = (%v, %v), want (%v, true)", s.String(), got, ok, s)
		}
	}
	if _, ok := breaker.ParseState("bogus"); ok {
		t.Error("ParseState(bogus) ok = true, want false")
	}
}

// TestAggregateMajorityOpen: 3 live sub-binds, 2 open → the connector aggregate is open.
func TestAggregateMajorityOpen(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	pub := &capturePub{}

	report := func(pod string, s breaker.State) breaker.State {
		agg, err := breaker.NewAggregator(rdb, pub, pod).Report(ctx, cid, 0, s)
		if err != nil {
			t.Fatalf("report %s: %v", pod, err)
		}
		return agg
	}
	report("pod-a", breaker.Open)
	report("pod-b", breaker.Open)
	if got := report("pod-c", breaker.Closed); got != breaker.Open {
		t.Errorf("aggregate = %s, want open (2 of 3 open)", got)
	}
	if got := rdb.Get(ctx, "breaker:state:{"+cid+"}").Val(); got != "open" {
		t.Errorf("breaker:state = %q, want open", got)
	}
}

// TestAggregateMinorityStaysClosed: only 1 of 3 sub-binds open → aggregate stays closed (a single
// isolated pod must not fence the whole connector).
func TestAggregateMinorityStaysClosed(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	pub := &capturePub{}

	mustReport(t, breaker.NewAggregator(rdb, pub, "pod-a"), cid, breaker.Open)
	mustReport(t, breaker.NewAggregator(rdb, pub, "pod-b"), cid, breaker.Closed)
	got, err := breaker.NewAggregator(rdb, pub, "pod-c").Report(ctx, cid, 0, breaker.Closed)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if got != breaker.Closed {
		t.Errorf("aggregate = %s, want closed (1 of 3 open)", got)
	}
}

// TestExpiredBindExcludedFromQuorum: sub-binds whose heartbeat has aged past the TTL are swept and do
// not count toward the majority — a dead pod cannot freeze the aggregate at open.
func TestExpiredBindExcludedFromQuorum(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	clk := newClock()
	pub := &capturePub{}
	agg := func(pod string) *breaker.Aggregator {
		return breaker.NewAggregator(rdb, pub, pod, breaker.WithTTL(10*time.Second), breaker.WithClock(clk.now))
	}

	mustReport(t, agg("pod-a"), cid, breaker.Open)
	mustReport(t, agg("pod-b"), cid, breaker.Open) // 2 open → aggregate open
	if v := rdb.Get(ctx, "breaker:state:{"+cid+"}").Val(); v != "open" {
		t.Fatalf("precondition breaker:state = %q, want open", v)
	}

	clk.advance(11 * time.Second) // both opens now stale (> 10s TTL)
	got, err := agg("pod-c").Report(ctx, cid, 0, breaker.Closed)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if got != breaker.Closed {
		t.Errorf("aggregate = %s, want closed (stale opens swept, only a fresh closed remains)", got)
	}
}

// TestPublishesOnlyOnChange: an invalidation is published on breaker:events exactly when the aggregate
// transitions, not on every report (the router rebuild is expensive — no spurious fan-out).
func TestPublishesOnlyOnChange(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	pub := &capturePub{}
	report := func(pod string, s breaker.State) {
		if _, err := breaker.NewAggregator(rdb, pub, pod).Report(ctx, cid, 0, s); err != nil {
			t.Fatalf("report %s: %v", pod, err)
		}
	}

	report("pod-a", breaker.Closed) // closed == default → no change, no publish
	report("pod-b", breaker.Closed)
	if pub.count() != 0 {
		t.Errorf("published %d times on all-closed, want 0", pub.count())
	}

	report("pod-a", breaker.Open)
	report("pod-b", breaker.Open) // 2/2 open → closed→open transition → one publish
	if pub.count() != 1 {
		t.Fatalf("published %d times on transition, want exactly 1", pub.count())
	}
	if pub.channels[0] != config.ChannelSnapshotInvalidation || pub.payloads[0] != cid {
		t.Errorf("published (%q,%q), want (%q,%q)", pub.channels[0], pub.payloads[0], config.ChannelSnapshotInvalidation, cid)
	}

	report("pod-a", breaker.Open) // still open → no further publish
	if pub.count() != 1 {
		t.Errorf("published %d times after a no-op report, want still 1", pub.count())
	}
}

// TestAggregateMajorityHalfOpen: a strict majority of live sub-binds probing → aggregate half_open.
func TestAggregateMajorityHalfOpen(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	pub := &capturePub{}

	mustReport(t, breaker.NewAggregator(rdb, pub, "pod-a"), cid, breaker.HalfOpen)
	mustReport(t, breaker.NewAggregator(rdb, pub, "pod-b"), cid, breaker.HalfOpen)
	got, err := breaker.NewAggregator(rdb, pub, "pod-c").Report(ctx, cid, 0, breaker.Closed)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if got != breaker.HalfOpen {
		t.Errorf("aggregate = %s, want half_open (2 of 3 half_open)", got)
	}
}

// TestMultipleBindIndexPerPod: one pod's several sub-binds are distinct quorum members, not overwrites.
func TestMultipleBindIndexPerPod(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	a := breaker.NewAggregator(rdb, &capturePub{}, "pod-a")

	if _, err := a.Report(ctx, cid, 0, breaker.Open); err != nil {
		t.Fatalf("report bind 0: %v", err)
	}
	if _, err := a.Report(ctx, cid, 1, breaker.Closed); err != nil {
		t.Fatalf("report bind 1: %v", err)
	}
	// Two distinct sub-binds (0 open, 1 closed): no strict majority → closed. If bind_index were ignored,
	// bind 1 would have overwritten bind 0 and the single member (closed) would still be closed — so make
	// it decisive by adding a third open member: 2 open of 3 → open only if all three are counted.
	got, err := a.Report(ctx, cid, 2, breaker.Open)
	if err != nil {
		t.Fatalf("report bind 2: %v", err)
	}
	if got != breaker.Open {
		t.Errorf("aggregate = %s, want open (2 of this pod's 3 sub-binds open — each bind_index is its own member)", got)
	}
	if n := rdb.HLen(ctx, "breaker:binds:{"+cid+"}").Val(); n != 3 {
		t.Errorf("hash has %d fields, want 3 (one per bind_index)", n)
	}
}

// TestKeysExpire: the aggregation keys carry a TTL so a fully dead connector does not leak state forever.
func TestKeysExpire(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	a := breaker.NewAggregator(rdb, &capturePub{}, "pod-a", breaker.WithKeyTTL(30*time.Second))

	if _, err := a.Report(ctx, cid, 0, breaker.Open); err != nil {
		t.Fatalf("report: %v", err)
	}
	for _, k := range []string{"breaker:binds:{" + cid + "}", "breaker:state:{" + cid + "}"} {
		if ttl := rdb.PTTL(ctx, k).Val(); ttl <= 0 || ttl > 30*time.Second {
			t.Errorf("%s TTL = %v, want a positive value ≤ 30s", k, ttl)
		}
	}
}

// failPub fails the first n publishes, then succeeds — to prove at-least-once republication.
type failPub struct {
	mu        sync.Mutex
	failsLeft int
	ok        int
}

func (p *failPub) Publish(_ context.Context, _ string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failsLeft > 0 {
		p.failsLeft--
		return errPublish
	}
	p.ok++
	return nil
}

var errPublish = &pubErr{}

type pubErr struct{}

func (*pubErr) Error() string { return "publish failed" }

// TestFailedPublishRepublishes: when Publish fails, the aggregate is still committed but not acked, so a
// later report re-attempts the publish rather than dropping the invalidation for good (MAJEUR fix).
func TestFailedPublishRepublishes(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	cid := "c:" + t.Name()
	pub := &failPub{failsLeft: 1}
	a := breaker.NewAggregator(rdb, pub, "pod-a")

	// First transition to open: publish fails.
	if _, err := a.Report(ctx, cid, 0, breaker.Open); err == nil {
		t.Fatal("first report: want a publish error, got nil")
	}
	if v := rdb.Get(ctx, "breaker:state:{"+cid+"}").Val(); v != "open" { // committed despite the failed publish
		t.Fatalf("breaker:state = %q, want open (committed)", v)
	}

	// Same state reported again: not acked yet, so it must retry the publish (and succeed this time).
	if _, err := a.Report(ctx, cid, 0, breaker.Open); err != nil {
		t.Fatalf("second report: %v", err)
	}
	if pub.ok != 1 {
		t.Errorf("successful publishes = %d, want 1 (republished after the failure)", pub.ok)
	}

	// Now acked: a further identical report must NOT publish again.
	if _, err := a.Report(ctx, cid, 0, breaker.Open); err != nil {
		t.Fatalf("third report: %v", err)
	}
	if pub.ok != 1 {
		t.Errorf("successful publishes = %d after ack, want still 1", pub.ok)
	}
}
