package breaker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
)

// clock is a deterministic, concurrency-safe injectable clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }
func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func testConfig() breaker.Config {
	return breaker.Config{MinRequests: 5, FailureRate: 0.5, Window: 10 * time.Second, Cooldown: 30 * time.Second, HalfOpenProbes: 2}
}

// TestBreakerLifecycle: a burst of failures opens it; after the cooldown it half-opens; enough probe
// successes close it.
func TestBreakerLifecycle(t *testing.T) {
	clk := newClock()
	b := breaker.New(testConfig(), clk.now)

	if b.State() != breaker.Closed {
		t.Fatalf("initial state = %s, want closed", b.State())
	}
	// A burst of 5 failures (min 5, rate 1.0 ≥ 0.5) trips it open.
	for i := 0; i < 5; i++ {
		b.RecordFailure()
	}
	if b.State() != breaker.Open {
		t.Fatalf("after burst = %s, want open", b.State())
	}
	if b.Allow() {
		t.Error("open breaker allowed a request")
	}

	// Before the cooldown: still open.
	clk.advance(29 * time.Second)
	if b.State() != breaker.Open {
		t.Errorf("before cooldown = %s, want open", b.State())
	}
	// After the cooldown: half-open, admitting probes.
	clk.advance(2 * time.Second)
	if b.State() != breaker.HalfOpen {
		t.Fatalf("after cooldown = %s, want half_open", b.State())
	}
	if !b.Allow() {
		t.Error("half-open breaker refused the first probe")
	}

	// Two probe successes (HalfOpenProbes=2) close it.
	b.RecordSuccess()
	b.RecordSuccess()
	if b.State() != breaker.Closed {
		t.Errorf("after successful probes = %s, want closed", b.State())
	}
}

// TestHalfOpenProbeFailureReopens: a single failed probe re-opens the breaker for another cooldown.
func TestHalfOpenProbeFailureReopens(t *testing.T) {
	clk := newClock()
	b := breaker.New(testConfig(), clk.now)
	for i := 0; i < 5; i++ {
		b.RecordFailure()
	}
	clk.advance(31 * time.Second)
	if b.State() != breaker.HalfOpen {
		t.Fatalf("state = %s, want half_open", b.State())
	}
	b.RecordFailure() // a probe fails
	if b.State() != breaker.Open {
		t.Errorf("after failed probe = %s, want open", b.State())
	}
}

// TestHalfOpenProbeQuota: half-open admits at most HalfOpenProbes concurrent probes.
func TestHalfOpenProbeQuota(t *testing.T) {
	clk := newClock()
	b := breaker.New(testConfig(), clk.now) // HalfOpenProbes = 2
	for i := 0; i < 5; i++ {
		b.RecordFailure()
	}
	clk.advance(31 * time.Second)
	_ = b.State() // advance to half_open

	probe1, probe2 := b.Allow(), b.Allow() // each Allow admits (and counts) one probe
	if !probe1 || !probe2 {
		t.Fatal("half-open refused a probe within quota")
	}
	if b.Allow() {
		t.Error("half-open admitted a third probe beyond the quota of 2")
	}
}

// TestFailureThresholds is table-driven over the min-requests and rate gates.
func TestFailureThresholds(t *testing.T) {
	cases := []struct {
		name             string
		successes, fails int
		wantOpen         bool
	}{
		{"below min requests stays closed", 0, 4, false},    // 4 < MinRequests(5)
		{"at min, 100% failure opens", 0, 5, true},          // 5 fails, rate 1.0
		{"exactly at the rate threshold opens", 4, 4, true}, // 8 total, 4 fails = 0.5 == threshold (≥ opens)
		{"mixed above rate opens", 2, 8, true},              // 10 total, 8 fails = 0.8 ≥ 0.5
		{"mixed below rate stays closed", 8, 2, false},      // 10 total, 2 fails = 0.2
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newClock()
			b := breaker.New(testConfig(), clk.now)
			for i := 0; i < tc.successes; i++ {
				b.RecordSuccess()
			}
			for i := 0; i < tc.fails; i++ {
				b.RecordFailure()
			}
			open := b.State() == breaker.Open
			if open != tc.wantOpen {
				t.Errorf("open = %v, want %v (%d ok / %d fail)", open, tc.wantOpen, tc.successes, tc.fails)
			}
		})
	}
}

// TestWindowRollsOverForgetsOldFailures: failures that age out of the window do not accumulate toward
// the threshold.
func TestWindowRollsOverForgetsOldFailures(t *testing.T) {
	clk := newClock()
	b := breaker.New(testConfig(), clk.now)

	// 3 failures, then let the window elapse, then 3 more: never 5 within one window → stays closed.
	for i := 0; i < 3; i++ {
		b.RecordFailure()
	}
	clk.advance(11 * time.Second) // past the 10s window
	for i := 0; i < 4; i++ {
		b.RecordFailure()
	}
	if b.State() != breaker.Closed {
		t.Errorf("state = %s, want closed (old failures aged out)", b.State())
	}
}

// TestConcurrentRecording: concurrent outcomes are race-free (run under -race).
func TestConcurrentRecording(t *testing.T) {
	b := breaker.New(testConfig(), nil)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				b.RecordSuccess()
			} else {
				b.RecordFailure()
			}
			_ = b.Allow()
			_ = b.State()
		}(i)
	}
	wg.Wait()
}

// TestClassifyAndRecord: ESME_ROK is a success; throttle/queue-full/message-level rejects are transient
// (ignored by the breaker); a system/bind failure is a connector-health failure that trips it.
func TestClassifyAndRecord(t *testing.T) {
	cases := []struct {
		status uint32
		want   breaker.Outcome
	}{
		{0x00000000, breaker.Success},   // ESME_ROK
		{0x00000058, breaker.Transient}, // ESME_RTHROTTLED
		{0x00000014, breaker.Transient}, // ESME_RMSGQFUL
		{0x0000000B, breaker.Transient}, // ESME_RINVDSTADR (message-level)
		{0x00000008, breaker.Failure},   // ESME_RSYSERR (connector health)
		{0x00000045, breaker.Failure},   // ESME_RSUBMITFAIL
	}
	for _, c := range cases {
		if got := breaker.Classify(c.status); got != c.want {
			t.Errorf("Classify(0x%02x) = %v, want %v", c.status, got, c.want)
		}
	}

	// Record feeds the breaker via Classify: throttle results never trip it; health failures do.
	clk := newClock()
	b := breaker.New(testConfig(), clk.now) // MinRequests 5, rate 0.5
	for i := 0; i < 100; i++ {
		b.Record(0x00000058) // all throttled → transient → ignored
	}
	if b.State() != breaker.Closed {
		t.Errorf("throttle results tripped the breaker (state=%s), want closed", b.State())
	}
	for i := 0; i < 5; i++ {
		b.Record(0x00000008) // system errors → failure
	}
	if b.State() != breaker.Open {
		t.Errorf("system errors did not trip the breaker (state=%s), want open", b.State())
	}
}

// TestHalfOpenTimeoutReopens: probes that never resolve (all transient/lost) must not fence the
// connector forever — after HalfOpenTimeout the breaker re-opens and retries after the next cooldown.
func TestHalfOpenTimeoutReopens(t *testing.T) {
	clk := newClock()
	cfg := testConfig()
	cfg.HalfOpenTimeout = 5 * time.Second
	b := breaker.New(cfg, clk.now)
	for i := 0; i < 5; i++ {
		b.RecordFailure()
	}
	clk.advance(31 * time.Second) // cooldown → half_open
	if b.State() != breaker.HalfOpen {
		t.Fatalf("state = %s, want half_open", b.State())
	}
	// All probes come back throttled (transient) — never conclusive.
	for i := 0; i < 5; i++ {
		b.Record(0x00000058)
	}
	clk.advance(6 * time.Second) // past HalfOpenTimeout
	if b.State() != breaker.Open {
		t.Errorf("half_open with only transient probes = %s after timeout, want open (liveness)", b.State())
	}
}

// TestTransientProbeFreesSlotAndRecovers: a transient probe frees its slot so a later successful probe
// can still close the breaker within the half-open window.
func TestTransientProbeFreesSlotAndRecovers(t *testing.T) {
	clk := newClock()
	cfg := testConfig()
	cfg.HalfOpenProbes = 1
	b := breaker.New(cfg, clk.now)
	for i := 0; i < 5; i++ {
		b.RecordFailure()
	}
	clk.advance(31 * time.Second)
	_ = b.State() // → half_open

	if !b.Allow() {
		t.Fatal("first probe refused")
	}
	b.Record(0x00000058) // transient → frees the slot
	if !b.Allow() {
		t.Fatal("second probe refused after a transient result freed the slot")
	}
	b.Record(0x00000000) // success → closes (HalfOpenProbes=1)
	if b.State() != breaker.Closed {
		t.Errorf("state = %s, want closed after a successful probe", b.State())
	}
}

// TestConfigDefaults: zero-value config fields fall back to working defaults (never a disabled breaker).
func TestConfigDefaults(t *testing.T) {
	b := breaker.New(breaker.Config{}, newClock().now) // all zero
	// Defaults: MinRequests 20, rate 0.5. 20 failures → open.
	for i := 0; i < 20; i++ {
		b.RecordFailure()
	}
	if b.State() != breaker.Open {
		t.Errorf("default-config breaker did not open after 20 failures (state=%s)", b.State())
	}
}
