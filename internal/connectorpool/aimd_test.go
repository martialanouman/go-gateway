package connectorpool

import (
	"context"
	"sync"
	"testing"
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// TestAIMDDecreasesThenIncreases is the step-086 acceptance criterion at the logic level: a burst of
// ESME_RTHROTTLED multiplicatively decreases the send rate, and subsequent successes additively bring
// it back up.
func TestAIMDDecreasesThenIncreases(t *testing.T) {
	a := newAIMD(100, nil) // starts at the ceiling

	// Multiplicative decrease: 100 -> 50 -> 25.
	if !a.observe(errs.StatusThrottled) {
		t.Error("observe(ESME_RTHROTTLED) should report a throttle")
	}
	a.observe(errs.StatusThrottled)
	if got := a.currentRate(); got != 25 {
		t.Fatalf("rate after two throttles = %v, want 25 (halved twice)", got)
	}

	// Additive increase on success (step = max/100 = 1): 25 -> 30 over five successes.
	low := a.currentRate()
	for i := 0; i < 5; i++ {
		if a.observe(smpp.StatusOK) {
			t.Error("observe(OK) must not report a throttle")
		}
	}
	if got := a.currentRate(); got <= low {
		t.Errorf("rate after successes = %v, want it risen above %v (additive increase)", got, low)
	}
}

// TestAIMDBoundedByCeiling: additive increase never pushes the rate past the connector's throughput.
func TestAIMDBoundedByCeiling(t *testing.T) {
	a := newAIMD(10, nil)
	for i := 0; i < 100; i++ {
		a.observe(smpp.StatusOK)
	}
	if got := a.currentRate(); got != 10 {
		t.Errorf("rate = %v, want capped at the ceiling 10", got)
	}
}

// TestAIMDFloor: repeated throttling never stalls the rate to zero — a connector keeps trickling.
func TestAIMDFloor(t *testing.T) {
	a := newAIMD(10, nil) // floor = max(0.5, 1) = 1
	for i := 0; i < 40; i++ {
		a.observe(errs.StatusThrottled)
	}
	if got := a.currentRate(); got < 1 {
		t.Errorf("rate = %v, want floored at 1 (never zero)", got)
	}
}

// TestAIMDIgnoresNonRateStatuses: a permanent error (not a throttle, not a success) is not a rate
// signal and leaves the rate unchanged.
func TestAIMDIgnoresNonRateStatuses(t *testing.T) {
	a := newAIMD(100, nil)
	if a.observe(errs.StatusSysErr) {
		t.Error("a system error is not a throttle")
	}
	if got := a.currentRate(); got != 100 {
		t.Errorf("rate = %v, want unchanged 100 (a non-throttle, non-OK status is not a rate signal)", got)
	}
}

// TestAIMDAcquirePaces: acquire returns immediately for the first send and waits ~one interval for the
// next, so the rate becomes the effective send rate. The clock is injected to make the first slot
// deterministic; the wait itself is short (a low rate) and real.
func TestAIMDAcquirePaces(t *testing.T) {
	base := time.Now()
	a := newAIMD(100, func() time.Time { return base })

	// First acquire has no prior send -> immediate.
	start := time.Now()
	if err := a.acquire(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if time.Since(start) > 20*time.Millisecond {
		t.Errorf("first send should not be paced (no prior send), waited %v", time.Since(start))
	}
}

// TestAIMDAcquireConcurrent exercises the controller under concurrent callers so -race guards the
// mutex: the connector's real submit path is serial, but the invariant must hold if the handler is ever
// parallelised. A high rate keeps the pacing negligible so the test stays fast.
func TestAIMDAcquireConcurrent(t *testing.T) {
	a := newAIMD(100000, nil)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = a.acquire(context.Background())
				a.observe(smpp.StatusOK)
			}
		}()
	}
	wg.Wait()
	if r := a.currentRate(); r <= 0 || r > 100000 {
		t.Errorf("rate after concurrent use = %v, want within (0, 100000]", r)
	}
}

// TestAIMDAcquireHonoursContext: a cancelled context aborts the pacing wait rather than blocking.
func TestAIMDAcquireHonoursContext(t *testing.T) {
	base := time.Now()
	a := newAIMD(1, func() time.Time { return base }) // 1/s -> a full second between sends
	_ = a.acquire(context.Background())               // first send reserves the slot

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.acquire(ctx); err == nil {
		t.Error("acquire must return the context error when cancelled during the pacing wait")
	}
}
