package bindthrottle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// testConfig is the throttle used by most integration tests: threshold 3, a long window (so a test
// controls the count, not the clock), base 1s, cap 30s.
func testConfig() bindthrottle.Config {
	return bindthrottle.Config{
		MaxFailures: 3,
		Window:      15 * time.Minute,
		BackoffBase: time.Second,
		BackoffMax:  30 * time.Second,
	}
}

func TestCheckBlocksAtThreshold(t *testing.T) {
	rdb := redistest.Client(t)
	th := bindthrottle.New(rdb, testConfig())
	ctx := context.Background()
	sid, ip := uuid.NewString(), uuid.NewString()

	// Below the threshold: the bind is allowed.
	for i := 0; i < 2; i++ {
		if err := th.RecordFailure(ctx, sid, ip); err != nil {
			t.Fatalf("record failure %d: %v", i, err)
		}
	}
	dec, err := th.Check(ctx, sid, ip)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if dec.Blocked {
		t.Fatalf("blocked at 2 failures, want allowed")
	}

	// The third failure reaches the threshold: the next bind is throttled with the base backoff.
	if err := th.RecordFailure(ctx, sid, ip); err != nil {
		t.Fatalf("record third failure: %v", err)
	}
	dec, err = th.Check(ctx, sid, ip)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !dec.Blocked {
		t.Fatal("not blocked at 3 failures, want blocked")
	}
	if dec.Failures != 3 {
		t.Fatalf("failures = %d, want 3", dec.Failures)
	}
	if dec.RetryAfter != time.Second {
		t.Fatalf("retry after = %v, want 1s (base backoff at threshold)", dec.RetryAfter)
	}
	// system_id and IP counters climbed together; a tie attributes the block to system_id.
	if dec.Subject != bindthrottle.SubjectSystemID {
		t.Fatalf("subject = %q, want %q", dec.Subject, bindthrottle.SubjectSystemID)
	}
}

func TestProgressiveBackoffThroughCheck(t *testing.T) {
	rdb := redistest.Client(t)
	th := bindthrottle.New(rdb, testConfig())
	ctx := context.Background()
	sid, ip := uuid.NewString(), uuid.NewString()

	// Five failures, threshold 3 → two past the threshold → base doubled twice = 4s.
	for i := 0; i < 5; i++ {
		if err := th.RecordFailure(ctx, sid, ip); err != nil {
			t.Fatalf("record failure %d: %v", i, err)
		}
	}
	dec, err := th.Check(ctx, sid, ip)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !dec.Blocked || dec.RetryAfter != 4*time.Second {
		t.Fatalf("blocked=%v retryAfter=%v, want blocked=true retryAfter=4s", dec.Blocked, dec.RetryAfter)
	}
}

func TestIPCounterBlocksAcrossSystemIDs(t *testing.T) {
	rdb := redistest.Client(t)
	th := bindthrottle.New(rdb, testConfig())
	ctx := context.Background()
	ip := uuid.NewString()

	// Three failures from one IP but three different system_ids: no single system_id hits the
	// threshold, yet the shared IP counter does.
	for i := 0; i < 3; i++ {
		if err := th.RecordFailure(ctx, uuid.NewString(), ip); err != nil {
			t.Fatalf("record failure %d: %v", i, err)
		}
	}

	// A brand-new system_id from that IP is still blocked by the IP counter.
	dec, err := th.Check(ctx, uuid.NewString(), ip)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !dec.Blocked {
		t.Fatal("fresh system_id from a hot IP not blocked, want blocked by IP counter")
	}
	if dec.Subject != bindthrottle.SubjectIP {
		t.Fatalf("subject = %q, want %q (IP counter tripped)", dec.Subject, bindthrottle.SubjectIP)
	}
}

func TestResetClearsSystemIDButNotIP(t *testing.T) {
	rdb := redistest.Client(t)
	th := bindthrottle.New(rdb, testConfig())
	ctx := context.Background()
	sid, ip := uuid.NewString(), uuid.NewString()

	for i := 0; i < 3; i++ {
		if err := th.RecordFailure(ctx, sid, ip); err != nil {
			t.Fatalf("record failure %d: %v", i, err)
		}
	}
	if err := th.Reset(ctx, sid); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// The system_id counter is cleared: the same system_id from a fresh IP is allowed again.
	dec, err := th.Check(ctx, sid, uuid.NewString())
	if err != nil {
		t.Fatalf("check system_id: %v", err)
	}
	if dec.Blocked {
		t.Fatal("system_id still blocked after reset, want cleared")
	}

	// The IP counter survived the reset: any bind from that IP is still blocked, so a legitimate bind
	// cannot launder an attacker sharing the source IP.
	dec, err = th.Check(ctx, uuid.NewString(), ip)
	if err != nil {
		t.Fatalf("check ip: %v", err)
	}
	if !dec.Blocked {
		t.Fatal("IP counter cleared by reset, want preserved")
	}
}

// TestWindowForgivesAfterQuiet proves the sliding window: with a one-second window, a lockout lifts
// once a subject stops failing for the whole window. It is the one test that must pass real time,
// because the window is enforced by Redis' own TTL, which no injected clock can fast-forward.
func TestWindowForgivesAfterQuiet(t *testing.T) {
	rdb := redistest.Client(t)
	cfg := testConfig()
	cfg.MaxFailures = 1
	cfg.Window = time.Second
	th := bindthrottle.New(rdb, cfg)
	ctx := context.Background()
	sid, ip := uuid.NewString(), uuid.NewString()

	if err := th.RecordFailure(ctx, sid, ip); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	dec, err := th.Check(ctx, sid, ip)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !dec.Blocked {
		t.Fatal("not blocked right after a failure, want blocked")
	}

	// Stay quiet past the window: both counters expire and the subject is forgiven.
	time.Sleep(1500 * time.Millisecond)
	dec, err = th.Check(ctx, sid, ip)
	if err != nil {
		t.Fatalf("check after window: %v", err)
	}
	if dec.Blocked {
		t.Fatal("still blocked after the window elapsed, want forgiven")
	}
}
