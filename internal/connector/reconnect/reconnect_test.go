package reconnect_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
)

var errDropped = errors.New("link dropped")

// recordingSleeper captures the backoff schedule and never actually waits.
type recordingSleeper struct{ delays []time.Duration }

func (r *recordingSleeper) sleep(_ context.Context, d time.Duration) error {
	r.delays = append(r.delays, d)
	return nil
}

// noJitter makes the schedule deterministic.
func noJitter(d time.Duration, _ int) time.Duration { return d }

func testCfg() reconnect.Config {
	return reconnect.Config{Enabled: true, InitialDelay: 100 * time.Millisecond, Multiplier: 2, MaxDelay: time.Second}
}

// TestReconnectBacksOffUntilHealthy: a link that drops a few times is retried with exponential backoff,
// then serves cleanly.
func TestReconnectBacksOffUntilHealthy(t *testing.T) {
	sl := &recordingSleeper{}
	loop := reconnect.New(testCfg(), reconnect.WithSleeper(sl.sleep), reconnect.WithJitterer(noJitter))

	attempts := 0
	err := loop.Run(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 4 {
			return errDropped // drops three times, then stays up (returns nil)
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("Run = %v, want nil (recovered)", err)
	}
	if attempts != 4 {
		t.Errorf("attempts = %d, want 4", attempts)
	}
	// Backoff observed: 100ms, 200ms, 400ms (geometric, clamped at 1s).
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	if len(sl.delays) != len(want) {
		t.Fatalf("delays = %v, want %v", sl.delays, want)
	}
	for i, w := range want {
		if sl.delays[i] != w {
			t.Errorf("delay[%d] = %v, want %v", i, sl.delays[i], w)
		}
	}
}

// TestBackoffClampedToMax: the delay never exceeds MaxDelay.
func TestBackoffClampedToMax(t *testing.T) {
	sl := &recordingSleeper{}
	cfg := reconnect.Config{Enabled: true, InitialDelay: 400 * time.Millisecond, Multiplier: 10, MaxDelay: time.Second, MaxAttempts: 4}
	loop := reconnect.New(cfg, reconnect.WithSleeper(sl.sleep), reconnect.WithJitterer(noJitter))

	_ = loop.Run(context.Background(), func(context.Context) error { return errDropped }, nil)
	for i, d := range sl.delays {
		if d > time.Second {
			t.Errorf("delay[%d] = %v exceeds MaxDelay 1s", i, d)
		}
	}
}

// TestDisabledDoesNotRetry: with reconnection off, a drop returns immediately, no backoff.
func TestDisabledDoesNotRetry(t *testing.T) {
	sl := &recordingSleeper{}
	cfg := testCfg()
	cfg.Enabled = false
	loop := reconnect.New(cfg, reconnect.WithSleeper(sl.sleep), reconnect.WithJitterer(noJitter))

	attempts := 0
	err := loop.Run(context.Background(), func(context.Context) error { attempts++; return errDropped }, nil)
	if !errors.Is(err, errDropped) {
		t.Errorf("Run = %v, want the drop error", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (no retry when disabled)", attempts)
	}
	if len(sl.delays) != 0 {
		t.Errorf("delays = %v, want none (no backoff when disabled)", sl.delays)
	}
}

// TestNonRetryableStopsLoop: an error the retryable predicate rejects (e.g. bad credentials, or a
// non-link fault) stops the loop with no retry, even when enabled.
func TestNonRetryableStopsLoop(t *testing.T) {
	sl := &recordingSleeper{}
	loop := reconnect.New(testCfg(), reconnect.WithSleeper(sl.sleep), reconnect.WithJitterer(noJitter))
	fatal := errors.New("ESME_RINVPASWD")

	attempts := 0
	err := loop.Run(context.Background(), func(context.Context) error { attempts++; return fatal },
		func(e error) bool { return !errors.Is(e, fatal) }) // retry everything EXCEPT fatal
	if !errors.Is(err, fatal) {
		t.Errorf("Run = %v, want the fatal error", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (non-retryable → no retry)", attempts)
	}
}

// TestMaxAttempts: the loop gives up after MaxAttempts reconnects.
func TestMaxAttempts(t *testing.T) {
	sl := &recordingSleeper{}
	cfg := testCfg()
	cfg.MaxAttempts = 3
	loop := reconnect.New(cfg, reconnect.WithSleeper(sl.sleep), reconnect.WithJitterer(noJitter))

	attempts := 0
	err := loop.Run(context.Background(), func(context.Context) error { attempts++; return errDropped }, nil)
	if !errors.Is(err, errDropped) {
		t.Errorf("Run = %v, want the drop error after exhausting attempts", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (MaxAttempts)", attempts)
	}
}

// TestContextCancelStops: cancelling the context ends the loop even while backing off.
func TestContextCancelStops(t *testing.T) {
	cfg := testCfg()
	cfg.InitialDelay = time.Hour // would block a long time if not for cancellation
	loop := reconnect.New(cfg, reconnect.WithJitterer(noJitter))
	ctx, cancel := context.WithCancel(context.Background())

	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	err := loop.Run(ctx, func(context.Context) error { return errDropped }, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled", err)
	}
}
