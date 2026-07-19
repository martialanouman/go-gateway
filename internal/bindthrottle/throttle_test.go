package bindthrottle

import (
	"testing"
	"time"
)

// TestBackoffProgressiveAndCapped checks the pure backoff curve: the base delay at the threshold,
// doubling once per failure past it, capped at BackoffMax without overflowing on large counts.
func TestBackoffProgressiveAndCapped(t *testing.T) {
	th := &Throttle{cfg: Config{MaxFailures: 3, BackoffBase: time.Second, BackoffMax: 8 * time.Second}}

	cases := []struct {
		failures int
		want     time.Duration
	}{
		{3, 1 * time.Second}, // at the threshold: the base delay
		{4, 2 * time.Second},
		{5, 4 * time.Second},
		{6, 8 * time.Second},  // would be the cap exactly
		{7, 8 * time.Second},  // capped
		{40, 8 * time.Second}, // still capped, no overflow from repeated doubling
	}
	for _, c := range cases {
		if got := th.backoff(c.failures); got != c.want {
			t.Errorf("backoff(%d) = %v, want %v", c.failures, got, c.want)
		}
	}
}

// TestWindowSecondsFloor guards the EXPIRE argument: a sub-second window must not truncate to zero
// (which would make EXPIRE delete the key immediately), and a normal window passes through in seconds.
func TestWindowSecondsFloor(t *testing.T) {
	th := &Throttle{cfg: Config{Window: 500 * time.Millisecond}}
	if got := th.windowSeconds(); got != 1 {
		t.Errorf("windowSeconds(500ms) = %d, want 1 (floor)", got)
	}

	th.cfg.Window = 15 * time.Minute
	if got := th.windowSeconds(); got != 900 {
		t.Errorf("windowSeconds(15m) = %d, want 900", got)
	}
}
