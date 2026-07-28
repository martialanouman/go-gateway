// Package reconnect is the opt-in SMSC auto-reconnection loop (§6.13, step-127): it re-establishes a
// dropped bind with exponential backoff and jitter, bounded by a maximum delay and attempt count, and
// stops hard on a permanent authentication failure (bad password) so a misconfigured connector never
// hammers the SMSC. It is deliberately separate from the circuit breaker: this governs the TCP/bind LINK
// (link_status), not the connector's APPLICATION health (breaker_state) — the two are never conflated.
package reconnect

import (
	"context"
	"time"
)

// Config is a connector's reconnect policy, mirroring the reconnect_* columns (schema §9). The zero
// value is a disabled loop (Enabled false), matching auto_reconnect_enabled's default.
type Config struct {
	Enabled      bool          // auto_reconnect_enabled: opt-in; when false a dropped link is not retried
	InitialDelay time.Duration // reconnect_initial_delay_ms
	Multiplier   float64       // reconnect_multiplier (>= 1)
	MaxDelay     time.Duration // reconnect_max_delay_ms: the backoff ceiling
	JitterPct    int           // reconnect_jitter_pct (0..100): ± randomisation to de-synchronise pods
	MaxAttempts  int           // reconnect_max_attempts: 0 = infinite
}

// Loop drives reconnection. The sleeper and jitterer are injectable so a test can observe the backoff
// schedule deterministically without real time or randomness.
type Loop struct {
	cfg     Config
	sleep   func(ctx context.Context, d time.Duration) error
	jittere func(d time.Duration, pct int) time.Duration
}

// New builds a reconnect loop. A zero/negative InitialDelay or MaxDelay, or a Multiplier below 1, fall
// back to the schema defaults so a partially-filled config still backs off sanely.
func New(cfg Config, opts ...Option) *Loop {
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = time.Minute
	}
	if cfg.Multiplier < 1 {
		cfg.Multiplier = 2
	}
	if cfg.JitterPct < 0 {
		cfg.JitterPct = 0
	}
	if cfg.JitterPct > 100 {
		cfg.JitterPct = 100
	}
	l := &Loop{cfg: cfg, sleep: sleepCtx, jittere: jitter}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Option customises a Loop (tests inject a deterministic sleeper/jitterer).
type Option func(*Loop)

// WithSleeper injects the wait primitive (defaults to a ctx-aware timer).
func WithSleeper(s func(ctx context.Context, d time.Duration) error) Option {
	return func(l *Loop) {
		if s != nil {
			l.sleep = s
		}
	}
}

// WithJitterer injects the jitter function (defaults to ± JitterPct% uniform randomisation).
func WithJitterer(j func(d time.Duration, pct int) time.Duration) Option {
	return func(l *Loop) {
		if j != nil {
			l.jittere = j
		}
	}
}

// Run calls attempt repeatedly to keep the link alive. attempt blocks while the link is healthy and
// returns an error when it drops (or nil on a clean ctx-driven shutdown). Between attempts Run waits an
// exponentially-growing, jittered delay. It returns when:
//   - attempt returns nil (clean stop) → nil;
//   - ctx is cancelled → ctx error;
//   - reconnection is disabled (opt-in off) → the error, unretried;
//   - the error is NOT retryable per retryable → that error (propagated, so the caller/supervisor
//     handles it — e.g. a permanent ESME_RINVPASWD, or a non-link fault like a Kafka blip that should
//     restart rather than churn healthy binds);
//   - MaxAttempts consecutive reconnects fail → the last error.
//
// retryable narrows what triggers a backoff-retry to a genuine link drop; a nil retryable retries every
// error.
func (l *Loop) Run(ctx context.Context, attempt func(context.Context) error, retryable func(error) bool) error {
	delay := l.cfg.InitialDelay
	attempts := 0
	for {
		err := attempt(ctx)
		if err == nil {
			return nil // clean shutdown
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !l.cfg.Enabled {
			return err // opt-in off: a dropped link waits for a manual rebind (step-128)
		}
		if retryable != nil && !retryable(err) {
			return err // permanent (bad credentials) or a non-link fault: do not backoff-retry here
		}
		attempts++
		if l.cfg.MaxAttempts > 0 && attempts >= l.cfg.MaxAttempts {
			return err
		}
		if serr := l.sleep(ctx, l.jittere(delay, l.cfg.JitterPct)); serr != nil {
			return serr // ctx cancelled while waiting
		}
		delay = nextDelay(delay, l.cfg.Multiplier, l.cfg.MaxDelay)
	}
}

// nextDelay grows the backoff geometrically, clamped to the ceiling.
func nextDelay(cur time.Duration, multiplier float64, max time.Duration) time.Duration {
	next := time.Duration(float64(cur) * multiplier)
	if next > max {
		return max
	}
	return next
}

// sleepCtx waits d or until ctx is cancelled, whichever comes first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
