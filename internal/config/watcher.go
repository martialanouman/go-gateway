package config

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

// Invalidation pub/sub channels (Appendix B). The control plane announces a coarse change on
// ChannelConfigChanged; config-sync coalesces those and republishes on ChannelSnapshotInvalidation,
// which every data-plane pod subscribes to and treats as "rebuild your snapshot". The circuit breaker
// reuses the invalidation channel in M8 (step-123), so subscribers stay payload-agnostic.
const (
	ChannelConfigChanged        = "config:changed"
	ChannelSnapshotInvalidation = "breaker:events"
	// ChannelOptOutChanged is announced by a STOP received as an MO (step-398), apart from
	// ChannelSnapshotInvalidation so a STOP reloads the router's opt-out filter only.
	ChannelOptOutChanged = "optout:changed"
)

// defaultCoalesceWindow is the trailing window a burst of notifications collapses into one rebuild.
// Short enough to stay "near-immediate", long enough to absorb a bulk admin operation's fan-out.
const defaultCoalesceWindow = 250 * time.Millisecond

// The failed-rebuild retry backoff matches the router's boot loadWithRetry, so the service retries
// Postgres on one schedule whether it is booting or serving.
const (
	defaultRetryStart = 500 * time.Millisecond
	defaultRetryMax   = 30 * time.Second
)

// Stream is the message source a Watcher consumes. *redisstore.Subscription satisfies it (its Receive
// handles reconnection internally, so a transient blip is a retryable error, not a dead stream).
type Stream interface {
	Receive(ctx context.Context) ([]byte, error)
	Close() error
}

// Watcher subscribes to an invalidation channel and runs rebuild on each notification, coalescing a
// burst into a single rebuild (trailing window + a one-slot pending tick that also absorbs
// notifications arriving *during* a rebuild). rebuild must not mutate anything on error: a failed
// rebuild is logged and the current state keeps serving (no downtime), and it is retried on a bounded
// backoff without waiting for the next notification. It is a supervised component — Run returns nil
// on a clean stop, with no leaked goroutine.
type Watcher struct {
	open       func(ctx context.Context) (Stream, error)
	rebuild    func(ctx context.Context) error
	window     time.Duration
	retryStart time.Duration
	retryMax   time.Duration
	resync     time.Duration
	logger     *slog.Logger
}

// Option configures a Watcher.
type Option func(*Watcher)

// WithWindow overrides the trailing coalesce window.
func WithWindow(d time.Duration) Option {
	return func(w *Watcher) {
		if d > 0 {
			w.window = d
		}
	}
}

// WithRetryBackoff overrides the delay before a failed rebuild is retried: it starts at initial,
// doubles on each consecutive failure up to max, and resets on success.
func WithRetryBackoff(initial, max time.Duration) Option {
	return func(w *Watcher) {
		if initial > 0 && max >= initial {
			w.retryStart, w.retryMax = initial, max
		}
	}
}

// WithResync rebuilds once per period (±10 % jitter) after the last successful rebuild, even without a
// notification, so a lost invalidation leaves the config stale for one period at most. d ≤ 0 disables it.
func WithResync(d time.Duration) Option {
	return func(w *Watcher) {
		if d > 0 {
			w.resync = d
		}
	}
}

// WithLogger sets the logger (defaults to slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(w *Watcher) {
		if l != nil {
			w.logger = l
		}
	}
}

// NewWatcher builds a Watcher. open lazily subscribes to the channel (so a resubscribe on reconnect is
// the stream's concern); rebuild is the action a notification triggers.
func NewWatcher(open func(ctx context.Context) (Stream, error), rebuild func(ctx context.Context) error, opts ...Option) *Watcher {
	w := &Watcher{
		open: open, rebuild: rebuild, window: defaultCoalesceWindow,
		retryStart: defaultRetryStart, retryMax: defaultRetryMax, logger: slog.Default(),
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Run subscribes and processes invalidations until ctx is cancelled. The receive goroutine and the
// coalescing loop both stop on ctx, so nothing outlives Run.
func (w *Watcher) Run(ctx context.Context) error {
	stream, err := w.open(ctx)
	if err != nil {
		return fmt.Errorf("config watcher: subscribe: %w", err)
	}

	// ticks is one-buffered: a burst collapses to one pending tick, and notifications arriving while a
	// rebuild runs (the loop is not selecting) collapse into a single follow-up rebuild.
	ticks := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, rerr := stream.Receive(ctx); rerr != nil {
				if ctx.Err() != nil {
					return
				}
				w.logger.Warn("config watcher: receive failed; retrying", "err", rerr)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			select {
			case ticks <- struct{}{}:
			default:
			}
		}
	}()

	var timer *time.Timer
	var timerC <-chan time.Time
	// retryDelay is the last backoff, kept across a notification so a persisting outage keeps backing
	// off; backingOff says the armed timer is that backoff rather than the coalesce window. A
	// notification cuts a backoff short: it means the database changed, and making it wait up to
	// retryMax would be slower than the window a healthy watcher answers in.
	var retryDelay time.Duration
	var backingOff bool
	arm := func(d time.Duration, backoff bool) {
		if timer != nil {
			timer.Stop()
		}
		timer = time.NewTimer(d)
		timerC = timer.C
		backingOff = backoff
	}
	var resyncTimer *time.Timer
	var resyncC <-chan time.Time
	// Jittered because a notification aligns every pod: without it they would all rebuild at the same
	// second, one period after each admin mutation, for good.
	armResync := func() {
		if w.resync <= 0 {
			return
		}
		if resyncTimer != nil {
			resyncTimer.Stop()
		}
		resyncTimer = time.NewTimer(w.resync - w.resync/10 + rand.N(w.resync/5+1))
		resyncC = resyncTimer.C
	}
	armResync()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if resyncTimer != nil {
			resyncTimer.Stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			// Close the subscription first: a real Redis Receive blocks on the socket and does NOT
			// unblock on ctx cancellation alone, so closing it is what makes the receive goroutine
			// return. Then wait for it to exit — no goroutine outlives Run.
			_ = stream.Close()
			<-done
			return nil
		case <-ticks:
			if timerC == nil || backingOff { // arm the trailing window; further ticks inside it are coalesced
				arm(w.window, false)
			}
		case <-resyncC:
			resyncC = nil
			select {
			case ticks <- struct{}{}:
			default:
			}
		case <-timerC:
			timer, timerC, backingOff = nil, nil, false
			rerr := w.rebuild(ctx)
			if rerr == nil {
				retryDelay = 0
				armResync()
				continue
			}
			retryDelay = min(max(2*retryDelay, w.retryStart), w.retryMax)
			w.logger.Error("config watcher: rebuild failed; keeping current state",
				"err", rerr, "retry_in", retryDelay.String())
			arm(retryDelay, true)
		}
	}
}
