package config

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Invalidation pub/sub channels (Appendix B). The control plane announces a coarse change on
// ChannelConfigChanged; config-sync coalesces those and republishes on ChannelSnapshotInvalidation,
// which every data-plane pod subscribes to and treats as "rebuild your snapshot". The circuit breaker
// reuses the invalidation channel in M8 (step-123), so subscribers stay payload-agnostic.
const (
	ChannelConfigChanged        = "config:changed"
	ChannelSnapshotInvalidation = "breaker:events"
)

// defaultCoalesceWindow is the trailing window a burst of notifications collapses into one rebuild.
// Short enough to stay "near-immediate", long enough to absorb a bulk admin operation's fan-out.
const defaultCoalesceWindow = 250 * time.Millisecond

// Stream is the message source a Watcher consumes. *redisstore.Subscription satisfies it (its Receive
// handles reconnection internally, so a transient blip is a retryable error, not a dead stream).
type Stream interface {
	Receive(ctx context.Context) ([]byte, error)
	Close() error
}

// Watcher subscribes to an invalidation channel and runs rebuild on each notification, coalescing a
// burst into a single rebuild (trailing window + a one-slot pending tick that also absorbs
// notifications arriving *during* a rebuild). rebuild must not mutate anything on error: a failed
// rebuild is logged and the current state keeps serving (no downtime), and the next notification
// retries. It is a supervised component — Run returns nil on a clean stop, with no leaked goroutine.
type Watcher struct {
	open    func(ctx context.Context) (Stream, error)
	rebuild func(ctx context.Context) error
	window  time.Duration
	logger  *slog.Logger
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
	w := &Watcher{open: open, rebuild: rebuild, window: defaultCoalesceWindow, logger: slog.Default()}
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
	defer func() {
		if timer != nil {
			timer.Stop()
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
			if timerC == nil { // arm the trailing window; further ticks inside it are coalesced
				timer = time.NewTimer(w.window)
				timerC = timer.C
			}
		case <-timerC:
			timer, timerC = nil, nil
			if rerr := w.rebuild(ctx); rerr != nil {
				w.logger.Error("config watcher: rebuild failed; keeping current state", "err", rerr)
			}
		}
	}
}
