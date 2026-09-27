package config_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
)

// fakeStream is a channel-driven config.Stream: the test emits notifications on demand, so Watcher
// coalescing is exercised deterministically without Redis.
type fakeStream struct {
	msgs   chan []byte
	closed atomic.Bool
}

func newFakeStream() *fakeStream { return &fakeStream{msgs: make(chan []byte, 16)} }

func (f *fakeStream) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m := <-f.msgs:
		return m, nil
	}
}

func (f *fakeStream) Close() error { f.closed.Store(true); return nil }

func (f *fakeStream) emit() { f.msgs <- []byte("x") }

// runWatcher starts w.Run in the background and returns a stop func that cancels and waits for it.
func runWatcher(t *testing.T, w *config.Watcher) (context.Context, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	return ctx, func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("Run returned %v, want nil on clean stop", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Run did not return within 2s of cancel (leaked goroutine?)")
		}
	}
}

// TestWatcherRebuildsOnNotification: a single notification triggers exactly one rebuild.
func TestWatcherRebuildsOnNotification(t *testing.T) {
	stream := newFakeStream()
	rebuilt := make(chan struct{}, 4)
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		func(context.Context) error { rebuilt <- struct{}{}; return nil },
		config.WithWindow(5*time.Millisecond),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit()
	select {
	case <-rebuilt:
	case <-time.After(2 * time.Second):
		t.Fatal("no rebuild after a notification")
	}
}

// TestWatcherCoalescesNotificationsDuringRebuild: notifications arriving while a rebuild is in flight
// collapse into a single follow-up rebuild — three notifications yield two rebuilds, never three. The
// gate makes this deterministic with no reliance on the window's real duration.
func TestWatcherCoalescesNotificationsDuringRebuild(t *testing.T) {
	stream := newFakeStream()
	var count atomic.Int32
	started := make(chan struct{}, 4)
	gate := make(chan struct{})
	var gated atomic.Bool

	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		func(context.Context) error {
			count.Add(1)
			started <- struct{}{}
			if gated.CompareAndSwap(false, true) {
				<-gate // only the FIRST rebuild blocks, holding the loop while more notifications land
			}
			return nil
		},
		config.WithWindow(5*time.Millisecond),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit() // arms the window → rebuild #1 fires and blocks on the gate
	<-started
	// With the loop stuck in rebuild #1, land two more notifications: they collapse into one pending
	// tick, so exactly one follow-up rebuild will run.
	stream.emit()
	stream.emit()
	close(gate) // release rebuild #1
	<-started   // rebuild #2 (the coalesced follow-up)

	// Give any erroneous third rebuild a chance to appear, then assert exactly two ran.
	select {
	case <-started:
		t.Fatal("a third rebuild ran; the two mid-rebuild notifications were not coalesced")
	case <-time.After(100 * time.Millisecond):
	}
	if got := count.Load(); got != 2 {
		t.Errorf("rebuilds = %d, want 2 (three notifications, one coalesced pair)", got)
	}
}

// TestWatcherRebuildFailureKeepsRunning: a failed rebuild does not stop the Watcher or propagate — the
// next notification retries (the current state keeps serving in between).
func TestWatcherRebuildFailureKeepsRunning(t *testing.T) {
	stream := newFakeStream()
	attempts := make(chan struct{}, 4)
	var n atomic.Int32
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		func(context.Context) error {
			attempts <- struct{}{}
			if n.Add(1) == 1 {
				return errors.New("rebuild boom")
			}
			return nil
		},
		config.WithWindow(5*time.Millisecond),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit()
	<-attempts // first rebuild: fails
	stream.emit()
	select {
	case <-attempts: // second rebuild: the Watcher kept running and retried
	case <-time.After(2 * time.Second):
		t.Fatal("Watcher did not retry after a rebuild failure")
	}
}

// TestWatcherCoalescesBurstBeforeRebuild: several notifications inside one trailing window collapse to
// a single rebuild. A generous window makes the tight burst land together deterministically.
func TestWatcherCoalescesBurstBeforeRebuild(t *testing.T) {
	stream := newFakeStream()
	rebuilt := make(chan struct{}, 8)
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		func(context.Context) error { rebuilt <- struct{}{}; return nil },
		config.WithWindow(300*time.Millisecond),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit()
	stream.emit()
	stream.emit()

	select {
	case <-rebuilt:
	case <-time.After(2 * time.Second):
		t.Fatal("no rebuild after a burst")
	}
	select {
	case <-rebuilt:
		t.Fatal("a second rebuild ran; the burst was not coalesced into one")
	case <-time.After(200 * time.Millisecond):
	}
}

// errThenStream returns one transient error, then behaves like a normal fake — exercising the receive
// retry branch (the Watcher must recover and keep serving notifications).
type errThenStream struct {
	*fakeStream
	failed atomic.Bool
}

func (s *errThenStream) Receive(ctx context.Context) ([]byte, error) {
	if s.failed.CompareAndSwap(false, true) {
		return nil, errors.New("transient receive failure")
	}
	return s.fakeStream.Receive(ctx)
}

// TestWatcherRecoversFromReceiveError: a transient receive error is retried, not fatal — a later
// notification still triggers a rebuild.
func TestWatcherRecoversFromReceiveError(t *testing.T) {
	stream := &errThenStream{fakeStream: newFakeStream()}
	rebuilt := make(chan struct{}, 4)
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		func(context.Context) error { rebuilt <- struct{}{}; return nil },
		config.WithWindow(5*time.Millisecond),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit() // delivered after the retry backoff following the first (failed) Receive
	select {
	case <-rebuilt:
	case <-time.After(3 * time.Second): // > the 1s retry backoff
		t.Fatal("Watcher did not recover from a transient receive error")
	}
}

// TestWatcherClosesStreamOnStop: a clean stop closes the subscription (no leaked Redis subscription).
func TestWatcherClosesStreamOnStop(t *testing.T) {
	stream := newFakeStream()
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		func(context.Context) error { return nil },
		config.WithWindow(5*time.Millisecond),
	)
	_, stop := runWatcher(t, w)
	stop()
	if !stream.closed.Load() {
		t.Error("stream not closed after Run stopped")
	}
}

// failingRebuild fails the calls fail picks (numbered from 1) and records when each call started.
type failingRebuild struct {
	fail  func(call int32) bool
	calls atomic.Int32
	at    chan time.Time
}

func newFailingRebuild(fail func(call int32) bool) *failingRebuild {
	return &failingRebuild{fail: fail, at: make(chan time.Time, 64)}
}

func failsFirst(n int32) func(int32) bool { return func(call int32) bool { return call <= n } }

func (f *failingRebuild) rebuild(context.Context) error {
	f.at <- time.Now()
	if f.fail(f.calls.Add(1)) {
		return errors.New("rebuild boom")
	}
	return nil
}

func (f *failingRebuild) next(t *testing.T, why string) time.Time {
	t.Helper()
	select {
	case at := <-f.at:
		return at
	case <-time.After(2 * time.Second):
		t.Fatalf("no rebuild within 2s: %s", why)
		return time.Time{}
	}
}

// TestWatcherRetriesAFailedRebuildWithoutANotification: a failed rebuild is replayed on its own. Waiting
// for the next invalidation would leave a pod serving a stale config for as long as the control plane
// stays silent — the whole night, typically (step-395).
func TestWatcherRetriesAFailedRebuildWithoutANotification(t *testing.T) {
	stream := newFakeStream()
	rb := newFailingRebuild(failsFirst(1))
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		rb.rebuild,
		config.WithWindow(5*time.Millisecond),
		config.WithRetryBackoff(5*time.Millisecond, 20*time.Millisecond),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit()
	rb.next(t, "the notification did not trigger the first rebuild")
	rb.next(t, "the failed rebuild was never replayed without a new notification")
}

// TestWatcherRetryBackoffDoublesUpToItsCap: consecutive failures are retried after a delay that doubles
// from the initial backoff and stops growing at the cap. A backoff pinned at zero would hammer a database
// that is already down; an uncapped one would leave the pod stale for longer than the outage; one pinned
// at the cap would make every transient blip cost the full cap.
func TestWatcherRetryBackoffDoublesUpToItsCap(t *testing.T) {
	const initial, capped = 20 * time.Millisecond, 160 * time.Millisecond
	stream := newFakeStream()
	rb := newFailingRebuild(func(int32) bool { return true })
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		rb.rebuild,
		config.WithWindow(5*time.Millisecond),
		config.WithRetryBackoff(initial, capped),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit()
	prev := rb.next(t, "the notification did not trigger the first rebuild")
	gaps := make([]time.Duration, 0, 6)
	for range 6 {
		at := rb.next(t, "a failed rebuild was not retried")
		gaps = append(gaps, at.Sub(prev))
		prev = at
	}
	for i, floor := range []time.Duration{initial, 2 * initial, 4 * initial, capped, capped, capped} {
		if gaps[i] < floor {
			t.Errorf("retry %d came %v after the failure, want at least %v (gaps %v)", i+1, gaps[i], floor, gaps)
		}
	}
	// Timers never fire early, so only these two ceilings carry timing risk, and both keep a wide margin.
	if gaps[0] >= capped/2 {
		t.Errorf("the first retry came %v after the failure: the backoff did not start from %v (gaps %v)", gaps[0], initial, gaps)
	}
	// Uncapped, the sixth delay would be 640 ms.
	if gaps[5] >= 3*capped {
		t.Errorf("the sixth retry came %v after the failure: the backoff grew past its %v cap (gaps %v)", gaps[5], capped, gaps)
	}
}

// TestWatcherRetryBackoffResetsOnSuccess: a success ends the outage, so the next failure starts again from
// the initial backoff instead of inheriting the delay the previous outage had grown to.
func TestWatcherRetryBackoffResetsOnSuccess(t *testing.T) {
	const initial, capped = 20 * time.Millisecond, 2 * time.Second
	stream := newFakeStream()
	// Calls 1-5 fail and grow the backoff to 320 ms, call 6 succeeds, call 7 fails again.
	rb := newFailingRebuild(func(call int32) bool { return call <= 5 || call == 7 })
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		rb.rebuild,
		config.WithWindow(5*time.Millisecond),
		config.WithRetryBackoff(initial, capped),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit()
	for range 6 {
		rb.next(t, "the outage was not retried through to its recovery")
	}
	stream.emit()
	failedAt := rb.next(t, "the notification after the recovery did not trigger a rebuild")
	retryAt := rb.next(t, "the failure after the recovery was not retried")
	if gap := retryAt.Sub(failedAt); gap >= 300*time.Millisecond {
		t.Errorf("the first retry after a success came %v later: the backoff was not reset to %v", gap, initial)
	}
}

// TestWatcherNotificationCutsTheRetryBackoffShort: an invalidation arriving while a retry waits is
// rebuilt within the coalesce window, not after the backoff — it announces a change, and a healthy
// watcher answers one in a window. A burst still collapses into that ONE rebuild, and cutting the wait
// does not end the outage: the next failure keeps doubling instead of starting over from initial.
func TestWatcherNotificationCutsTheRetryBackoffShort(t *testing.T) {
	const initial = 300 * time.Millisecond
	stream := newFakeStream()
	rb := newFailingRebuild(func(int32) bool { return true })
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		rb.rebuild,
		config.WithWindow(5*time.Millisecond),
		config.WithRetryBackoff(initial, time.Minute),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	stream.emit()
	failedAt := rb.next(t, "the notification did not trigger the first rebuild")
	stream.emit()
	stream.emit()
	stream.emit()
	cutAt := rb.next(t, "a notification during the retry backoff waited for the backoff")
	if gap := cutAt.Sub(failedAt); gap >= initial-50*time.Millisecond {
		t.Errorf("the notification was rebuilt %v after the failure: it waited out the %v backoff", gap, initial)
	}
	retryAt := rb.next(t, "the failure after the cut was not retried")
	if gap := retryAt.Sub(cutAt); gap < 2*initial {
		t.Errorf("the retry after the cut came %v later, want at least %v: either the burst was not "+
			"coalesced into one rebuild, or the cut reset an outage that is still going on", gap, 2*initial)
	}
}

// TestWatcherWindowIsNotPushedBackBySustainedNotifications: the window is armed by the first notification
// and NOT re-armed by the ones inside it. Re-arming would turn it into a debounce that a steady trickle
// of invalidations — a bulk admin operation, a flapping breaker — starves of any rebuild at all.
func TestWatcherWindowIsNotPushedBackBySustainedNotifications(t *testing.T) {
	const window = 50 * time.Millisecond
	stream := newFakeStream()
	rb := newFailingRebuild(func(int32) bool { return false })
	w := config.NewWatcher(
		func(context.Context) (config.Stream, error) { return stream, nil },
		rb.rebuild,
		config.WithWindow(window),
	)
	_, stop := runWatcher(t, w)
	defer stop()

	trickleEnd := time.Now().Add(20 * window)
	for time.Now().Before(trickleEnd) {
		stream.emit()
		time.Sleep(window / 5)
	}
	select {
	case at := <-rb.at:
		if !at.Before(trickleEnd) {
			t.Errorf("the first rebuild ran only after the notifications stopped: the window slid with them")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no rebuild at all")
	}
}
