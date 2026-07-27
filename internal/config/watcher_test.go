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
