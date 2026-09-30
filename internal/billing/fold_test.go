package billing_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/billing"
)

type fakeFoldStore struct {
	batches []int64
	always  int64
	calls   int
	oldest  time.Time
	pending bool
	err     error
}

func (f *fakeFoldStore) FoldOnce(context.Context, int) (int64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	if f.always > 0 {
		return f.always, nil
	}
	if f.calls > len(f.batches) {
		return 0, nil
	}
	return f.batches[f.calls-1], nil
}

func (f *fakeFoldStore) OldestPendingDelta(context.Context) (time.Time, bool, error) {
	return f.oldest, f.pending, nil
}

type lagProbe struct {
	value float64
	set   bool
}

func (l *lagProbe) Set(seconds float64) { l.value, l.set = seconds, true }

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestDrainOnceCatchesUpABacklog: a full batch means more deltas wait, so one pass keeps folding until a
// batch comes back short — a backlog is caught up in one tick, not one batch per second.
func TestDrainOnceCatchesUpABacklog(t *testing.T) {
	store := &fakeFoldStore{batches: []int64{5000, 5000, 3}}
	billing.NewFolder(store, &lagProbe{}, quiet()).DrainOnce(context.Background())
	if store.calls != 3 {
		t.Errorf("FoldOnce calls = %d, want 3 (two full batches, then a short one)", store.calls)
	}
}

// TestDrainOnceIsBoundedUnderAnEndlessBacklog: when deltas arrive faster than one pass folds them, the pass
// must still end and publish the lag — the very moment the lag alert exists for.
func TestDrainOnceIsBoundedUnderAnEndlessBacklog(t *testing.T) {
	store := &fakeFoldStore{always: 5000, oldest: time.Now().Add(-40 * time.Second), pending: true}
	lag := &lagProbe{}
	billing.NewFolder(store, lag, quiet()).DrainOnce(context.Background())
	if store.calls < 2 || store.calls > 20 {
		t.Errorf("FoldOnce calls = %d, want a few full batches then a stop", store.calls)
	}
	if !lag.set || lag.value < 40 {
		t.Errorf("lag = %.1fs (set %v), want about 40s published despite the backlog", lag.value, lag.set)
	}
}

// TestDrainOncePublishesTheLag: the lag is the age of the oldest unfolded delta, 0 when none waits, never
// negative when the database clock runs ahead of the pod's.
func TestDrainOncePublishesTheLag(t *testing.T) {
	for _, tc := range []struct {
		name     string
		oldest   time.Time
		pending  bool
		min, max float64
	}{
		{"waiting", time.Now().Add(-7 * time.Second), true, 7, 9},
		{"none", time.Time{}, false, 0, 0},
		{"clock ahead", time.Now().Add(5 * time.Second), true, 0, 0},
	} {
		lag := &lagProbe{}
		billing.NewFolder(&fakeFoldStore{oldest: tc.oldest, pending: tc.pending}, lag, quiet()).DrainOnce(context.Background())
		if !lag.set || lag.value < tc.min || lag.value > tc.max {
			t.Errorf("%s: lag = %.1fs (set %v), want %.0f..%.0fs", tc.name, lag.value, lag.set, tc.min, tc.max)
		}
	}
}

// TestDrainOnceStopsOnAFailedFold: a fold error (a 40P01 against an admin tx) waits for the next tick.
func TestDrainOnceStopsOnAFailedFold(t *testing.T) {
	store := &fakeFoldStore{err: errors.New("deadlock detected")}
	billing.NewFolder(store, &lagProbe{}, quiet()).DrainOnce(context.Background())
	if store.calls != 1 {
		t.Errorf("FoldOnce calls after an error = %d, want 1", store.calls)
	}
}
