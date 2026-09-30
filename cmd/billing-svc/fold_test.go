package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeFolder struct {
	batches []int64
	calls   int
	oldest  time.Time
	pending bool
	err     error
}

func (f *fakeFolder) FoldOnce(context.Context, int) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	if f.calls >= len(f.batches) {
		f.calls++
		return 0, nil
	}
	n := f.batches[f.calls]
	f.calls++
	return n, nil
}

func (f *fakeFolder) OldestPendingDelta(context.Context) (time.Time, bool, error) {
	return f.oldest, f.pending, nil
}

func newLagGauge() prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_lag_seconds"})
}

// TestFoldTickDrainsABacklog: a full batch means more deltas wait, so one tick keeps folding until a batch
// comes back short — a backlog is caught up in one tick, not one batch per second.
func TestFoldTickDrainsABacklog(t *testing.T) {
	f := &fakeFolder{batches: []int64{foldBatch, foldBatch, 3}}
	foldTick(context.Background(), f, newLagGauge(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if f.calls != 3 {
		t.Errorf("FoldOnce calls = %d, want 3 (two full batches, then a short one)", f.calls)
	}
}

// TestFoldTickPublishesTheLag: the gauge is the age of the oldest unfolded delta, 0 when none waits.
func TestFoldTickPublishesTheLag(t *testing.T) {
	lag := newLagGauge()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	foldTick(context.Background(), &fakeFolder{oldest: time.Now().Add(-7 * time.Second), pending: true}, lag, logger)
	if got := testutil.ToFloat64(lag); got < 7 || got > 9 {
		t.Errorf("lag = %.1fs, want about 7s", got)
	}
	foldTick(context.Background(), &fakeFolder{}, lag, logger)
	if got := testutil.ToFloat64(lag); got != 0 {
		t.Errorf("lag with nothing pending = %.1fs, want 0", got)
	}
}

// TestFoldTickSurvivesAFailedFold: a fold error (a 40P01 against an admin tx) is logged and the next tick
// retries; the tick must not spin on it.
func TestFoldTickSurvivesAFailedFold(t *testing.T) {
	f := &fakeFolder{err: errors.New("deadlock detected")}
	foldTick(context.Background(), f, newLagGauge(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if f.calls > 1 {
		t.Errorf("FoldOnce calls after an error = %d, want 1", f.calls)
	}
}
