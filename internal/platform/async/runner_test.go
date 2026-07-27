package async_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/async"
)

// TestGoIsNonBlocking: Go returns before the job finishes — the request is not held for the work.
func TestGoIsNonBlocking(t *testing.T) {
	r := async.New(1, nil)
	release := make(chan struct{})
	started := make(chan struct{})

	if err := r.Go("job", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Go: %v", err)
	}
	<-started // the job is running while Go has already returned
	close(release)
	if err := r.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestGoRejectsWhenSaturated: with every slot taken, Go returns ErrBusy instead of blocking or
// spawning an unbounded goroutine.
func TestGoRejectsWhenSaturated(t *testing.T) {
	r := async.New(1, nil)
	release := make(chan struct{})
	started := make(chan struct{})
	if err := r.Go("slow", func(context.Context) error { close(started); <-release; return nil }); err != nil {
		t.Fatalf("first Go: %v", err)
	}
	<-started

	if err := r.Go("rejected", func(context.Context) error { return nil }); !errors.Is(err, async.ErrBusy) {
		t.Fatalf("saturated Go = %v, want ErrBusy", err)
	}
	close(release)
	_ = r.Close(context.Background())
}

// TestCloseDrainsRunningJobs: Close returns only after every started job has finished.
func TestCloseDrainsRunningJobs(t *testing.T) {
	r := async.New(4, nil)
	var done atomic.Int32
	proceed := make(chan struct{})
	const n = 4
	for i := 0; i < n; i++ {
		if err := r.Go("j", func(context.Context) error {
			<-proceed
			done.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("Go %d: %v", i, err)
		}
	}
	close(proceed)
	if err := r.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := done.Load(); got != n {
		t.Errorf("drained %d jobs, want %d (Close must wait for all)", got, n)
	}
}

// TestGoAfterCloseIsRejected: once closed, the runner accepts no more work.
func TestGoAfterCloseIsRejected(t *testing.T) {
	r := async.New(1, nil)
	if err := r.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := r.Go("late", func(context.Context) error { return nil }); !errors.Is(err, async.ErrClosed) {
		t.Fatalf("Go after Close = %v, want ErrClosed", err)
	}
}

// TestPanickingJobIsContained: a job that panics must not crash the process — the Runner recovers it,
// frees the slot, and keeps accepting work. (If recovery were missing, this test goroutine's panic
// would take the whole test binary down.)
func TestPanickingJobIsContained(t *testing.T) {
	r := async.New(1, nil)
	if err := r.Go("boom", func(context.Context) error { panic("kaboom") }); err != nil {
		t.Fatalf("Go: %v", err)
	}
	// Drain the panicking job, then prove the slot was released and the runner still works.
	if err := r.Close(context.Background()); err != nil {
		t.Fatalf("Close after panic: %v", err)
	}
	r2 := async.New(1, nil)
	ran := make(chan struct{})
	if err := r2.Go("after", func(context.Context) error { close(ran); return nil }); err != nil {
		t.Fatalf("Go after a panic: %v", err)
	}
	<-ran
	_ = r2.Close(context.Background())
}

// TestCloseDeadlineCancelsJobContext: when the drain deadline expires, Close cancels the job context
// (so a blocked job is asked to stop) and still waits for the job to return — no leak. Close reports
// the deadline error.
func TestCloseDeadlineCancelsJobContext(t *testing.T) {
	r := async.New(1, nil)
	observed := make(chan struct{})
	started := make(chan struct{})
	if err := r.Go("blocked", func(jctx context.Context) error {
		close(started)
		<-jctx.Done() // only the Runner's cancellation can free this job
		close(observed)
		return jctx.Err()
	}); err != nil {
		t.Fatalf("Go: %v", err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close on deadline = %v, want DeadlineExceeded", err)
	}
	<-observed // the job saw cancellation and returned; Close waited for it
}
