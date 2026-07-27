// Package async runs bounded, fire-and-forget background jobs with a graceful drain. It exists for
// admin endpoints that must accept work and answer 202 without holding the HTTP connection for the
// whole job (e.g. a bulk import), while honouring the project rule that no goroutine outlives its
// owner: every job is tracked and either finishes or is cancelled at shutdown.
package async

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

// ErrBusy is returned by Go when every worker slot is taken. The caller answers a retryable status
// rather than blocking the request.
var ErrBusy = errors.New("async: concurrency limit reached")

// ErrClosed is returned by Go after Close: the runner no longer accepts work.
var ErrClosed = errors.New("async: runner closed")

// Runner starts each job in its own goroutine, bounded by a semaphore. A job's context is owned by the
// Runner, not by the request that submitted it, so the job keeps running after the 202 is written and
// stops only when it returns or Close cancels it at the drain deadline.
type Runner struct {
	sem    chan struct{}
	jobCtx context.Context
	cancel context.CancelFunc
	logger *slog.Logger

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// New returns a Runner that runs at most maxConcurrent jobs at once. logger records job failures; a nil
// logger falls back to the default so callers need not guard it.
func New(maxConcurrent int, logger *slog.Logger) *Runner {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runner{sem: make(chan struct{}, maxConcurrent), jobCtx: ctx, cancel: cancel, logger: logger}
}

// Go starts job immediately if a slot is free, returning nil; it returns ErrBusy when saturated or
// ErrClosed after Close. It never blocks the caller. job receives the Runner's context (cancelled at
// the Close deadline), never the caller's request context. A non-nil error from job is logged with
// name; its return value is otherwise discarded (fire-and-forget).
func (r *Runner) Go(name string, job func(ctx context.Context) error) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	select {
	case r.sem <- struct{}{}:
	default:
		r.mu.Unlock()
		return ErrBusy
	}
	r.wg.Add(1) // under mu, paired with Close setting closed under mu: no Add-after-Wait race
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer func() { <-r.sem }()
		// Recover here, not on the request goroutine: this job runs on a fresh goroutine after the 202
		// is written, so chi's Recoverer middleware gives it no cover. An unrecovered panic would take
		// down the whole process (and every other in-flight request); contain it to this job instead.
		defer func() {
			if p := recover(); p != nil {
				r.logger.Error("background job panicked", "job", name, "panic", p)
			}
		}()
		if err := job(r.jobCtx); err != nil {
			r.logger.Error("background job failed", "job", name, "err", err)
		}
	}()
	return nil
}

// Close stops accepting new work and waits for in-flight jobs to finish. If ctx expires first it
// cancels the jobs' context (so a blocked job — e.g. a slow query — is aborted) and still waits for
// them to return, so no goroutine leaks past Close. It returns ctx.Err() on a deadline, else nil.
//
// The deadline only *triggers* cancellation; it does not force-kill a job. A job that ignores its
// context makes Close block past the deadline, so every job must honour jctx (BulkUpsert does, via
// pgx). Callers must not treat a returned deadline error as "the work stopped" — only as "we asked".
func (r *Runner) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()

	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()

	select {
	case <-done:
		r.cancel()
		return nil
	case <-ctx.Done():
		r.cancel() // ask jobs to stop, then wait for them to actually return
		<-done
		return ctx.Err()
	}
}
