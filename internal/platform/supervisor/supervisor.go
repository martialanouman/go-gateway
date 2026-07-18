// Package supervisor runs the long-lived components of a service main. A Group runs each component in
// its own goroutine under a shared context; when the first component returns an error or the parent
// context is cancelled, it cancels the rest, waits for them to stop, and returns the first error (or
// nil on a clean shutdown). It captures the identical wg/errCh/select scaffolding the pipeline
// service mains otherwise re-inline.
//
// It is the UNORDERED supervisor: all components tear down together. A service whose components have
// a shutdown ordering constraint (e.g. drain the HTTP listener before the writer it feeds) must
// sequence its own teardown rather than use a Group.
package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Component is a supervised unit of work: it runs until its context is cancelled, returning nil on a
// clean stop or an error to bring the whole group down.
type Component func(context.Context) error

type namedComponent struct {
	name string
	fn   Component
}

// Group collects components and runs them together under one lifecycle. The zero value is ready to
// use; add components with Add, then call Run.
type Group struct {
	comps []namedComponent
}

// Add registers a component under a name used in shutdown logs and error wrapping. Call it before
// Run; the order of registration does not imply any shutdown order.
func (g *Group) Add(name string, fn Component) {
	g.comps = append(g.comps, namedComponent{name: name, fn: fn})
}

// Run starts every registered component, then blocks until ctx is cancelled or the first component
// fails. It then cancels all components, waits for them, and returns the first non-nil error — nil on
// a clean ctx-driven shutdown. errCh is sized to the component count, so no goroutine blocks
// reporting its error even if several fail at once.
func (g *Group) Run(ctx context.Context, logger *slog.Logger) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(g.comps))
	for _, c := range g.comps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.fn(runCtx); err != nil {
				select {
				case errCh <- fmt.Errorf("%s: %w", c.name, err):
				default:
				}
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}
	cancel()
	wg.Wait()

	if runErr != nil {
		return runErr
	}
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
