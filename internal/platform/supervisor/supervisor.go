// Package supervisor runs the long-lived components of a service main. A Group runs each component in
// its own goroutine under a shared context; when the first component returns an error or the parent
// context is cancelled, it cancels the rest, waits for them to stop, and returns the first error (or
// nil on a clean shutdown). It captures the identical wg/errCh/select scaffolding the pipeline
// service mains otherwise re-inline.
//
// It is the UNORDERED supervisor: all components tear down together. A service whose components have
// a shutdown ordering constraint (e.g. drain the HTTP listener before the writer it feeds) uses
// Ordered instead, which drains in reverse registration order.
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

// DrainHook runs once at the very start of a shutdown, before any component is cancelled. It exists
// because the drain has an instant that no component can occupy: a pod must announce itself
// NOT-ready and give the load balancer time to stop routing to it BEFORE its listeners close.
// Running that as a component cannot work — components tear down, they do not run first.
//
// It receives a context detached from the one that just fired — the parent is already cancelled by the
// time a hook runs, so a hook that must wait can actually wait. That detached context carries the
// parent's values but is NEVER cancelled: a hook is responsible for bounding its own wait, because
// neither Group nor Ordered imposes an overall drain budget today.
//
// Hooks also run when a COMPONENT FAILS, not only on SIGTERM. That is deliberate: a consumer that dies
// under load leaves the pod in the Service endpoints and still being handed work, so it must announce
// itself not-ready before the rest tears down. The supervisor cannot tell that case from a boot
// failure — that would mean knowing whether the pod was ever ready, which lives above it — so a
// service that fails to bind its port also pays the drain delay before reporting the error.
// TestDrainHooksRunOnComponentFailureToo pins it.
type DrainHook func(context.Context)

// runDrainHooks runs every registered hook in registration order on a detached context, before the
// components are torn down. A hook is best-effort: it cannot fail the shutdown, because there is
// nothing left to abort — the process is going down either way.
func runDrainHooks(ctx context.Context, hooks []DrainHook) {
	if len(hooks) == 0 {
		return
	}
	detached := context.WithoutCancel(ctx)
	for _, fn := range hooks {
		fn(detached)
	}
}

type namedComponent struct {
	name string
	fn   Component
}

// Group collects components and runs them together under one lifecycle. The zero value is ready to
// use; add components with Add, then call Run.
type Group struct {
	comps   []namedComponent
	onDrain []DrainHook
}

// Add registers a component under a name used in shutdown logs and error wrapping. Call it before
// Run; the order of registration does not imply any shutdown order.
func (g *Group) Add(name string, fn Component) {
	g.comps = append(g.comps, namedComponent{name: name, fn: fn})
}

// OnDrain registers a hook run once when the drain starts, BEFORE any component is cancelled. See
// DrainHook for why that instant needs a name of its own.
func (g *Group) OnDrain(fn DrainHook) {
	g.onDrain = append(g.onDrain, fn)
}

// Run starts every registered component, then blocks until ctx is cancelled or the first component
// fails. It then cancels all components, waits for them, and returns the first non-nil error — nil on
// a clean ctx-driven shutdown. errCh is sized to the component count, so no goroutine blocks
// reporting its error even if several fail at once.
func (g *Group) Run(ctx context.Context, logger *slog.Logger) error {
	// Detached from the parent, like Ordered's: a component whose context died the instant SIGTERM
	// arrived would already be tearing down before the pre-drain hooks could run, which would make
	// OnDrain unimplementable here. Run cancels runCtx explicitly below, once the hooks have run, so
	// the observable shutdown behaviour is unchanged.
	//nolint:gosec // G118: cancel is invoked below on every path and by the defer safety net.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
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
	runDrainHooks(ctx, g.onDrain)
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

// Ordered runs components that must tear down in a fixed sequence — the reverse of their registration
// order, so a producer added before the sink it feeds is drained first (e.g. an HTTP or SMPP listener
// added after the accepted-CDR writer stops before it, letting an in-flight request's Enqueue still
// land). Each component runs on a context detached from the parent (context.WithoutCancel), so a
// parent cancellation does not stop them all at once; Run drives the drain one component at a time in
// reverse. It captures the per-component cancel/waitgroup/errCh scaffolding the pipeline mains
// otherwise re-inline. The zero value is ready to use.
type Ordered struct {
	comps   []namedComponent
	onDrain []DrainHook
}

// Add registers a component under a name used in shutdown logs and error wrapping. Registration order
// IS the shutdown order: the last component added is drained first.
func (o *Ordered) Add(name string, fn Component) {
	o.comps = append(o.comps, namedComponent{name: name, fn: fn})
}

// OnDrain registers a hook run once when the drain starts, BEFORE the first component is drained —
// that is, before even the last-registered one. See DrainHook.
func (o *Ordered) OnDrain(fn DrainHook) {
	o.onDrain = append(o.onDrain, fn)
}

// Run starts every component on its own detached, cancellable context, then blocks until ctx is
// cancelled or the first component fails. It then drains the components in reverse registration order —
// cancelling each and waiting for it to stop before moving to the next — and returns the first non-nil
// error, or nil on a clean ctx-driven shutdown.
func (o *Ordered) Run(ctx context.Context, logger *slog.Logger) error {
	cancels := make([]context.CancelFunc, len(o.comps))
	dones := make([]chan struct{}, len(o.comps))
	errCh := make(chan error, len(o.comps))

	for i, c := range o.comps {
		// Detached from the parent so a signal does not stop every component at once; the ordered drain
		// below cancels them one by one.
		//nolint:gosec // G118: cancel is stored in cancels[] and invoked by the reverse drain and the defer safety net below.
		compCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		cancels[i] = cancel
		done := make(chan struct{})
		dones[i] = done
		go func() {
			defer close(done)
			if err := c.fn(compCtx); err != nil {
				select {
				case errCh <- fmt.Errorf("%s: %w", c.name, err):
				default:
				}
			}
		}()
	}
	// Safety net: guarantee every context is cancelled even on an unexpected early return (the reverse
	// drain below cancels them all in the normal path; cancel is idempotent).
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down", "reason", context.Cause(ctx))
	case runErr = <-errCh:
		logger.Error("component failed, shutting down", "err", runErr)
	}

	runDrainHooks(ctx, o.onDrain)

	// Drain in reverse registration order: cancel each component and wait for it before the next.
	for i := len(o.comps) - 1; i >= 0; i-- {
		cancels[i]()
		<-dones[i]
	}

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
