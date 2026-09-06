// Package supervisor runs the long-lived components of a service main. A Group runs each component in
// its own goroutine under a shared context; when the first component returns an error or the parent
// context is cancelled, it cancels the rest, waits for them to stop, and returns the first error (or
// nil on a clean shutdown). It captures the identical wg/errCh/select scaffolding the pipeline
// service mains otherwise re-inline.
//
// It is the UNORDERED supervisor: all components tear down together. A service whose components have
// a shutdown ordering constraint (e.g. drain the HTTP listener before the writer it feeds) uses
// Ordered instead, which drains in reverse registration order.
//
// Both bound the teardown with a drain budget (the caller passes cfg.DrainBudget). Past it, Run
// stops waiting, logs which components never returned and reports ErrDrainBudgetExceeded — their
// goroutines are left running, which costs nothing since the process is on its way out. Without that
// ceiling a single component that ignores its context holds the pod open until the kubelet's SIGKILL,
// and terminationGracePeriodSeconds becomes arithmetic on a number nothing enforces (step-270).
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// ErrDrainBudgetExceeded is returned when the components did not all stop within the drain budget.
var ErrDrainBudgetExceeded = errors.New("drain budget exceeded")

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
// parent's values but is NEVER cancelled: a hook is responsible for bounding its own wait. The drain
// budget Run enforces starts AFTER the hooks, deliberately — a hook's wait is a deliberate one
// (DRAIN_DELAY, bounded by its own constant), and folding it into the budget would let a slow
// component eat the load balancer's notice period.
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
// fails. It then cancels all components and waits for them under budget, returning the first non-nil
// error — a component's failure first, then ErrDrainBudgetExceeded if the budget ran out, then nil on
// a clean shutdown. errCh is sized to the component count, so no goroutine blocks reporting its error
// even if several fail at once.
func (g *Group) Run(ctx context.Context, logger *slog.Logger, budget time.Duration) error {
	// Detached from the parent, like Ordered's: a component whose context died the instant SIGTERM
	// arrived would already be tearing down before the pre-drain hooks could run, which would make
	// OnDrain unimplementable here. Run cancels runCtx explicitly below, once the hooks have run, so
	// the observable shutdown behaviour is unchanged.
	//nolint:gosec // G118: cancel is invoked below on every path and by the defer safety net.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	dones := make([]chan struct{}, len(g.comps))
	errCh := make(chan error, len(g.comps))
	for i, c := range g.comps {
		done := make(chan struct{})
		dones[i] = done
		go func() {
			defer close(done)
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

	// Every component was cancelled at once, so whatever has not stopped when the budget runs out is
	// genuinely late — unlike Ordered, which can only name the one it was waiting on.
	var stuck []string
	deadline, stop := drainDeadline(budget)
	defer stop()
wait:
	for i := range dones {
		select {
		case <-dones[i]:
		case <-deadline:
			// The timer and the component can become ready in the same instant, and select picks
			// between two ready cases at random — so reaching this arm does not mean anything is
			// still running. notStopped decides; an empty result means they all made it.
			stuck = notStopped(g.comps, dones)
			break wait
		}
	}
	if len(stuck) > 0 {
		logger.Error("drain budget exceeded, abandoning components", "budget", budget, "components", stuck)
	}

	if runErr != nil {
		return runErr
	}
	select {
	case err := <-errCh:
		return err
	default:
	}
	if len(stuck) > 0 {
		return fmt.Errorf("%w after %s: %s still running", ErrDrainBudgetExceeded, budget, strings.Join(stuck, ", "))
	}
	return nil
}

// drainDeadline returns the channel that fires when the drain budget runs out, and the func that
// releases its timer. A non-positive budget yields a nil channel, which blocks forever in a select —
// that is the "no ceiling" behaviour the package had before, kept for tests and for a caller with no
// grace period to respect.
func drainDeadline(budget time.Duration) (<-chan time.Time, func()) {
	if budget <= 0 {
		return nil, func() {}
	}
	t := time.NewTimer(budget)
	return t.C, func() { t.Stop() }
}

// notStopped names the components whose goroutine has not returned, in registration order. It is a
// snapshot taken the instant the budget expired: one of them may stop a microsecond later, and naming
// it anyway costs an operator nothing next to missing the one that never will.
func notStopped(comps []namedComponent, dones []chan struct{}) []string {
	stuck := make([]string, 0, len(comps))
	for i := range dones {
		select {
		case <-dones[i]:
		default:
			stuck = append(stuck, comps[i].name)
		}
	}
	return stuck
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
// cancelling each and waiting for it to stop before moving to the next — under one budget shared by
// the whole sequence. It returns the first non-nil error: a component's failure first, then
// ErrDrainBudgetExceeded if the budget ran out, then nil on a clean shutdown.
//
// budget must exceed what a single component may legitimately spend stopping (cfg.ShutdownTimeout),
// since the drain is sequential: cfg.DrainBudget is the value services pass, and config refuses one
// at or under the per-component timeout.
func (o *Ordered) Run(ctx context.Context, logger *slog.Logger, budget time.Duration) error {
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

	stuck := o.drain(cancels, dones, budget, logger)

	if runErr != nil {
		return runErr
	}
	select {
	case err := <-errCh:
		return err
	default:
	}
	if stuck != "" {
		return fmt.Errorf("%w after %s: %s never stopped", ErrDrainBudgetExceeded, budget, stuck)
	}
	return nil
}

// drain stops the components in reverse registration order — cancelling each and waiting for it before
// the next — under one budget shared by the whole sequence. It returns the name of the component the
// budget expired on, or "" if they all stopped in time.
//
// The budget releases the REST of the sequence, not just the wait that overran: draining one at a time
// means a component that hangs would otherwise keep the ones registered before it from ever being
// cancelled — they would still be serving traffic when the kubelet SIGKILLs the pod. Only the component
// we were waiting on is named: those behind it were never given their turn, so calling them stuck would
// accuse the innocent.
func (o *Ordered) drain(cancels []context.CancelFunc, dones []chan struct{}, budget time.Duration, logger *slog.Logger) string {
	deadline, stop := drainDeadline(budget)
	defer stop()

	for i := len(o.comps) - 1; i >= 0; i-- {
		cancels[i]()
		select {
		case <-dones[i]:
		case <-deadline:
			// Both arms can be ready at once — select would then blame a component that just stopped.
			// Confirm before accusing; the budget is spent either way, so the next iteration lands
			// here immediately and the sequence still gets released.
			select {
			case <-dones[i]:
				continue
			default:
			}
			for j := i; j >= 0; j-- {
				cancels[j]()
			}
			logger.Error("drain budget exceeded, abandoning components", "budget", budget,
				"blocked_on", o.comps[i].name, "not_awaited", i)
			return o.comps[i].name
		}
	}
	return ""
}
