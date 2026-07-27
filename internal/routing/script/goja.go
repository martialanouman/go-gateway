package script

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/google/uuid"
)

// jsMaxCallStack bounds recursion depth (a net alongside the wall-clock budget). A routing script has
// no legitimate reason to recurse deeply.
const jsMaxCallStack = 128

// errBudget is the value handed to vm.Interrupt when the wall-clock budget expires. It never escapes:
// classify maps the resulting *goja.InterruptedError to ErrScriptTimeout.
var errBudget = errors.New("script: wall-clock budget")

// sandboxPrelude hardens each VM before the operator program runs. It (1) blocks dynamic code
// generation — stripping the global Function is not enough, since the Function constructor is reachable
// via [].constructor.constructor, so it neutralizes .constructor on every function-constructor
// prototype — and (2) caps the string-synthesizing builtins that a native call could use to allocate
// unbounded memory past the wall-clock guard. Residual heavy-native vectors (huge Array allocation,
// catastrophic RegExp backtracking) are not fully bounded by mainline goja and rely on the pod memory
// limit (GOMEMLIMIT / container) as the net — tracked as a known limitation.
const sandboxPrelude = `
(function () {
  'use strict';
  var MAX = 65536;
  function blocked() { throw new Error('dynamic code generation is disabled'); }
  var protos = [Function.prototype];
  try { protos.push((function*(){}).constructor.prototype); } catch (e) {}
  try { protos.push((async function(){}).constructor.prototype); } catch (e) {}
  protos.forEach(function (p) {
    Object.defineProperty(p, 'constructor', { value: blocked, writable: false, configurable: false });
  });
  var repeat = String.prototype.repeat;
  Object.defineProperty(String.prototype, 'repeat', { writable: false, configurable: false, value: function (n) {
    if (n > MAX) { throw new Error('string too large'); }
    return repeat.call(this, n);
  }});
  ['padStart', 'padEnd'].forEach(function (m) {
    var orig = String.prototype[m];
    Object.defineProperty(String.prototype, m, { writable: false, configurable: false, value: function (len) {
      if (len > MAX) { throw new Error('string too large'); }
      return orig.apply(this, arguments);
    }});
  });
})();
`

// preludeProg is the compiled sandbox prelude, evaluated once per fresh VM.
var preludeProg = mustCompilePrelude()

func mustCompilePrelude() *goja.Program {
	p, err := goja.Compile("sandbox-prelude", sandboxPrelude, true)
	if err != nil {
		panic("script: compile sandbox prelude: " + err.Error())
	}
	return p
}

// JSRuntime runs one compiled JavaScript routing script (one checksum) on a pool of goja VMs. The
// compiled *goja.Program is shared (thread-safe); each *goja.Runtime is not thread-safe, so VMs are
// pooled and never shared across concurrent executions. The wall-clock interrupt (timeout_ms, ≤20ms)
// is the PRIMARY guard: mainline goja has no per-instruction counter, so a deterministic instruction
// ceiling is a Lua-only capability (see ErrInstructionLimit). SetMaxCallStackSize nets recursion, and
// sandboxPrelude blocks dynamic code generation and caps the cheap string-allocation vectors; residual
// heavy-native allocation (huge Array, catastrophic RegExp) relies on the pod memory limit as the net.
type JSRuntime struct {
	prog   *goja.Program
	budget time.Duration
	pool   sync.Pool
}

type vmSlot struct {
	vm      *goja.Runtime
	resolve goja.Callable
}

// NewJSRuntime compiles a JS routing script once. A source that does not compile is ErrScriptFailed.
// The runtime is safe for concurrent Resolve calls.
func NewJSRuntime(s Script) (*JSRuntime, error) {
	prog, err := goja.Compile(s.Name, s.Source, true)
	if err != nil {
		return nil, fmt.Errorf("%w: compile: %v", ErrScriptFailed, err)
	}
	budget := time.Duration(s.TimeoutMs) * time.Millisecond
	if budget <= 0 {
		budget = 2 * time.Millisecond
	}
	return &JSRuntime{prog: prog, budget: budget}, nil
}

// Resolve runs resolveRoute(message) and returns the route id it yields, nil for a null/undefined
// result (no route → the caller falls back to declarative resolution), or a typed error. A VM is
// discarded (not pooled) after any execution error so a half-run or interrupted VM is never reused.
func (r *JSRuntime) Resolve(ctx context.Context, msg Message) (*uuid.UUID, error) {
	slot, err := r.get(ctx)
	if err != nil {
		return nil, err
	}
	v, execErr := r.runGuarded(ctx, slot.vm, func() (goja.Value, error) {
		return slot.resolve(goja.Undefined(), slot.vm.ToValue(msg))
	})
	if execErr != nil {
		return nil, classify(execErr) // slot dropped for GC — never pooled after an error
	}
	// The VM executed cleanly; parse while we still exclusively own it, then return it to the pool.
	rid, perr := parseRouteID(v)
	r.pool.Put(slot)
	return rid, perr
}

// get returns a pooled VM or builds a fresh one. A pooled VM has its interrupt flag cleared defensively
// so reuse never inherits a stale interrupt.
func (r *JSRuntime) get(ctx context.Context) (*vmSlot, error) {
	if s, ok := r.pool.Get().(*vmSlot); ok {
		s.vm.ClearInterrupt()
		return s, nil
	}
	return r.newSlot(ctx)
}

// newSlot builds a sandboxed VM and evaluates the program once to define resolveRoute (top-level code
// runs under the same budget, since it too can loop). It caches the exported function.
func (r *JSRuntime) newSlot(ctx context.Context) (*vmSlot, error) {
	vm := goja.New()
	vm.SetMaxCallStackSize(jsMaxCallStack)
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	// Harden BEFORE stripping globals: the prelude needs the real Function/String builtins to neutralize
	// the constructor escape and cap the string builtins.
	if _, err := vm.RunProgram(preludeProg); err != nil {
		return nil, fmt.Errorf("%w: sandbox prelude: %v", ErrScriptFailed, err)
	}
	sandbox(vm)

	if _, err := r.runGuarded(ctx, vm, func() (goja.Value, error) { return vm.RunProgram(r.prog) }); err != nil {
		return nil, classify(err)
	}
	fn, ok := goja.AssertFunction(vm.Get("resolveRoute"))
	if !ok {
		return nil, fmt.Errorf("%w: script does not define a resolveRoute(message) function", ErrScriptFailed)
	}
	return &vmSlot{vm: vm, resolve: fn}, nil
}

// runGuarded runs call under a wall-clock watchdog. The critical ordering: after call returns we close
// stop, JOIN the watchdog (so no goroutine can Interrupt this VM again), and only then ClearInterrupt
// — this closes the race where a watchdog firing just after a fast script would otherwise leave a
// pending interrupt that kills the NEXT execution on a reused VM.
func (r *JSRuntime) runGuarded(ctx context.Context, vm *goja.Runtime, call func() (goja.Value, error)) (goja.Value, error) {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTimer(r.budget)
		defer t.Stop()
		select {
		case <-t.C:
			vm.Interrupt(errBudget)
		case <-ctx.Done():
			vm.Interrupt(ctx.Err())
		case <-stop:
		}
	}()

	// Deferred so the watchdog is always joined and the interrupt always cleared, even if call panics
	// (a future host binding could): no watchdog goroutine ever outlives this call.
	defer func() {
		close(stop)
		<-done
		vm.ClearInterrupt()
	}()
	return call()
}

// sandbox strips the host capabilities and non-determinism a routing script must not have: no real
// clock (Date), no dynamic code generation (eval/Function), and a fixed random source so Math.random
// cannot leak entropy or vary a routing decision non-reproducibly. Bare goja exposes no I/O, network,
// module system, or timers to begin with (those live in goja_nodejs, which is never imported).
func sandbox(vm *goja.Runtime) {
	_ = vm.Set("Date", goja.Undefined())
	_ = vm.Set("eval", goja.Undefined())
	_ = vm.Set("Function", goja.Undefined())
	vm.SetRandSource(func() float64 { return 0 })
}

// parseRouteID converts the script's return value to a route id. null/undefined means "no route" (nil,
// nil → declarative fallback); a UUID string is the route; anything else is ErrBadResult.
func parseRouteID(v goja.Value) (*uuid.UUID, error) {
	x := v.Export()
	if x == nil {
		return nil, nil
	}
	s, ok := x.(string)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", ErrBadResult, x)
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrBadResult, s)
	}
	return &id, nil
}

// classify maps a goja execution error to the shared taxonomy. An interrupt (budget or ctx) is a
// wall-clock timeout; a stack overflow or a thrown/runtime error is a script failure.
func classify(err error) error {
	var ie *goja.InterruptedError
	if errors.As(err, &ie) {
		// A context cancellation (request aborted, pod shutdown) is not a script timeout — surface it
		// distinctly so it never pollutes the timeout metric or reads as a deterministic budget trip.
		if cerr, ok := ie.Value().(error); ok && (errors.Is(cerr, context.Canceled) || errors.Is(cerr, context.DeadlineExceeded)) {
			return cerr
		}
		return fmt.Errorf("%w: %v", ErrScriptTimeout, ie.Value())
	}
	return fmt.Errorf("%w: %v", ErrScriptFailed, err)
}
