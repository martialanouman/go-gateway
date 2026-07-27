package script_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/routing/script"
)

func jsScript(src string) script.Script {
	return script.Script{Name: "t", Language: script.LanguageJS, Source: src, TimeoutMs: 50}
}

const jsRoute = "550e8400-e29b-41d4-a716-446655440000"

// TestJSResolveReturnsRoute: a script returning a UUID string routes to it.
func TestJSResolveReturnsRoute(t *testing.T) {
	rt, err := script.NewJSRuntime(jsScript(`function resolveRoute(m){ return m.to.startsWith("2250") ? "` + jsRoute + `" : null }`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := rt.Resolve(context.Background(), script.Message{To: "2250700000001"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got == nil || got.String() != jsRoute {
		t.Errorf("route = %v, want %s", got, jsRoute)
	}
}

// TestJSResolveNullFallsBack: a script returning null yields (nil, nil) — the caller falls back.
func TestJSResolveNullFallsBack(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ return null }`))
	got, err := rt.Resolve(context.Background(), script.Message{To: "12025550123"})
	if err != nil || got != nil {
		t.Errorf("resolve(null) = (%v, %v), want (nil, nil)", got, err)
	}
}

// TestJSInfiniteLoopTimesOut: a runaway script is cut by the wall-clock budget and returns a typed
// ErrScriptTimeout — the pod is protected.
func TestJSInfiniteLoopTimesOut(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ for(;;){} }`))
	_, err := rt.Resolve(context.Background(), script.Message{To: "2250700000001"})
	if !errors.Is(err, script.ErrScriptTimeout) {
		t.Fatalf("runaway script error = %v, want ErrScriptTimeout", err)
	}
}

// TestJSInterruptDoesNotLeakToNextExecution: after a timeout, the SAME runtime resolves a fast script
// correctly — a late-firing watchdog must not poison a subsequent execution (the joined-then-cleared
// interrupt race).
func TestJSInterruptDoesNotLeakToNextExecution(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ if(m.to==="loop"){for(;;){}} return "` + jsRoute + `" }`))

	if _, err := rt.Resolve(context.Background(), script.Message{To: "loop"}); !errors.Is(err, script.ErrScriptTimeout) {
		t.Fatalf("first (loop) = %v, want ErrScriptTimeout", err)
	}
	// Run many fast executions: none may spuriously time out from a leaked interrupt.
	for i := 0; i < 50; i++ {
		got, err := rt.Resolve(context.Background(), script.Message{To: "ok"})
		if err != nil || got == nil {
			t.Fatalf("fast run %d after a timeout = (%v, %v), want the route", i, got, err)
		}
	}
}

// TestJSBadResultIsTyped: a non-UUID return is ErrBadResult (treated as no usable route).
func TestJSBadResultIsTyped(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ return 42 }`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrBadResult) {
		t.Errorf("numeric result error = %v, want ErrBadResult", err)
	}

	rt2, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ return "not-a-uuid" }`))
	if _, err := rt2.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrBadResult) {
		t.Errorf("non-uuid string error = %v, want ErrBadResult", err)
	}
}

// TestJSThrowIsScriptFailed: a thrown error is ErrScriptFailed.
func TestJSThrowIsScriptFailed(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ throw new Error("boom") }`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("throw error = %v, want ErrScriptFailed", err)
	}
}

// TestJSCompileErrorIsTyped: a source that does not compile fails NewJSRuntime with ErrScriptFailed.
func TestJSCompileErrorIsTyped(t *testing.T) {
	if _, err := script.NewJSRuntime(jsScript(`function resolveRoute( { syntax`)); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("compile error = %v, want ErrScriptFailed", err)
	}
}

// TestJSMissingFunctionIsTyped: a script without resolveRoute is ErrScriptFailed.
func TestJSMissingFunctionIsTyped(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`var x = 1`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("missing resolveRoute error = %v, want ErrScriptFailed", err)
	}
}

// TestJSNoDateOrEval: the stripped globals are undefined (sandbox).
func TestJSNoDateOrEval(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ return (typeof Date==="undefined" && typeof eval==="undefined") ? "` + jsRoute + `" : null }`))
	got, err := rt.Resolve(context.Background(), script.Message{})
	if err != nil || got == nil {
		t.Errorf("sandbox check = (%v, %v), want the route (Date/eval stripped)", got, err)
	}
}

// TestJSFunctionConstructorEscapeBlocked: dynamic code generation via the constructor chain
// ([].constructor.constructor("…")) is neutralized — the sandbox is not bypassable through it.
func TestJSFunctionConstructorEscapeBlocked(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ return [].constructor.constructor("return 1")() }`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("constructor-chain code-gen error = %v, want ErrScriptFailed (blocked)", err)
	}
}

// TestJSStringRepeatCapped: a huge string synthesis (a native builtin the wall clock cannot preempt)
// is rejected by the length cap rather than allowed to OOM the pod.
func TestJSStringRepeatCapped(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ return "a".repeat(1e9) }`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("huge repeat error = %v, want ErrScriptFailed (length cap)", err)
	}
}

// TestJSContextCancellationIsDistinct: a cancelled context surfaces as the context error, not as a
// script timeout — the timeout metric stays honest.
func TestJSContextCancellationIsDistinct(t *testing.T) {
	rt, _ := script.NewJSRuntime(jsScript(`function resolveRoute(m){ for(;;){} }`))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the watchdog interrupts with ctx.Err immediately

	_, err := rt.Resolve(ctx, script.Message{})
	if errors.Is(err, script.ErrScriptTimeout) || !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled-context error = %v, want context.Canceled (not ErrScriptTimeout)", err)
	}
}

// TestJSConcurrentReuse: many concurrent resolutions share the VM pool safely (run under -race).
func TestJSConcurrentReuse(t *testing.T) {
	// A generous budget: under -race + CI load, 32 goroutines can push a trivial script's wall-clock
	// past a few ms — instrumentation overhead, not a real timeout — so it must not trip here.
	rt, _ := script.NewJSRuntime(script.Script{Name: "t", Language: script.LanguageJS, TimeoutMs: 500,
		Source: `function resolveRoute(m){ return m.to==="hit" ? "` + jsRoute + `" : null }`})
	want := uuid.MustParse(jsRoute)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			to := "hit"
			if i%2 == 0 {
				to = "miss"
			}
			got, err := rt.Resolve(context.Background(), script.Message{To: to})
			if err != nil {
				t.Errorf("concurrent resolve: %v", err)
				return
			}
			if to == "hit" && (got == nil || *got != want) {
				t.Errorf("hit routed to %v, want %s", got, want)
			}
			if to == "miss" && got != nil {
				t.Errorf("miss routed to %v, want nil", got)
			}
		}(i)
	}
	wg.Wait()
}
