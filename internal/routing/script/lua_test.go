package script_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/routing/script"
)

func luaScript(src string) script.Script {
	return script.Script{Name: "t", Language: script.LanguageLua, Source: src, TimeoutMs: 50}
}

const luaRoute = "550e8400-e29b-41d4-a716-446655440001"

// TestLuaResolveReturnsRoute: a script returning a UUID string routes to it.
func TestLuaResolveReturnsRoute(t *testing.T) {
	rt, err := script.NewLuaRuntime(luaScript(`
		function resolveRoute(m)
			if string.sub(m.to, 1, 4) == "2250" then return "` + luaRoute + `" end
			return nil
		end`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := rt.Resolve(context.Background(), script.Message{To: "2250700000001"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got == nil || got.String() != luaRoute {
		t.Errorf("route = %v, want %s", got, luaRoute)
	}
}

// TestLuaResolveNilFallsBack: a script returning nil yields (nil, nil) — the caller falls back.
func TestLuaResolveNilFallsBack(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) return nil end`))
	got, err := rt.Resolve(context.Background(), script.Message{To: "12025550123"})
	if err != nil || got != nil {
		t.Errorf("resolve(nil) = (%v, %v), want (nil, nil)", got, err)
	}
}

// TestLuaInfiniteLoopTimesOut: a runaway script is cut by the wall-clock budget (SetContext) and
// returns a typed ErrScriptTimeout.
func TestLuaInfiniteLoopTimesOut(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) while true do end end`))
	_, err := rt.Resolve(context.Background(), script.Message{To: "2250700000001"})
	if !errors.Is(err, script.ErrScriptTimeout) {
		t.Fatalf("runaway script error = %v, want ErrScriptTimeout", err)
	}
}

// TestLuaContextCancellationIsDistinct: a cancelled parent context surfaces as the context error, not
// a script timeout.
func TestLuaContextCancellationIsDistinct(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) while true do end end`))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rt.Resolve(ctx, script.Message{})
	if errors.Is(err, script.ErrScriptTimeout) || !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled-context error = %v, want context.Canceled", err)
	}
}

// TestLuaBadResultIsTyped: a non-UUID return is ErrBadResult.
func TestLuaBadResultIsTyped(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) return 42 end`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrBadResult) {
		t.Errorf("numeric result error = %v, want ErrBadResult", err)
	}
	rt2, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) return "nope" end`))
	if _, err := rt2.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrBadResult) {
		t.Errorf("non-uuid string error = %v, want ErrBadResult", err)
	}
}

// TestLuaRuntimeErrorIsScriptFailed: a Lua runtime error (calling a nil global) is ErrScriptFailed.
func TestLuaRuntimeErrorIsScriptFailed(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) return nope.field end`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("runtime error = %v, want ErrScriptFailed", err)
	}
}

// TestLuaCompileErrorIsTyped: a source that will not parse fails NewLuaRuntime with ErrScriptFailed.
func TestLuaCompileErrorIsTyped(t *testing.T) {
	if _, err := script.NewLuaRuntime(luaScript(`function resolveRoute(m return`)); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("compile error = %v, want ErrScriptFailed", err)
	}
}

// TestLuaMissingFunctionIsTyped: a script without resolveRoute is ErrScriptFailed.
func TestLuaMissingFunctionIsTyped(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`local x = 1`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("missing resolveRoute error = %v, want ErrScriptFailed", err)
	}
}

// TestLuaSandboxHasNoHostLibs: os/io and dynamic loaders are not exposed to the script.
func TestLuaSandboxHasNoHostLibs(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`
		function resolveRoute(m)
			if os == nil and io == nil and load == nil and dofile == nil then return "` + luaRoute + `" end
			return nil
		end`))
	got, err := rt.Resolve(context.Background(), script.Message{})
	if err != nil || got == nil {
		t.Errorf("sandbox check = (%v, %v), want the route (os/io/load stripped)", got, err)
	}
}

// TestLuaStringRepCapped: a huge string.rep (a native builtin the wall clock cannot preempt) is
// rejected by the length cap rather than allowed to OOM the pod.
func TestLuaStringRepCapped(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) return string.rep("a", 1e9) end`))
	if _, err := rt.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("huge string.rep error = %v, want ErrScriptFailed (length cap)", err)
	}
	// The method form :rep also goes through the capped string table.
	rt2, _ := script.NewLuaRuntime(luaScript(`function resolveRoute(m) return ("a"):rep(1e9) end`))
	if _, err := rt2.Resolve(context.Background(), script.Message{}); !errors.Is(err, script.ErrScriptFailed) {
		t.Errorf("huge :rep error = %v, want ErrScriptFailed (length cap)", err)
	}
}

// TestLuaNoCoroutineOrDebug: the coroutine and debug libraries are not exposed (allow-list sandbox).
func TestLuaNoCoroutineOrDebug(t *testing.T) {
	rt, _ := script.NewLuaRuntime(luaScript(`
		function resolveRoute(m)
			if coroutine == nil and debug == nil and package == nil then return "` + luaRoute + `" end
			return nil
		end`))
	got, err := rt.Resolve(context.Background(), script.Message{})
	if err != nil || got == nil {
		t.Errorf("coroutine/debug check = (%v, %v), want the route (libs absent)", got, err)
	}
}

// TestLuaConcurrentReuse: many concurrent resolutions share the state pool safely (run under -race).
func TestLuaConcurrentReuse(t *testing.T) {
	// A generous budget: 32 goroutines contending for the state pool under -race can make even a trivial
	// script's wall-clock exceed a few ms in a loaded CI — that is instrumentation overhead, not a real
	// timeout, so it must not trip here (production runs without -race under the schema-capped budget).
	rt, _ := script.NewLuaRuntime(script.Script{Name: "t", Language: script.LanguageLua, TimeoutMs: 500,
		Source: `function resolveRoute(m) if m.to == "hit" then return "` + luaRoute + `" end return nil end`})
	want := uuid.MustParse(luaRoute)

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
