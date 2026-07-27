package script

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"github.com/google/uuid"
)

// luaCallStackSize / luaRegistrySize bound a pooled state's memory footprint modestly. A routing
// script is tiny; these are far above any legitimate need and a low ceiling on a runaway script.
const (
	luaCallStackSize = 128
	luaRegistrySize  = 1024 * 8
)

// luaMaxSynthLen caps the length of a string a single native builtin (string.rep) may synthesize. The
// SetContext deadline checks between bytecode instructions but cannot preempt one native call, so
// string.rep("a", 2e9) would allocate ~2GB uninterrupted and OOM the pod; the cap neutralizes it.
const luaMaxSynthLen = 1 << 16

// LuaRuntime runs one compiled Lua routing script (one checksum) on a pool of *lua.LState. The
// compiled *lua.FunctionProto is shared across states (it is read-only); each LState is single-threaded
// so states are pooled and never shared concurrently. The wall-clock guard is SetContext(deadline) —
// gopher-lua has no per-instruction hook (so the deterministic instruction ceiling of ErrInstructionLimit
// is not available) and its SetMx memory cap os.Exit()s the whole process, so it is deliberately unused.
// openSafeLibs caps string.rep (an OOM vector the deadline cannot preempt); residual native allocation
// relies on the pod memory limit as the net.
type LuaRuntime struct {
	proto  *lua.FunctionProto
	budget time.Duration
	pool   sync.Pool
}

// NewLuaRuntime parses and compiles a Lua routing script once. A source that will not parse/compile is
// ErrScriptFailed. The runtime is safe for concurrent Resolve calls.
func NewLuaRuntime(s Script) (*LuaRuntime, error) {
	chunk, err := parse.Parse(strings.NewReader(s.Source), s.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: parse: %v", ErrScriptFailed, err)
	}
	proto, err := lua.Compile(chunk, s.Name)
	if err != nil {
		return nil, fmt.Errorf("%w: compile: %v", ErrScriptFailed, err)
	}
	budget := time.Duration(s.TimeoutMs) * time.Millisecond
	if budget <= 0 {
		budget = 2 * time.Millisecond
	}
	return &LuaRuntime{proto: proto, budget: budget}, nil
}

// Resolve runs resolveRoute(message) and returns the route id it yields, nil for a nil result (no route
// → declarative fallback), or a typed error. A state is discarded (Close, not pooled) after any error
// so a dirty stack or an interrupted state is never reused.
func (r *LuaRuntime) Resolve(ctx context.Context, msg Message) (*uuid.UUID, error) {
	ls, err := r.get(ctx)
	if err != nil {
		return nil, err
	}

	tctx, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	ls.SetContext(tctx)

	fn := ls.GetGlobal("resolveRoute")
	if fn.Type() != lua.LTFunction {
		ls.RemoveContext()
		ls.Close()
		return nil, fmt.Errorf("%w: script does not define a resolveRoute(message) function", ErrScriptFailed)
	}

	callErr := ls.CallByParam(lua.P{Fn: fn, NRet: 1, Protect: true}, messageTable(ls, msg))

	var ret lua.LValue
	if callErr == nil {
		ret = ls.Get(-1)
		ls.Pop(1)
	}
	ls.RemoveContext()

	if callErr != nil {
		ls.Close() // discarded — never pooled after an error
		// A parent cancellation/deadline is the caller's, not a script timeout; surface it distinctly.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if isContextExpiry(callErr) {
			return nil, fmt.Errorf("%w: %v", ErrScriptTimeout, callErr)
		}
		return nil, fmt.Errorf("%w: %v", ErrScriptFailed, callErr)
	}

	rid, perr := parseLuaResult(ret)
	r.pool.Put(ls) // clean execution → reusable
	return rid, perr
}

// get returns a pooled state or builds a fresh sandboxed one.
func (r *LuaRuntime) get(ctx context.Context) (*lua.LState, error) {
	if ls, ok := r.pool.Get().(*lua.LState); ok {
		return ls, nil
	}
	return r.newState(ctx)
}

// newState builds a sandboxed LState and runs the program once to define resolveRoute (top-level runs
// under the same wall-clock budget, since it too can loop).
func (r *LuaRuntime) newState(ctx context.Context) (*lua.LState, error) {
	ls := lua.NewState(lua.Options{
		SkipOpenLibs:  true,
		CallStackSize: luaCallStackSize,
		RegistrySize:  luaRegistrySize,
	})
	openSafeLibs(ls)

	ls.Push(ls.NewFunctionFromProto(r.proto))
	// The one-time top-level init runs under the caller's ctx plus the wall-clock budget: if that caller
	// cancels, init fails and the state is discarded (not pooled), and the next caller retries with its own.
	tctx, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	ls.SetContext(tctx)
	err := ls.PCall(0, 0, nil)
	ls.RemoveContext()
	if err != nil {
		ls.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err() // caller cancelled during init, not a script timeout
		}
		if isContextExpiry(err) {
			return nil, fmt.Errorf("%w: %v", ErrScriptTimeout, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrScriptFailed, err)
	}
	return ls, nil
}

// openSafeLibs opens only the pure, deterministic standard libraries (base, table, string, math) and
// strips the base functions that touch the filesystem, load code dynamically, or expose the host: no
// os, io, package/require, dofile/loadfile/load, collectgarbage, or print. The sandbox is an allow-list.
func openSafeLibs(ls *lua.LState) {
	for _, lib := range []struct {
		name string
		open lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
	} {
		ls.Push(ls.NewFunction(lib.open))
		ls.Push(lua.LString(lib.name))
		ls.Call(1, 0)
	}
	for _, g := range []string{"dofile", "loadfile", "load", "loadstring", "collectgarbage", "print", "require", "module", "newproxy"} {
		ls.SetGlobal(g, lua.LNil)
	}
	// Cap string.rep: it is a native builtin the wall-clock deadline cannot preempt mid-call, so an
	// uncapped rep is an OOM vector. Overriding the string table covers both string.rep(s,n) and s:rep(n)
	// (the string metatable indexes into this table). Residual native allocation (string.format width,
	// string.char with a huge count) relies on the pod memory limit as the net — a known limitation.
	if strTbl, ok := ls.GetGlobal("string").(*lua.LTable); ok {
		strTbl.RawSetString("rep", ls.NewFunction(cappedRep))
	}
}

// cappedRep is string.rep with a result-length ceiling, raising a Lua error past luaMaxSynthLen.
func cappedRep(ls *lua.LState) int {
	s := ls.CheckString(1)
	n := ls.CheckInt(2)
	if n > 0 && len(s) > 0 && n > luaMaxSynthLen/len(s) {
		ls.RaiseError("string.rep: result too large")
		return 0
	}
	if n < 0 {
		n = 0
	}
	ls.Push(lua.LString(strings.Repeat(s, n)))
	return 1
}

// messageTable exposes the routing metadata to the script as a Lua table (msg.to, msg.from, …). No
// body is present (invariant a).
func messageTable(ls *lua.LState, msg Message) *lua.LTable {
	t := ls.NewTable()
	t.RawSetString("from", lua.LString(msg.From))
	t.RawSetString("to", lua.LString(msg.To))
	t.RawSetString("segments", lua.LNumber(msg.Segments))
	t.RawSetString("received_at_ms", lua.LNumber(msg.ReceivedAtMs))
	return t
}

// parseLuaResult converts the script's return value to a route id. nil means "no route" (nil, nil →
// declarative fallback); a UUID string is the route; anything else is ErrBadResult.
func parseLuaResult(v lua.LValue) (*uuid.UUID, error) {
	if v == nil || v.Type() == lua.LTNil {
		return nil, nil
	}
	s, ok := v.(lua.LString)
	if !ok {
		return nil, fmt.Errorf("%w: got %s", ErrBadResult, v.Type())
	}
	id, err := uuid.Parse(string(s))
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrBadResult, string(s))
	}
	return &id, nil
}

// isContextExpiry reports whether a gopher-lua error is a SetContext deadline/cancellation. gopher-lua
// raises it as a Lua error carrying ctx.Err().Error(), so the signal is the message text.
func isContextExpiry(err error) bool {
	m := err.Error()
	return strings.Contains(m, context.DeadlineExceeded.Error()) || strings.Contains(m, context.Canceled.Error())
}
