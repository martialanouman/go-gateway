package script

import "errors"

// The routing-script runtime error taxonomy, shared by the JS (goja) and Lua (gopher-lua) runtimes so
// step-110 can treat any of them uniformly (it falls back to declarative routing on ANY error). The
// distinction is not for control flow — it is for the failure metric's `reason` label and for honest
// logs (a wall-clock trip must not be reported as a deterministic instruction-limit trip).
var (
	// ErrInstructionLimit is the deterministic instruction-ceiling trip. It is currently RESERVED and
	// emitted by no runtime: neither goja nor gopher-lua exposes a per-instruction hook, so both bound
	// execution by the wall clock (ErrScriptTimeout). It stays in the taxonomy for API stability and for
	// a future runtime that can count instructions deterministically.
	ErrInstructionLimit = errors.New("script: instruction ceiling exceeded")

	// ErrScriptTimeout is the wall-clock budget trip (timeout_ms), the guard that actually protects the
	// pod. Both runtimes can return it; for JS it is the primary guard.
	ErrScriptTimeout = errors.New("script: wall-clock budget exceeded")

	// ErrScriptFailed is any execution or compile failure: a thrown error, a runtime fault, a stack
	// overflow, or a program that would not compile.
	ErrScriptFailed = errors.New("script: execution failed")

	// ErrBadResult is a script that returned something other than a route id string or null (a wrong
	// type, a non-UUID string, undefined, a thenable). Treated as "no usable route" → declarative fallback.
	ErrBadResult = errors.New("script: result is not a route id")
)

// Message is the routing metadata a script sees. It carries NO message body — invariant (a): the body
// must never reach a place it could be logged, and a script log/throw could surface anything it can
// read. The runtime resolves a route from these fields alone. ReceivedAtMs is an epoch-ms timestamp so
// a script can do time-based routing deterministically (no real clock is exposed).
type Message struct {
	From         string `json:"from"`
	To           string `json:"to"`
	Segments     int    `json:"segments"`
	ReceivedAtMs int64  `json:"received_at_ms"`
}
