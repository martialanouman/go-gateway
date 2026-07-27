package script

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Resolver is the unified routing-script contract both runtimes satisfy (goja JSRuntime, gopher-lua
// LuaRuntime): run resolveRoute(message) and return the route id it yields, nil for "no route" (the
// caller falls back to declarative resolution), or a typed error (ErrScriptTimeout / ErrScriptFailed /
// ErrBadResult) — on which the caller also falls back, logging and metering the reason.
type Resolver interface {
	Resolve(ctx context.Context, msg Message) (*uuid.UUID, error)
}

// NewResolver compiles a Script into the runtime for its language. A compile failure is ErrScriptFailed.
func NewResolver(s Script) (Resolver, error) {
	switch s.Language {
	case LanguageJS:
		return NewJSRuntime(s)
	case LanguageLua:
		return NewLuaRuntime(s)
	default:
		return nil, fmt.Errorf("%w: unknown language %q", ErrScriptFailed, s.Language)
	}
}

// Reason maps a runtime error to a bounded metric label (never the error text, which could carry
// script-controlled content). Unknown errors bucket as "runtime_error".
func Reason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrScriptTimeout):
		return "timeout"
	case errors.Is(err, ErrInstructionLimit):
		return "instruction_limit"
	case errors.Is(err, ErrBadResult):
		return "bad_result"
	default:
		return "runtime_error"
	}
}
