package routing

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/routing/script"
)

// ActiveScriptLister lists every active routing script. *postgres.RoutingScriptRepo satisfies it.
type ActiveScriptLister interface {
	ListActive(ctx context.Context) ([]script.Script, error)
}

// FailureMeter counts script fallbacks by runtime language and bounded reason. It never receives the
// error text or message body — only the two bounded labels.
type FailureMeter interface {
	Inc(language, reason string)
}

// ScriptSnapshot is an immutable set of compiled routing-script runtimes, indexed by scope. Nothing
// mutates it after build; hot reload swaps the whole snapshot (config-sync). Safe for concurrent reads.
type ScriptSnapshot struct {
	byScope map[string]compiledScript
}

type compiledScript struct {
	runtime  script.Resolver
	language string
}

func scopeKey(scope script.Scope, scopeID uuid.UUID) string {
	return string(scope) + "|" + scopeID.String()
}

// BuildScriptSnapshot compiles every active script into its runtime, indexed by (scope, scope_id). A
// script that fails to compile is skipped (logged), so one bad script never blocks routing for others.
func BuildScriptSnapshot(ctx context.Context, lister ActiveScriptLister, logger *slog.Logger) (*ScriptSnapshot, error) {
	scripts, err := lister.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("routing: load active scripts: %w", err)
	}
	byScope := make(map[string]compiledScript, len(scripts))
	for _, s := range scripts {
		rt, cerr := script.NewResolver(s)
		if cerr != nil {
			if logger != nil {
				logger.Warn("routing: skipping uncompilable active script", "script_id", s.ID, "scope", s.Scope, "err", cerr)
			}
			continue
		}
		id := uuid.Nil
		if s.ScopeID != nil {
			id = *s.ScopeID
		}
		byScope[scopeKey(s.Scope, id)] = compiledScript{runtime: rt, language: string(s.Language)}
	}
	return &ScriptSnapshot{byScope: byScope}, nil
}

// forScope returns the runtime that applies to (accountID, customerID), walking account → customer →
// platform (first active wins), or ok=false when no script applies.
func (s *ScriptSnapshot) forScope(accountID, customerID uuid.UUID) (compiledScript, bool) {
	if c, ok := s.byScope[scopeKey(script.ScopeAccount, accountID)]; ok {
		return c, true
	}
	if c, ok := s.byScope[scopeKey(script.ScopeCustomer, customerID)]; ok {
		return c, true
	}
	if c, ok := s.byScope[scopeKey(script.ScopePlatform, uuid.Nil)]; ok {
		return c, true
	}
	return compiledScript{}, false
}

// ScriptResolver runs the scope-resolved routing script for a request — the L1 stage between the exact
// short-cut (L0) and the declarative resolver (L2). A null result or ANY runtime error falls back to
// declarative resolution (never drops a message), logging and metering the reason. The snapshot is
// held behind an atomic pointer so config-sync can hot-swap it lock-free.
type ScriptResolver struct {
	current atomic.Pointer[ScriptSnapshot]
	logger  *slog.Logger
	meter   FailureMeter
}

// NewScriptResolver serves the given snapshot. logger/meter may be nil.
func NewScriptResolver(snap *ScriptSnapshot, logger *slog.Logger, meter FailureMeter) *ScriptResolver {
	r := &ScriptResolver{logger: logger, meter: meter}
	r.current.Store(snap)
	return r
}

// Swap atomically replaces the served script snapshot (hot reload).
func (r *ScriptResolver) Swap(snap *ScriptSnapshot) { r.current.Store(snap) }

// resolve returns the route id a scope-resolved script yields for req. ok is false when no script
// applies, the script returns null, or it errors — every one a declarative fallback. It never returns
// an error: a script failure is a fallback, not a pipeline error.
func (r *ScriptResolver) resolve(ctx context.Context, req pipeline.RouteRequest) (uuid.UUID, bool) {
	cs, ok := r.current.Load().forScope(req.AccountID, req.CustomerID)
	if !ok {
		return uuid.Nil, false // no active script for this scope
	}
	// The script sees routing metadata only — no body (invariant a).
	routeID, err := cs.runtime.Resolve(ctx, script.Message{
		From: req.From, To: req.Dest, Segments: req.Segments, ReceivedAtMs: req.ReceivedAtMs,
	})
	if err != nil {
		reason := script.Reason(err)
		if r.logger != nil {
			// Bounded fields only: the error text (which could echo script content) is never a field.
			r.logger.WarnContext(ctx, "routing script failed; declarative fallback", "runtime", cs.language, "reason", reason)
		}
		if r.meter != nil {
			r.meter.Inc(cs.language, reason)
		}
		return uuid.Nil, false
	}
	if routeID == nil {
		return uuid.Nil, false // null → declarative fallback
	}
	return *routeID, true
}
