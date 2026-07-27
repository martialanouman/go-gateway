package routing

import (
	"context"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
)

// ExactResolver is the L0 exact-number lookup (internal/routing/exact). A hit short-circuits route
// resolution; ok==false means "no override, resolve normally". The interface lives here, consumer-side.
type ExactResolver interface {
	Resolve(ctx context.Context, msisdn string) (exact.Target, bool, error)
}

// L0Resolver chains the three resolution levels of §6.1: the exact-number short-cut (L0), then the
// routing script (L1), then the declarative resolver (L2). A configured MSISDN — a ported number / MNP
// override — routes straight to its target; otherwise a scope-resolved script may pick a route; every
// other number, and any script null/error or unresolvable target, falls through to the declarative
// resolver (spec §6.1: fall back, don't dead-letter). Only *route resolution* is short-cut here — the
// compliance stages run earlier in the pipeline and are never bypassed (invariant b).
type L0Resolver struct {
	exact       ExactResolver
	script      *ScriptResolver // nil when no scripts are configured
	declarative *SnapshotResolver
}

// NewL0Resolver chains the exact short-cut and (optionally) the script stage in front of the
// declarative resolver. A nil script resolver skips the L1 stage. It satisfies pipeline.Resolver.
func NewL0Resolver(ex ExactResolver, scripts *ScriptResolver, declarative *SnapshotResolver) *L0Resolver {
	return &L0Resolver{exact: ex, script: scripts, declarative: declarative}
}

// Resolve applies L0 (exact), then L1 (script), then L2 (declarative). A transient exact-lookup fault
// is surfaced as an error rather than silently mis-routing; a script null/error is an internal
// fallback (never a pipeline error).
func (r *L0Resolver) Resolve(ctx context.Context, req pipeline.RouteRequest) (pipeline.Route, error) {
	// L0 — exact number.
	target, ok, err := r.exact.Resolve(ctx, req.Dest)
	if err != nil {
		return pipeline.Route{}, err
	}
	if ok {
		if route, matched := r.declarative.routeForTarget(ctx, target, req.Dest); matched {
			return route, nil
		}
		// Override points at a route no longer in the snapshot: fall through (spec §6.1).
	}

	// L1 — routing script (scope-resolved). A picked route id is resolved to its connector via the
	// snapshot; a script route id absent from the snapshot falls through to declarative.
	if r.script != nil {
		if routeID, picked := r.script.resolve(ctx, req); picked {
			if route, matched := r.declarative.routeByID(ctx, routeID, req.Dest); matched {
				return route, nil
			}
		}
	}

	// L2 — declarative.
	return r.declarative.Resolve(ctx, req.Dest)
}
