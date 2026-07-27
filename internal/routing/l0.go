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

// L0Resolver puts the exact-number short-cut (L0) in front of the declarative snapshot (L2). A
// configured MSISDN — a ported number / MNP override — routes straight to its target, skipping *route
// resolution only*; the compliance stages run earlier in the pipeline and are never bypassed
// (invariant b). Every other number, and any override whose target is no longer resolvable, falls
// through to the declarative resolver unchanged (spec §6.1: fall back, don't dead-letter).
type L0Resolver struct {
	exact       ExactResolver
	declarative *SnapshotResolver
}

// NewL0Resolver wraps the declarative resolver with the L0 exact-number short-cut. It satisfies
// pipeline.Resolver, so the router passes it wherever the declarative resolver went.
func NewL0Resolver(ex ExactResolver, declarative *SnapshotResolver) *L0Resolver {
	return &L0Resolver{exact: ex, declarative: declarative}
}

// Resolve applies the L0 short-cut, then falls back to declarative resolution. A transient exact-lookup
// fault is surfaced as an error rather than silently mis-routing: the caller retries the message.
func (r *L0Resolver) Resolve(ctx context.Context, dest string) (pipeline.Route, error) {
	target, ok, err := r.exact.Resolve(ctx, dest)
	if err != nil {
		return pipeline.Route{}, err
	}
	if ok {
		if route, matched := r.declarative.routeForTarget(target); matched {
			return route, nil
		}
		// The override points at a route that is no longer in the snapshot (deleted/disabled): fall
		// back to the normal chain rather than dropping the message (spec §6.1).
	}
	return r.declarative.Resolve(ctx, dest)
}
