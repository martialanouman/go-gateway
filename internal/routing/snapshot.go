// Package routing resolves a destination to a connector for the MT pipeline. M2 provides the
// declarative static resolver only: an immutable snapshot of the active static routes, loaded once
// at startup, matched by numeric destination prefix. Routing scripts, exact-number routes,
// weighted/failover distribution and hot reload (config-sync) arrive in later milestones.
package routing

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// RouteLister loads the declarative routes. *postgres.RouteRepo satisfies it structurally.
type RouteLister interface {
	List(ctx context.Context) ([]cp.Route, error)
}

// SnapshotResolver resolves against an immutable set of compiled routes. It is safe for concurrent
// reads (nothing mutates after LoadSnapshot), so the router reads it lock-free per message.
type SnapshotResolver struct {
	routes []compiledRoute
}

type compiledRoute struct {
	prefix      string // digits only ("" = catch-all)
	connectorID uuid.UUID
	routeID     uuid.UUID
	priority    int
}

// LoadSnapshot reads the active static routes once and compiles them for prefix matching. It skips
// non-static strategies and routes without a target connector (those belong to later milestones).
func LoadSnapshot(ctx context.Context, lister RouteLister) (*SnapshotResolver, error) {
	all, err := lister.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("routing: load routes: %w", err)
	}

	var compiled []compiledRoute
	for _, r := range all {
		if r.Status != cp.RouteActive || r.DistributionStrategy != cp.DistributionStatic || r.TargetConnectorID == nil {
			continue
		}
		prefix := ""
		if r.MatchDestPattern != nil {
			prefix = digits(*r.MatchDestPattern)
		}
		compiled = append(compiled, compiledRoute{
			prefix:      prefix,
			connectorID: *r.TargetConnectorID,
			routeID:     r.ID,
			priority:    r.Priority,
		})
	}

	// Most specific first: a longer prefix outranks a shorter one; equal lengths tie-break on the
	// lower priority number (evaluated first, per the schema).
	sort.SliceStable(compiled, func(i, j int) bool {
		if len(compiled[i].prefix) != len(compiled[j].prefix) {
			return len(compiled[i].prefix) > len(compiled[j].prefix)
		}
		return compiled[i].priority < compiled[j].priority
	})

	return &SnapshotResolver{routes: compiled}, nil
}

// Resolve returns the connector for dest, or ErrNoRoute when nothing matches. dest is the E.164 form
// ("+225…"); matching is on its digits, so a "225" prefix matches "+225…".
func (s *SnapshotResolver) Resolve(_ context.Context, dest string) (pipeline.Route, error) {
	d := digits(dest)
	for _, r := range s.routes {
		if r.prefix == "" || strings.HasPrefix(d, r.prefix) {
			routeID := r.routeID
			return pipeline.Route{ConnectorID: r.connectorID, RouteID: &routeID}, nil
		}
	}
	return pipeline.Route{}, errs.ErrNoRoute
}

// digits keeps only 0-9, dropping the leading "+" and any separators.
func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
