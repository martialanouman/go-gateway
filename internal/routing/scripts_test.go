package routing_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/routing/script"
)

// fakeActiveScripts is an ActiveScriptLister returning a fixed set of active scripts.
type fakeActiveScripts struct{ scripts []script.Script }

func (f fakeActiveScripts) ListActive(context.Context) ([]script.Script, error) {
	return f.scripts, nil
}

// countingMeter records script failures by reason.
type countingMeter struct {
	mu       sync.Mutex
	byReason map[string]int
}

func newCountingMeter() *countingMeter { return &countingMeter{byReason: map[string]int{}} }
func (m *countingMeter) Inc(_ /*language*/, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byReason[reason]++
}
func (m *countingMeter) count(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byReason[reason]
}

func jsReturning(id uuid.UUID) string {
	return `function resolveRoute(m){ return "` + id.String() + `" }`
}

// declSnapshotWith builds a declarative snapshot whose routes map each id→connector, plus a catch-all
// (prefix 225) to declConn for the null/error fallback path.
func declSnapshotWith(t *testing.T, declRoute, declConn uuid.UUID, routes map[uuid.UUID]uuid.UUID) *routing.SnapshotResolver {
	t.Helper()
	list := make([]cp.Route, 0, 1+len(routes))
	list = append(list, cp.Route{ID: declRoute, Priority: 100, DistributionStrategy: cp.DistributionStatic,
		Status: cp.RouteActive, MatchDestPattern: ptr("225"), TargetConnectorID: &declConn})
	for routeID, conn := range routes {
		c := conn
		list = append(list, cp.Route{ID: routeID, Priority: 50, DistributionStrategy: cp.DistributionStatic,
			Status: cp.RouteActive, MatchDestPattern: ptr("999"), TargetConnectorID: &c})
	}
	snap, err := routing.LoadSnapshot(context.Background(), fakeLister{routes: list})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return snap
}

// TestScriptScopeResolution: the active script is chosen account → customer → platform (first wins),
// and its route id resolves to the route's connector.
func TestScriptScopeResolution(t *testing.T) {
	accountX, customerY := uuid.New(), uuid.New()
	routeAcct, connAcct := uuid.New(), uuid.New()
	routeCust, connCust := uuid.New(), uuid.New()
	routePlat, connPlat := uuid.New(), uuid.New()
	declRoute, declConn := uuid.New(), uuid.New()

	decl := declSnapshotWith(t, declRoute, declConn, map[uuid.UUID]uuid.UUID{
		routeAcct: connAcct, routeCust: connCust, routePlat: connPlat,
	})
	snap, err := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "a", Language: script.LanguageJS, Scope: script.ScopeAccount, ScopeID: &accountX, Source: jsReturning(routeAcct)},
		{Name: "c", Language: script.LanguageJS, Scope: script.ScopeCustomer, ScopeID: &customerY, Source: jsReturning(routeCust)},
		{Name: "p", Language: script.LanguageJS, Scope: script.ScopePlatform, Source: jsReturning(routePlat)},
	}}, nil)
	if err != nil {
		t.Fatalf("BuildScriptSnapshot: %v", err)
	}
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, routing.NewScriptResolver(snap, nil, nil), decl)

	otherAcct, otherCust := uuid.New(), uuid.New()
	cases := []struct {
		name string
		req  pipeline.RouteRequest
		want uuid.UUID
	}{
		{"account wins", pipeline.RouteRequest{AccountID: accountX, CustomerID: customerY, Dest: "2250700000000"}, connAcct},
		{"customer when no account script", pipeline.RouteRequest{AccountID: otherAcct, CustomerID: customerY, Dest: "2250700000000"}, connCust},
		{"platform fallback", pipeline.RouteRequest{AccountID: otherAcct, CustomerID: otherCust, Dest: "2250700000000"}, connPlat},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := l0.Resolve(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got.ConnectorID != tc.want {
				t.Errorf("routed to %s, want %s", got.ConnectorID, tc.want)
			}
		})
	}
}

// TestScriptNullFallsBackToDeclarative: a script returning null routes via the declarative chain.
func TestScriptNullFallsBackToDeclarative(t *testing.T) {
	acct := uuid.New()
	declRoute, declConn := uuid.New(), uuid.New()
	decl := declSnapshotWith(t, declRoute, declConn, nil)
	snap, _ := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "n", Language: script.LanguageJS, Scope: script.ScopeAccount, ScopeID: &acct, Source: `function resolveRoute(m){ return null }`},
	}}, nil)
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, routing.NewScriptResolver(snap, nil, nil), decl)

	got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{AccountID: acct, Dest: "2250700000000"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.ConnectorID != declConn {
		t.Errorf("null script routed to %s, want the declarative %s", got.ConnectorID, declConn)
	}
}

// TestScriptErrorFallsBackAndMeters: a script that throws falls back to declarative AND increments the
// failure meter with the reason — the message is never dropped (M7 acceptance criterion).
func TestScriptErrorFallsBackAndMeters(t *testing.T) {
	acct := uuid.New()
	declRoute, declConn := uuid.New(), uuid.New()
	decl := declSnapshotWith(t, declRoute, declConn, nil)
	meter := newCountingMeter()
	snap, _ := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "boom", Language: script.LanguageJS, Scope: script.ScopeAccount, ScopeID: &acct, Source: `function resolveRoute(m){ throw new Error("boom") }`},
	}}, nil)
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, routing.NewScriptResolver(snap, nil, meter), decl)

	got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{AccountID: acct, Dest: "2250700000000"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.ConnectorID != declConn {
		t.Errorf("erroring script routed to %s, want the declarative fallback %s", got.ConnectorID, declConn)
	}
	if meter.count("runtime_error") != 1 {
		t.Errorf("runtime_error meter = %d, want 1", meter.count("runtime_error"))
	}
}

// TestScriptRouteIdNotInSnapshotFallsThrough: a script that returns a route id absent from the
// declarative snapshot (deleted/disabled route) falls through to declarative — never dropped.
func TestScriptRouteIdNotInSnapshotFallsThrough(t *testing.T) {
	acct := uuid.New()
	declRoute, declConn := uuid.New(), uuid.New()
	decl := declSnapshotWith(t, declRoute, declConn, nil)
	// The script points at a route id that is NOT in the declarative snapshot.
	snap, _ := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "dangling", Language: script.LanguageJS, Scope: script.ScopeAccount, ScopeID: &acct, Source: jsReturning(uuid.New())},
	}}, nil)
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, routing.NewScriptResolver(snap, nil, nil), decl)

	got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{AccountID: acct, Dest: "2250700000000"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.ConnectorID != declConn {
		t.Errorf("dangling script route routed to %s, want the declarative fallback %s", got.ConnectorID, declConn)
	}
}

// TestLuaScriptThroughResolver: a Lua runtime resolves through the same ScriptResolver (runtime
// unification — the whole point of step-110).
func TestLuaScriptThroughResolver(t *testing.T) {
	acct := uuid.New()
	routeID, conn := uuid.New(), uuid.New()
	declRoute, declConn := uuid.New(), uuid.New()
	decl := declSnapshotWith(t, declRoute, declConn, map[uuid.UUID]uuid.UUID{routeID: conn})
	snap, _ := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "lua", Language: script.LanguageLua, Scope: script.ScopeAccount, ScopeID: &acct,
			Source: `function resolveRoute(m) return "` + routeID.String() + `" end`},
	}}, nil)
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, routing.NewScriptResolver(snap, nil, nil), decl)

	got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{AccountID: acct, Dest: "2250700000000"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.ConnectorID != conn {
		t.Errorf("lua script routed to %s, want %s", got.ConnectorID, conn)
	}
}

// TestExactHitSkipsScript: an L0 exact hit resolves before the script stage — the script must not run.
func TestExactHitSkipsScript(t *testing.T) {
	acct := uuid.New()
	portedConn := uuid.New()
	declRoute, declConn := uuid.New(), uuid.New()
	decl := declSnapshotWith(t, declRoute, declConn, nil)
	// A script that would error if it ran — it must NOT run because exact resolves first.
	snap, _ := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "boom", Language: script.LanguageJS, Scope: script.ScopeAccount, ScopeID: &acct, Source: `function resolveRoute(m){ throw new Error("must not run") }`},
	}}, nil)
	meter := newCountingMeter()
	ported := "2250700000001"
	l0 := routing.NewL0Resolver(
		fakeExact{hits: map[string]exact.Target{ported: {Type: exact.TargetConnector, ID: portedConn}}},
		routing.NewScriptResolver(snap, nil, meter), decl)

	got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{AccountID: acct, Dest: ported})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.ConnectorID != portedConn {
		t.Errorf("exact hit routed to %s, want the ported connector %s", got.ConnectorID, portedConn)
	}
	if meter.count("runtime_error") != 0 {
		t.Errorf("script ran on an exact-hit message (meter=%d), want it skipped", meter.count("runtime_error"))
	}
}

// TestScriptResolverHotSwap: Swap replaces the served snapshot; subsequent resolutions use the new one.
func TestScriptResolverHotSwap(t *testing.T) {
	acct := uuid.New()
	routeA, connA := uuid.New(), uuid.New()
	routeB, connB := uuid.New(), uuid.New()
	declRoute, declConn := uuid.New(), uuid.New()
	decl := declSnapshotWith(t, declRoute, declConn, map[uuid.UUID]uuid.UUID{routeA: connA, routeB: connB})

	snapA, _ := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "a", Language: script.LanguageJS, Scope: script.ScopeAccount, ScopeID: &acct, Source: jsReturning(routeA)},
	}}, nil)
	sr := routing.NewScriptResolver(snapA, nil, nil)
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, sr, decl)

	got, _ := l0.Resolve(context.Background(), pipeline.RouteRequest{AccountID: acct, Dest: "2250700000000"})
	if got.ConnectorID != connA {
		t.Fatalf("before swap routed to %s, want connA %s", got.ConnectorID, connA)
	}

	snapB, _ := routing.BuildScriptSnapshot(context.Background(), fakeActiveScripts{scripts: []script.Script{
		{Name: "b", Language: script.LanguageJS, Scope: script.ScopeAccount, ScopeID: &acct, Source: jsReturning(routeB)},
	}}, nil)
	sr.Swap(snapB)

	got, _ = l0.Resolve(context.Background(), pipeline.RouteRequest{AccountID: acct, Dest: "2250700000000"})
	if got.ConnectorID != connB {
		t.Errorf("after swap routed to %s, want connB %s", got.ConnectorID, connB)
	}
}

// TestNoScriptResolverStillRoutes: a nil script resolver skips L1 entirely (declarative only).
func TestNoScriptResolverStillRoutes(t *testing.T) {
	declRoute, declConn := uuid.New(), uuid.New()
	decl := declSnapshotWith(t, declRoute, declConn, nil)
	l0 := routing.NewL0Resolver(fakeExact{hits: map[string]exact.Target{}}, nil, decl)
	got, err := l0.Resolve(context.Background(), pipeline.RouteRequest{Dest: "2250700000000"})
	if err != nil || got.ConnectorID != declConn {
		t.Errorf("no-script resolve = (%s, %v), want declConn %s", got.ConnectorID, err, declConn)
	}
}
