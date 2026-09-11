package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestRouterConfigSnapshotsDegradeSilentlyWhenPostgresIsCut is the step-260c acceptance test for the
// config-snapshot row of the failure-policy matrix (guide de codage §16). That row is neither
// fail-open nor fail-closed: a rebuild that cannot reach Postgres is logged and dropped, and the pod
// keeps serving the LAST snapshot it managed to build. The risk is not a refusal, it is CONFIG THAT
// HAS SILENTLY STOPPED MOVING — a withdrawn opt-out that keeps applying, a reroute that never lands.
//
// It lives in cmd/router-svc rather than internal/config on purpose. internal/config already proves
// the generic half (TestWatcherRebuildFailureKeepsRunning: a failing rebuild does not kill the
// Watcher), and it proves it against a rebuild closure the test writes itself. What §16 is about to
// claim is narrower and lives one layer up: the ROUTES stay resolvable, neither nil nor empty, and
// that is decided by the closure in wiring.go — the one the service actually runs. A test that built
// its own closure would be testing the stand-in.
//
// The two subtests split the row's two halves, which the fiche had merged into one: at boot Postgres
// is a hard dependency, and in service it is a masked degradation.
func TestRouterConfigSnapshotsDegradeSilentlyWhenPostgresIsCut(t *testing.T) {
	t.Run("a failed rebuild keeps serving the last snapshot", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		seed := pgtest.Pool(t) // uncut: every route change below must reach the database even while the graph's pool cannot
		rdb := redistest.Client(t)

		connectors := postgres.NewConnectorRepo(seed)
		newConnector := func(name string) cp.Connector {
			t.Helper()
			c, err := connectors.Create(ctx, cp.NewConnector{Name: name + "-" + uuid.NewString(), Host: "h",
				Port: 2775, BindType: cp.BindTRX, SystemID: name, PasswordHash: "h"})
			if err != nil {
				t.Fatalf("create connector %s: %v", name, err)
			}
			t.Cleanup(func() { _ = connectors.Delete(context.Background(), c.ID) })
			return c
		}
		first, second, third := newConnector("260c-1"), newConnector("260c-2"), newConnector("260c-3")

		// The snapshot ranks every route of the SHARED database (longest prefix, then priority): a prefix
		// unique to this run keeps a sibling test's route from outranking ours and answering for it.
		prio, dest := 1, "2259"+strconv.Itoa(int(uuid.New().ID()%90000+10000))
		routes := postgres.NewRouteRepo(seed)
		route, err := routes.Create(ctx, cp.NewRoute{
			Name: "260c-snapshot-" + uuid.NewString(), Priority: &prio, MatchDestPattern: &dest,
			DistributionStrategy: cp.DistributionStatic, TargetConnectorID: &first.ID,
		})
		if err != nil {
			t.Fatalf("create route: %v", err)
		}
		t.Cleanup(func() { _ = routes.Delete(context.Background(), route.ID) })
		dial := "+" + dest + "0001"

		retarget := func(to uuid.UUID) {
			t.Helper()
			if _, err := routes.Update(ctx, route.ID, cp.RoutePatch{TargetConnectorID: &to}); err != nil {
				t.Fatalf("retarget the route: %v", err)
			}
		}

		cfg, proxy := pgtest.CuttableConfig(t)
		routerCfg := testConfig()
		routerCfg.Postgres = cfg
		routerCfg.Redis = redistest.Config(t)

		// The graph opens its own pool from the config and pings it on boot, so it is built while the
		// link is still up. The log is captured rather than discarded: the single Error line the Watcher
		// writes is the ONLY trace a stale snapshot leaves anywhere — no metric, no readiness change —
		// and pinning it is what makes "masked" a documented property instead of an accident.
		logs := &syncBuffer{}
		app, err := newRouterApp(ctx, routerCfg, slog.New(slog.NewTextHandler(logs, nil)))
		if err != nil {
			t.Fatalf("newRouterApp: %v", err)
		}
		defer app.close()

		// newRouterApp builds the Watcher but does not run it — only main.go supervises it — so the test
		// plays the supervisor, and gives the Redis SUBSCRIBE a beat to register.
		watcherDone := make(chan error, 1)
		go func() { watcherDone <- app.watcher.Run(ctx) }()
		time.Sleep(200 * time.Millisecond)

		pub := redisstore.NewPubSubPublisher(rdb)
		invalidate := func() {
			t.Helper()
			if err := pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`)); err != nil {
				t.Fatalf("publish invalidation: %v", err)
			}
		}
		resolved := func() uuid.UUID {
			t.Helper()
			got, rerr := app.routes.Resolve(ctx, dial)
			if rerr != nil {
				t.Fatalf("Resolve(%s) with a snapshot in hand = %v: a rebuild failure must never empty "+
					"the snapshot, and ErrNoRoute here would route by default — for a ported number, to "+
					"its former operator", dial, rerr)
			}
			return got.ConnectorID
		}

		// Control A: the boot snapshot resolves to the connector the route named.
		if got := resolved(); got != first.ID {
			t.Fatalf("at boot Resolve(%s) = %s, want %s — the control failed", dial, got, first.ID)
		}

		// Control B, and the one that makes the outage observable: with the link up, a retarget really
		// does reach the served snapshot. Without it, "still serving the old one" would hold just as well
		// against a hot reload that never worked at all.
		retarget(second.ID)
		invalidate()
		waitResolves(t, resolved, second.ID, "the hot reload never reached the served snapshot with "+
			"postgres up, so the outage assertion below would prove nothing")

		// The outage. A third retarget is committed durably, then the link drops before the rebuild can
		// read it.
		retarget(third.ID)
		proxy.Cut()
		invalidate()

		// Past the coalescing window plus a rebuild's worth of slack: whatever the Watcher was going to
		// do, it has done.
		time.Sleep(2 * time.Second)

		if got := resolved(); got != second.ID {
			t.Errorf("with postgres cut Resolve(%s) = %s, want %s (the last successfully built "+
				"snapshot): serving %s would mean a rebuild that could not read the routes still "+
				"swapped something in", dial, got, second.ID, third.ID)
		}
		select {
		case err := <-watcherDone:
			t.Fatalf("the Watcher returned %v during the outage: a rebuild failure is logged and "+
				"retried on the next notification, never fatal — a pod that dies here takes the stale "+
				"but working snapshot with it", err)
		default:
		}
		if !strings.Contains(logs.String(), "config watcher: rebuild failed; keeping current state") {
			t.Error("the failed rebuild left no log line: this log is the ONLY signal a stale snapshot " +
				"produces — /readyz stays 200 and no metric moves — so losing it makes the degradation " +
				"completely invisible")
		}

		// Recovery, and the half a refusal alone would not establish: nothing latched. The republish loop
		// is infrastructure recovery, not the behaviour under test — a lone notification can land while
		// the pool is still handing out connections the cut killed, and NOTHING retries a failed rebuild
		// on its own (step-395).
		proxy.Resume()
		deadline := time.Now().Add(20 * time.Second)
		for {
			invalidate()
			if resolved() == third.ID {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("after postgres came back Resolve(%s) never reached %s: the snapshot latched "+
					"on the outage", dial, third.ID)
			}
			time.Sleep(300 * time.Millisecond)
		}
	})

	t.Run("the boot snapshot load retries instead of giving up", func(t *testing.T) {
		// The OTHER half of the boot story is already proven: TestNewRouterAppReportsAnUnreachablePostgres
		// pins that openStores fails fast (postgres.NewPool pings without retrying), so a pod that starts
		// while Postgres is down crash-loops rather than hanging. This subtest covers what nothing covers
		// — loadWithRetry, which only bites when Postgres drops BETWEEN the pool's ping and the snapshot
		// loads, and which has no attempt ceiling at all.
		pool, proxy := pgtest.Cuttable(t)
		proxy.Cut()

		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()

		type result struct {
			resolver *routing.SnapshotResolver
			err      error
		}
		done := make(chan result, 1)
		go func() {
			r, err := loadSnapshotWithRetry(ctx, postgres.NewRouteRepo(pool), silentLogger())
			done <- result{r, err}
		}()

		// The backoff runs 500 ms, 1 s, 2 s: by now several attempts have failed and been retried.
		select {
		case got := <-done:
			t.Fatalf("loadSnapshotWithRetry returned (%v, %v) while postgres was cut: giving up here "+
				"bricks a restarting pod on a transient outage, which is precisely what the retry exists "+
				"to prevent", got.resolver, got.err)
		case <-time.After(1500 * time.Millisecond):
		}

		proxy.Resume()

		select {
		case got := <-done:
			if got.err != nil {
				t.Fatalf("loadSnapshotWithRetry after postgres came back: %v", got.err)
			}
			if got.resolver == nil {
				t.Fatal("loadSnapshotWithRetry returned a nil resolver with no error")
			}
		case <-time.After(40 * time.Second):
			t.Fatal("loadSnapshotWithRetry never returned after postgres came back: the retry is not a " +
				"backoff, it is a hang — and the pod would never become ready")
		}
	})
}

// waitResolves polls resolve until it reports want, failing with why once the deadline passes. It
// polls because a rebuild is asynchronous: the Watcher coalesces for 250 ms before it even starts.
func waitResolves(t *testing.T, resolve func() uuid.UUID, want uuid.UUID, why string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		if got := resolve(); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the resolver never reached %s: %s", want, why)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// syncBuffer is an io.Writer a slog handler can share with the test goroutine. The Watcher logs from
// its own goroutine, so an unguarded bytes.Buffer would be a data race the -race build reports.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var _ io.Writer = (*syncBuffer)(nil)
