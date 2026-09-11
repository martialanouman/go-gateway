package main

import (
	"bytes"
	"context"
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
			return c
		}
		first, second, third := newConnector("260c-1"), newConnector("260c-2"), newConnector("260c-3")
		t.Cleanup(func() {
			// Background, not ctx: the test's context is cancelled by the time cleanups run, and the
			// shared database would keep these rows for every sibling run.
			for _, id := range []uuid.UUID{first.ID, second.ID, third.ID} {
				_ = connectors.Delete(context.Background(), id)
			}
		})

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

		// newRouterApp builds the Watcher and the ops server but runs neither — only main.go supervises
		// them — so the test plays the supervisor for both.
		watcherDone := make(chan error, 1)
		go func() { watcherDone <- app.watcher.Run(ctx) }()
		opsDone := make(chan struct{})
		go func() { defer close(opsDone); _ = app.ops.Run(ctx, time.Second) }()
		// Release in dependency order: stop the goroutines and JOIN them before closing what they read,
		// or the Watcher can run a rebuild against a pool app.close() has already shut.
		t.Cleanup(func() {
			cancel()
			<-watcherDone
			<-opsDone
			app.close()
		})

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
		waitResolves(t, invalidate, resolved, second.ID, "the hot reload never reached the served "+
			"snapshot with postgres up, so the outage assertion below would prove nothing")

		// The outage. A third retarget is committed durably, then the link drops before the rebuild can
		// read it.
		retarget(third.ID)

		// Everything the log has said so far is the control's business. Only the suffix written AFTER
		// the cut can testify about the cut — and the distinction is not pedantic here: the rebuild
		// closure swaps the routes BEFORE reloading the Blooms, the scripts, the credit and the content
		// policy, so a control rebuild can perfectly well have reached its route swap and then failed
		// further down, leaving this exact line in the buffer already.
		mark := len(logs.String())
		proxy.Cut()

		// Wait for the evidence rather than for a duration: the Watcher coalesces for 250 ms and a
		// loaded CI can take much longer than a sleep would allow for.
		deadline := time.Now().Add(20 * time.Second)
		for !strings.Contains(logs.String()[mark:], "config watcher: rebuild failed; keeping current state") {
			invalidate()
			if time.Now().After(deadline) {
				t.Fatal("no failed-rebuild log line after the cut: this log is the ONLY signal a stale " +
					"snapshot produces — /readyz stays 200 and no metric of the route snapshot moves — so " +
					"losing it makes the degradation completely invisible")
			}
			time.Sleep(100 * time.Millisecond)
		}

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

		// The other half of "invisible", and the half a future edit is most likely to break: readiness
		// does NOT notice. Adding postgres.PingCheck to the router's ops server would look like an
		// improvement and would drain every router pod during an outage it is designed to serve
		// through — the mirror image of the guard chaos_test.go keeps for Redis.
		//
		// The assertion is on the absence of the probe, not on a 200: Kafka sits at a closed port in
		// testConfig, so the aggregate is 503 for a reason that has nothing to do with this outage.
		// That Kafka DOES gate the router's readiness is pinned next door, in chaos_test.go.
		if _, body := readyz(t, app); len(body) > 0 {
			if _, named := body["postgres"]; named {
				t.Errorf("/readyz now probes postgres (%v): §16 records this dependency as a MASKED "+
					"degradation precisely because readiness ignores it, and gating on it would drain "+
					"every router pod during an outage the router is built to serve through", body)
			}
		}

		// Recovery, and the half a refusal alone would not establish: nothing latched. The republish loop
		// is infrastructure recovery, not the behaviour under test — a lone notification can land while
		// the pool is still handing out connections the cut killed, and NOTHING retries a failed rebuild
		// on its own (step-395).
		proxy.Resume()
		waitResolves(t, invalidate, resolved, third.ID, "the snapshot latched on the outage")
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
		// The logger is captured, not discarded: "it is still retrying" and "it is hung on the first
		// attempt" are indistinguishable from the outside, and loadWithRetry's own per-attempt Warn is
		// the only thing that tells them apart.
		logs := &syncBuffer{}
		go func() {
			r, err := loadSnapshotWithRetry(ctx, postgres.NewRouteRepo(pool), slog.New(slog.NewTextHandler(logs, nil)))
			done <- result{r, err}
		}()

		// Wait for a SECOND attempt to have failed — the backoff runs 500 ms, 1 s, 2 s — rather than for
		// a duration that would prove nothing about what happened during it.
		deadline := time.Now().Add(20 * time.Second)
		for !strings.Contains(logs.String(), "attempt=2") {
			select {
			case got := <-done:
				t.Fatalf("loadSnapshotWithRetry returned (%v, %v) while postgres was cut: giving up here "+
					"bricks a restarting pod on a transient outage, which is precisely what the retry "+
					"exists to prevent", got.resolver, got.err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("no second attempt was ever logged (log: %q): the first failure is not being "+
					"retried at all, so the boot is hung rather than backing off", logs.String())
			}
			time.Sleep(50 * time.Millisecond)
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

// waitResolves republishes the invalidation and polls resolve until it reports want, failing with why
// once the deadline passes.
//
// It republishes on every pass rather than once up front, and that is not belt-and-braces: pub/sub has
// no delivery guarantee, a notification published before the Watcher's SUBSCRIBE has registered is
// simply gone, and NOTHING in the Watcher replays a missed or failed rebuild on its own (step-395). A
// single publish would turn that into a 15-second hang ending in a misleading "the hot reload never
// worked".
func waitResolves(t *testing.T, invalidate func(), resolve func() uuid.UUID, want uuid.UUID, why string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		invalidate()
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
