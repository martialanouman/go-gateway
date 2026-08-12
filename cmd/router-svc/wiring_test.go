package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// The wiring must fail as a VALUE, never as a process exit: a constructor that log.Fatals cannot be
// tested, and a boot failure that kills the process cannot be reported by the caller either.

func TestOpenStoresRejectsAnUnparsableDatabaseURL(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Postgres.URL = "postgres://gateway:hunter2@:::/gateway"

	st, err := openStores(t.Context(), cfg)
	if err == nil {
		st.close()
		t.Fatal("openStores accepted an unparsable postgres url")
	}
	if !strings.Contains(err.Error(), "connect postgres") {
		t.Errorf("error not attributed to postgres: %v", err)
	}
	// The DSN carries a password: neither it nor the URL may reach the error, which is logged.
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks the connection string: %v", err)
	}
}

func TestNewRouterAppReportsAnUnreachablePostgres(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Postgres.URL = "postgres://gateway:gateway@" + closedPort(t) + "/gateway?sslmode=disable"

	app, err := newRouterApp(t.Context(), cfg, silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newRouterApp succeeded with an unreachable postgres")
	}
	if !strings.Contains(err.Error(), "connect postgres") {
		t.Errorf("error not attributed to postgres: %v", err)
	}
}

// TestNewRouterAppReleasesInDependencyOrder asserts the order on the graph newRouterApp actually
// builds, not on a stack a test pushed by hand: the pipeline, both projectors and the metric stream all read the stores.
func TestNewRouterAppReleasesInDependencyOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newRouterApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRouterApp: %v", err)
	}

	want := []string{"stream", "outcome", "accepted", "pipeline", "redis", "stores"}
	if got := releaseOrder(app); !slices.Equal(got, want) {
		t.Errorf("release order is %v, want %v", got, want)
	}
}

// releaseOrder runs close() and reports the names in the order the closers ACTUALLY ran. It wraps the
// registered functions rather than reading the slice backwards: a test that reverses the slice itself
// would replay close()'s own loop, and an inverted loop would keep it green.
//
// It releases the app, so a caller must not close it a second time.
func releaseOrder(a *routerApp) []string {
	var released []string
	for i := range a.closers {
		c := a.closers[i] // a copy, so c.fn is the original and not the stand-in below
		a.closers[i].fn = func() {
			released = append(released, c.name)
			c.fn()
		}
	}
	a.close()
	return released
}

// TestNewRouterAppBuildsTheWholeGraph assembles the real service against test dependencies. Kafka,
// ClickHouse, billing-svc and content-key-svc are deliberately pointed at a closed port: none of them
// may be touched while the graph is being built, so a boot that reaches them is a regression.
func TestNewRouterAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newRouterApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRouterApp: %v", err)
	}
	defer app.close()

	// The outcome projection must begin at the START of its topic. This is a durability property, not
	// a preference: nothing orders the rollout of connector-pool-svc and router-svc, so outcomes can
	// already be on mt.outcome when this group first joins. A group starting at the end skips them for
	// ever — those messages read "accepted" until they are purged, and billing.Reaper, which settles
	// orphan reservations against the recorded CDR outcome, holds their credit for good (step-201c D9).
	if app.outcome == nil {
		t.Fatal("the outcome projection was not wired")
	}
	if app.outcome.kafka.StartsFromEnd() {
		t.Error("the outcome projection starts at the end of mt.outcome: every outcome produced before " +
			"this group first joined is skipped for ever, and its reservation held for good")
	}

	for name, component := range map[string]any{
		"ops":      app.ops,
		"router":   app.router,
		"accepted": app.accepted,
		"outcome":  app.outcome,
		"watcher":  app.watcher,
		"emitter":  app.emitter,
		"consumer": app.consumer,
		"catalog":  app.catalog,
	} {
		if component == nil || reflect.ValueOf(component).IsNil() {
			t.Errorf("component %q was not wired", name)
		}
	}

	// Building the graph must not start serving: the ops port is bound by ops.Run, which only the
	// supervisor calls.
	if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.OpsPort)), time.Second); err == nil {
		_ = c.Close()
		t.Errorf("ops port %d is listening after wiring alone", cfg.OpsPort)
	}
}

// TestOpsExposesTheOutcomeProjectionDrainRate asserts against the REAL graph that the projection's drain
// rate reaches a scraper, because a counter that no registry owns is the failure this metric cannot
// afford.
//
// It is the denominator of the status-lag alert (step-201c, D13): queue_depth_records{queue="mt.outcome"}
// over rate(cdr_outcome_projected_total). Declared but unregistered, it increments diligently in memory,
// is scraped by nobody, and the alert expression divides by a series that does not exist — no result, no
// alert, and a lag that climbs behind a dashboard showing nothing at all. Counting is not the deliverable;
// being scraped is.
func TestOpsExposesTheOutcomeProjectionDrainRate(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newRouterApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRouterApp: %v", err)
	}
	defer app.close()

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(app.ops.Registry(), promhttp.HandlerOpts{}).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "cdr_outcome_projected_total") {
		t.Error("cdr_outcome_projected_total is fed by the outcome projection but not exposed on /metrics: " +
			"the status-lag alert would divide by a series that does not exist")
	}
}

// TestLagAlertOperandsHaveTheLabelSetsTheExpressionAssumes freezes the two facts the alert expression
// rests on, because the expression itself is evaluated by no test in this repo (step-201c, D14/D17).
//
// The alert is:
//
//	max(queue_depth_records{queue="mt.outcome"}) / sum(rate(cdr_outcome_projected_total[5m])) > 30
//
// Both sides are aggregated, and the first version of this expression was not — it divided the two
// operands directly. A PromQL binary operator matches on the FULL label set, so a left side carrying
// `queue` and a right side carrying nothing produce ZERO matched pairs: an empty vector, no alert, ever.
// `max` and `sum` differ for a second reason: the gauge is group-scoped (every replica publishes the same
// value) while the counter is per-pod, so pairwise matching would have multiplied the quotient by the
// replica count.
//
// This test cannot catch a badly written expression. It catches the drift that would silently invalidate
// the one we wrote: a label appearing on the counter, or the gauge's `queue` label being renamed.
func TestLagAlertOperandsHaveTheLabelSetsTheExpressionAssumes(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newRouterApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRouterApp: %v", err)
	}
	defer app.close()

	// A gauge vector exposes nothing until it has a child; feed it the value publishQueueDepth would.
	app.catalog.QueueDepth.WithLabelValues("mt.outcome").Set(0)

	rec := httptest.NewRecorder()
	promhttp.HandlerFor(app.ops.Registry(), promhttp.HandlerOpts{}).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, `queue_depth_records{queue="mt.outcome"}`) {
		t.Error(`the alert's numerator is not exposed as queue_depth_records{queue="mt.outcome"}: ` +
			"the expression selects a series that does not exist")
	}
	// No braces: the counter must carry NO label of its own. One added here would survive sum() but would
	// break any future expression that matched pairwise, and would silently change what sum() aggregates.
	if !strings.Contains(body, "\ncdr_outcome_projected_total ") {
		t.Error("cdr_outcome_projected_total now carries labels: the alert's denominator is no longer the " +
			"bare per-pod counter the aggregation was written against")
	}
}

// testConfig is a valid router configuration whose external dependencies all point at a closed port.
func testConfig() config.Config {
	closed := "127.0.0.1:1"
	return config.Config{
		ServiceName:     serviceName,
		Environment:     config.EnvDevelopment,
		LogLevel:        "info",
		OpsPort:         freePort(),
		ShutdownTimeout: 5 * time.Second,
		Postgres:        config.Postgres{URL: "postgres://gateway:gateway@" + closed + "/gateway?sslmode=disable", MaxConns: 2, Timeout: 500 * time.Millisecond},
		Kafka:           config.Kafka{Brokers: []string{closed}, Timeout: time.Second},
		ClickHouse:      config.ClickHouse{Addr: []string{closed}, Database: "gateway", Timeout: time.Second},
		Redis:           config.Redis{URL: "redis://" + closed, Timeout: 500 * time.Millisecond},
		Billing:         config.Billing{Addr: closed, ReserveTimeout: 200 * time.Millisecond},
		ContentKey:      config.ContentKey{Addr: closed},
		OTel:            config.OTel{Disabled: true},
	}
}

// closedPort returns a host:port nothing listens on: a port is bound and immediately released, so the
// address is valid and refuses connections.
func closedPort(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

// freePort returns a port no server is bound to, so a test can assert that nothing started listening
// on it. It falls back to a fixed high port if the probe fails, which only weakens the assertion.
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 59090
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
