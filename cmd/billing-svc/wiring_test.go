package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// The wiring must fail as a VALUE, never as a process exit: a constructor that log.Fatals cannot be
// tested, and a boot failure that kills the process cannot be reported by the caller either.

// Redis opens first, so it is the store a no-dependency test can reach.
func TestOpenStoresRejectsAnUnparsableRedisURL(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Redis.URL = "redis://gateway:hunter2@:::/0"

	st, err := openStores(t.Context(), cfg)
	if err == nil {
		st.close()
		t.Fatal("openStores accepted an unparsable redis url")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("error not attributed to redis: %v", err)
	}
	// The DSN carries a password: neither it nor the URL may reach the error, which is logged.
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks the connection string: %v", err)
	}
}

// The Postgres DSN carries a password too, and reaching the code that parses it means getting past
// Redis — hence the real one. Both stores are checked because either error is logged verbatim.
func TestOpenStoresRejectsAnUnparsableDatabaseURL(t *testing.T) {
	cfg := testConfig()
	cfg.Redis = redistest.Config(t)
	cfg.Postgres.URL = "postgres://gateway:hunter2@:::/gateway"

	st, err := openStores(t.Context(), cfg)
	if err == nil {
		st.close()
		t.Fatal("openStores accepted an unparsable postgres url")
	}
	if !strings.Contains(err.Error(), "postgres") {
		t.Errorf("error not attributed to postgres: %v", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks the connection string: %v", err)
	}
}

// Redis opens first, so it is the dependency an all-unreachable config must name.
func TestNewBillingAppReportsAnUnreachableRedis(t *testing.T) {
	t.Parallel()

	app, err := newBillingApp(t.Context(), testConfig(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newBillingApp succeeded with an unreachable redis")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("error not attributed to redis: %v", err)
	}
}

// TestNewBillingAppReleasesInDependencyOrder asserts the order on the graph newBillingApp actually
// builds, not on a stack a test pushed by hand: the reaper settles through the Postgres and Redis pools it must outlive.
func TestNewBillingAppReleasesInDependencyOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newBillingApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newBillingApp: %v", err)
	}

	want := []string{"alerts", "reaper", "stores"}
	if got := releaseOrder(app); !slices.Equal(got, want) {
		t.Errorf("release order is %v, want %v", got, want)
	}
}

// releaseOrder runs close() and reports the names in the order the closers ACTUALLY ran. It wraps the
// registered functions rather than reading the slice backwards: a test that reverses the slice itself
// would replay close()'s own loop, and an inverted loop would keep it green.
//
// It releases the app, so a caller must not close it a second time.
func releaseOrder(a *billingApp) []string {
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

// TestNewBillingAppBuildsTheWholeGraph assembles the real service against test dependencies. Kafka and
// ClickHouse are deliberately pointed at a closed port: neither may be touched while the graph is being
// built, so a boot that reaches them is a regression.
func TestNewBillingAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newBillingApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newBillingApp: %v", err)
	}
	defer app.close()

	for name, component := range map[string]any{
		"ops":            app.ops,
		"grpc":           app.grpc,
		"repo":           app.repo,
		"configProvider": app.configProvider,
		"reconciler":     app.reconciler,
		"reaper":         app.reaper,
	} {
		if component == nil || reflect.ValueOf(component).IsNil() {
			t.Errorf("component %q was not wired", name)
		}
	}

	// Building the graph must not start serving: both ports are bound by their Run, which only the
	// supervisor calls.
	for name, port := range map[string]int{"ops": cfg.OpsPort, "grpc": cfg.GRPC.Port} {
		if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second); err == nil {
			_ = c.Close()
			t.Errorf("%s port %d is listening after wiring alone", name, port)
		}
	}
}

// TestClickHouseIsNotAReadinessDependency pins the property step-207 is about to turn into a kubelet
// probe. The reaper reads ClickHouse to settle orphaned reservations, but it is a periodic background
// job: a ClickHouse outage must leave billing-svc IN the load balancer, since balances are still served
// from Redis and the durable ledger. Until now a comment said so and nothing checked it.
//
// It asserts the answer the kubelet actually gets, not the declaration: ClickHouse is unreachable here,
// so adding it to the checks turns this 200 into a 503 AND adds a "clickhouse" key — the test fails
// twice over.
func TestClickHouseIsNotAReadinessDependency(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	// Port 0 lets the kernel pick: the ops server is genuinely served here, so it cannot reuse the port
	// the "nothing is listening" test asserts is free.
	cfg.OpsPort = 0

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newBillingApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newBillingApp: %v", err)
	}
	defer app.close()

	served, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- app.ops.Run(served, time.Second) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("ops server: %v", err)
		}
	})

	addr := boundAddr(t, app.ops.Addr)
	resp, err := http.Get("http://" + addr + "/readyz") //nolint:noctx // bounded by the server's own timeouts
	if err != nil {
		t.Fatalf("get /readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("/readyz returned %d with an unreachable ClickHouse: %s", resp.StatusCode, body)
	}

	var got struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode /readyz body: %v", err)
	}

	names := make([]string, 0, len(got.Checks))
	for name := range got.Checks {
		names = append(names, name)
	}
	slices.Sort(names)
	if want := []string{"postgres", "redis"}; !slices.Equal(names, want) {
		t.Errorf("readiness checks are %v, want exactly %v", names, want)
	}
}

// boundAddr waits for the ops server to publish the address it actually listened on. Before Run binds,
// Addr still reports the configured ":0", which no client can dial.
func boundAddr(t *testing.T, addr func() string) string {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a := addr(); !strings.HasSuffix(a, ":0") {
			return a
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("ops server never bound a port")
	return ""
}

// testConfig is a valid billing-svc configuration whose external dependencies all point at a closed
// port.
func testConfig() config.Config {
	closed := "127.0.0.1:1"
	return config.Config{
		ServiceName:     serviceName,
		Environment:     config.EnvDevelopment,
		LogLevel:        "info",
		OpsPort:         freePort(),
		ShutdownTimeout: 5 * time.Second,
		Postgres:        config.Postgres{URL: "postgres://gateway:gateway@" + closed + "/gateway?sslmode=disable", MaxConns: 2, Timeout: 500 * time.Millisecond},
		Redis:           config.Redis{URL: "redis://" + closed, Timeout: 500 * time.Millisecond},
		Kafka:           config.Kafka{Brokers: []string{closed}, Timeout: time.Second},
		ClickHouse:      config.ClickHouse{Addr: []string{closed}, Database: "gateway", Timeout: time.Second},
		GRPC:            config.GRPC{Port: freePort()},
		BillingReaper:   config.BillingReaper{MinAge: 15 * time.Minute, Interval: time.Minute},
		OTel:            config.OTel{Disabled: true},
	}
}

// freePort returns a port no server is bound to, so a test can assert that nothing started listening on
// it.
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRequiredSectionsValidatesTheReaperKnobs is the boot contract of this binary, asserted on the very
// declaration the process uses. config.Load validates only what a service declares, so an omission here
// is silent by construction: BILLING_REAPER_INTERVAL=0 booted fine until step-193d and then panicked in
// time.NewTicker, and BILLING_REAPER_MIN_AGE=1s would have let the reaper settle messages still in
// flight.
//
// Removing config.SectionBillingReaper from requiredSections must turn this red.
func TestRequiredSectionsValidatesTheReaperKnobs(t *testing.T) {
	for name, vars := range map[string]map[string]string{
		"an interval time.NewTicker would panic on": {"BILLING_REAPER_INTERVAL": "0"},
		"a window that would race connector-pool":   {"BILLING_REAPER_MIN_AGE": "1s"},
	} {
		t.Run(name, func(t *testing.T) {
			for k, v := range vars {
				t.Setenv(k, v)
			}

			_, err := config.Load(serviceName, requiredSections...)
			if err == nil {
				t.Fatalf("billing-svc accepted %v at boot", vars)
			}
			// The environment here is the developer's, not a hermetic one: without naming the variable
			// an unrelated failure would pass this test for the wrong reason.
			for k := range vars {
				if !strings.Contains(err.Error(), k) {
					t.Errorf("boot refusal does not name %s: %v", k, err)
				}
			}
		})
	}
}

// spyOrphanSource records the cut-off the reaper asked for and reports no orphans, so ReapOnce touches
// neither the outcome reader nor the settler — the sweep window is observable without a database.
type spyOrphanSource struct{ olderThan time.Time }

func (s *spyOrphanSource) OrphanedReservations(_ context.Context, olderThan time.Time, _ int) ([]cp.OrphanedReservation, error) {
	s.olderThan = olderThan
	return nil, nil
}

// TestReaperIsWiredWithTheConfiguredMinAge closes the hole this PR's review found: swapping MinAge and
// Interval at the wiring site compiles and — until now — left the whole suite green. The reaper would
// then sweep reservations minutes old instead of a quarter of an hour, settling messages still in flight
// and refunding SMS the SMSC actually took. That is the exact failure BILLING_REAPER_MIN_AGE's floor
// exists to prevent, reintroduced one layer below the validation.
//
// It builds the reaper through reaperOptions — the call the process itself uses — rather than repeating
// billing.WithMinAge(cfg…) here, which would pass no matter what the wiring did.
func TestReaperIsWiredWithTheConfiguredMinAge(t *testing.T) {
	cfg := testConfig()
	cfg.BillingReaper.MinAge = 42 * time.Minute
	// Distinct from MinAge, so wiring the wrong field lands far outside the window asserted below.
	cfg.BillingReaper.Interval = time.Minute

	spy := &spyOrphanSource{}
	reaper := billing.NewReaper(spy, nil, nil, reaperOptions(cfg)...)

	before := time.Now()
	if err := reaper.ReapOnce(t.Context()); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	after := time.Now()

	// ReapOnce reads the clock itself, somewhere between these two samples, and subtracts MinAge from it.
	// Bracketing the cut-off by both samples asserts the window exactly, with no arbitrary slack: any
	// other duration — Interval's minute, or the reaper's own 15m default — falls outside.
	if spy.olderThan.Before(before.Add(-cfg.BillingReaper.MinAge)) ||
		spy.olderThan.After(after.Add(-cfg.BillingReaper.MinAge)) {
		t.Errorf("reaper sweeps reservations older than %s, want %s (BILLING_REAPER_MIN_AGE)",
			before.Sub(spy.olderThan), cfg.BillingReaper.MinAge)
	}
}
