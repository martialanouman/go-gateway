package main

import (
	"context"
	"errors"
	"fmt"
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

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

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
	if !strings.Contains(err.Error(), "postgres") {
		t.Errorf("error not attributed to postgres: %v", err)
	}
	// The DSN carries a password: neither it nor the URL may reach the error, which is logged.
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks the connection string: %v", err)
	}
}

func TestNewAdminAppReportsAnUnreachablePostgres(t *testing.T) {
	t.Parallel()

	app, err := newAdminApp(t.Context(), testConfig(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newAdminApp succeeded with an unreachable postgres")
	}
	if !strings.Contains(err.Error(), "postgres") {
		t.Errorf("error not attributed to postgres: %v", err)
	}
}

// A malformed archive prefix is interpolated into the retention statement, so it must be refused at
// construction — not discovered at the first purge, hours after the pod went Ready.
func TestNewRetainerRejectsAMalformedArchivePrefix(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.ClickHouse.ArchivePrefix = "cdr archive; DROP"

	if _, err := newRetainer(cfg, nil, silentLogger()); err == nil {
		t.Fatal("newRetainer accepted a malformed archive prefix")
	}
}

// TestNewAdminAppReleasesInDependencyOrder asserts the order on the graph newAdminApp actually builds,
// not on a stack a test pushed by hand. Here that order is load-bearing: the background runners' jobs
// use the Postgres pool, so their drain must complete BEFORE the pool is closed. Swap those two
// registrations and this service closes the pool under in-flight jobs on every deploy.
func TestNewAdminAppReleasesInDependencyOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}

	// Redis is released AFTER the runners, not before: since step-250e the exact-route import job
	// invalidates cache keys and announces its config change ON REDIS, after its Postgres commit. With
	// the previous order, every deploy that caught an import in flight closed the client first, both
	// calls failed silently (they are best-effort by design), and up to 10 000 numbers stayed pointed
	// at their former carrier for a full TTL while the job logged "completed".
	want := []string{"feed", "clients", "runners", "redis", "stores"}
	if got := releaseOrder(app); !slices.Equal(got, want) {
		t.Errorf("release order is %v, want %v — the runners' jobs use the Postgres pool AND Redis, so "+
			"both must outlive the drain", got, want)
	}
}

// releaseOrder runs close() and reports the names in the order the closers ACTUALLY ran. It wraps the
// registered functions rather than reading the slice backwards: a test that reverses the slice itself
// would replay close()'s own loop, and an inverted loop would keep it green.
//
// It releases the app, so a caller must not close it a second time.
func releaseOrder(a *adminApp) []string {
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

// TestNewAdminAppBuildsTheWholeGraph assembles the real service against test dependencies. Kafka,
// ClickHouse, session-manager and content-key-svc are deliberately pointed at a closed port: none of
// them may be touched while the graph is being built, so a boot that reaches them is a regression.
func TestNewAdminAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}
	defer app.close()

	for name, component := range map[string]any{
		"ops":      app.ops,
		"http":     app.http,
		"retainer": app.retainer,
		"hub":      app.hub,
		"stream":   app.stream,
	} {
		if component == nil || reflect.ValueOf(component).IsNil() {
			t.Errorf("component %q was not wired", name)
		}
	}

	// Building the graph must not start serving: both ports are bound by their Run, which only the
	// supervisor calls.
	for name, port := range map[string]int{"ops": cfg.OpsPort, "admin http": cfg.HTTP.Port} {
		if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second); err == nil {
			_ = c.Close()
			t.Errorf("%s port %d is listening after wiring alone", name, port)
		}
	}
}

// testConfig is a valid Admin API configuration whose external dependencies all point at a closed
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
		Kafka:           config.Kafka{Brokers: []string{closed}, Timeout: time.Second},
		ClickHouse:      config.ClickHouse{Addr: []string{closed}, Database: "gateway", Timeout: time.Second, CDRRetention: 24 * time.Hour},
		Redis:           config.Redis{URL: "redis://" + closed, Timeout: 500 * time.Millisecond},
		HTTP:            config.HTTP{Port: freePort(), ReadHeaderTimeout: 5 * time.Second, AdminTokens: []string{"test-token:admin:read|admin:write"}},
		SMPP:            config.SMPP{SessionManagerAddr: closed},
		ContentKey:      config.ContentKey{Addr: closed},
		OTel:            config.OTel{Disabled: true},
	}
}

// freePort returns a port no server is bound to, so a test can assert that nothing started listening
// on it.
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

// TestNewAdminAppInvalidatesTheExactRouteCache proves, on the graph newAdminApp actually builds, that
// an exact-route mutation reaches the Redis the data plane reads. The handlers default a missing
// invalidator to a no-op, so a Deps literal that forgot ExactRouteCache would boot, pass every handler
// test and leave each re-ported number on its former carrier for a whole TTL — the same shape as the
// L0 lookup counter that was fed on every message and registered nowhere (step-250e). A test that
// injects the invalidator itself cannot see that hole; only the booted service can.
func TestNewAdminAppInvalidatesTheExactRouteCache(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	rdb := redistest.Client(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}
	defer app.close()

	// A stale entry, as a lost invalidation or the cache-aside window leaves one. Written in the wire
	// form the resolver reads — as a literal on purpose, the anchoring TestExactRouteRedisEncodingIsPinned
	// asks for, so a drift in either package shows up here rather than cancelling out.
	msisdn := fmt.Sprintf("22507%08d", uuid.New().ID()%100_000_000)
	key := "exactroute:{" + msisdn + "}"
	if err := rdb.Set(ctx, key, "connector:"+uuid.NewString(), 0).Err(); err != nil {
		t.Fatalf("seed the stale cache entry: %v", err)
	}

	body := fmt.Sprintf(`{"msisdn":%q,"target_type":"connector","target_id":%q}`, msisdn, uuid.NewString())
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/exact-routes", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	app.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create exact route = %d, want 201; body=%s", rec.Code, rec.Body)
	}

	if err := rdb.Get(ctx, key).Err(); !errors.Is(err, redis.Nil) {
		t.Errorf("after the create the cache key is still there (err=%v); the booted service did not "+
			"invalidate the data-plane cache, so a re-ported number would keep its former carrier for a "+
			"whole TTL — the handlers' nil-to-no-op default hides a missing ExactRouteCache in the wiring", err)
	}
}
