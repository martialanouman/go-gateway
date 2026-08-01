package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestAppCloseReleasesInReverseOrderOfOpening(t *testing.T) {
	t.Parallel()

	// This is the invariant the deferred Closes in run() used to guarantee for free: a store must
	// never be released before something built on top of it.
	var released []string
	a := &routerApp{}
	a.onClose(func() { released = append(released, "postgres") })
	a.onClose(func() { released = append(released, "redis") })
	a.onClose(func() { released = append(released, "billing") })
	a.close()

	if want := []string{"billing", "redis", "postgres"}; !reflect.DeepEqual(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
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

	for name, component := range map[string]any{
		"ops":      app.ops,
		"router":   app.router,
		"accepted": app.accepted,
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
