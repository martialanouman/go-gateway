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

func TestAppCloseReleasesInReverseOrderOfOpening(t *testing.T) {
	t.Parallel()

	// This is the invariant the deferred Closes in run() used to guarantee for free — and here it is
	// load-bearing beyond tidiness: the background runners' jobs use the Postgres pool, so their drain
	// must complete before the pool is closed.
	var released []string
	a := &adminApp{}
	a.onClose(func() { released = append(released, "stores") })
	a.onClose(func() { released = append(released, "runners") })
	a.onClose(func() { released = append(released, "clients") })
	a.close()

	if want := []string{"clients", "runners", "stores"}; !reflect.DeepEqual(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
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
