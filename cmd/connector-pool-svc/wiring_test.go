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

	"github.com/google/uuid"

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

	st, err := openStores(t.Context(), cfg, uuid.New())
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

func TestNewPoolAppReportsAnUnreachableRedis(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t) // Postgres opens first; Redis is the one left unreachable

	app, err := newPoolApp(t.Context(), cfg, testBindEnv(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newPoolApp succeeded with an unreachable redis")
	}
	if !strings.Contains(err.Error(), "connect redis") {
		t.Errorf("error not attributed to redis: %v", err)
	}
}

func TestAppCloseReleasesInReverseOrderOfOpening(t *testing.T) {
	t.Parallel()

	// This is the invariant the deferred Closes in run() used to guarantee for free: a store must
	// never be released before something built on top of it.
	var released []string
	a := &poolApp{}
	a.onClose(func() { released = append(released, "stores") })
	a.onClose(func() { released = append(released, "billing") })
	a.onClose(func() { released = append(released, "drainer") })
	a.close()

	if want := []string{"drainer", "billing", "stores"}; !reflect.DeepEqual(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
}

// TestNewPoolAppBuildsTheWholeGraph assembles the real service against test dependencies. Kafka,
// ClickHouse, billing-svc and the SMSC itself are deliberately pointed at a closed port: none of them
// may be touched while the graph is being built — the bind is dialled by Run, not by the wiring — so
// a boot that reaches them is a regression.
func TestNewPoolAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newPoolApp(ctx, cfg, testBindEnv(), silentLogger())
	if err != nil {
		t.Fatalf("newPoolApp: %v", err)
	}
	defer app.close()

	for name, component := range map[string]any{
		"ops":      app.ops,
		"pool":     app.pool,
		"drainer":  app.drainer,
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

// testConfig is a valid connector-pool configuration whose external dependencies all point at a
// closed port.
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
		Billing:         config.Billing{Addr: closed, SettleTimeout: 200 * time.Millisecond},
		OTel:            config.OTel{Disabled: true},
	}
}

// testBindEnv mirrors the env defaults, pointed at a port no SMSC answers on.
func testBindEnv() connectorEnv {
	return connectorEnv{
		Addr:                 "127.0.0.1:1",
		SystemID:             "gateway",
		Password:             "gateway",
		ID:                   uuid.New(),
		DialTimeout:          time.Second,
		ResponseTimeout:      time.Second,
		EnquireLinkInterval:  30 * time.Second,
		EnquireLinkMaxMissed: 3,
		WindowSize:           10,
		BindPoolSize:         1,
	}
}

// freePort returns a port no server is bound to, so a test can assert that nothing started listening
// on it. It falls back to a fixed high port if the probe fails, which only weakens the assertion.
func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 59091
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
