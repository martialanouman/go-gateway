package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// The wiring must fail as a VALUE, never as a process exit: a constructor that log.Fatals cannot be
// tested, and a boot failure that kills the process cannot be reported by the caller either.

func TestOpenStoresRejectsAnUnparsableRedisURL(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Redis.URL = "redis://user:hunter2@:::/0"

	st, err := openStores(t.Context(), cfg)
	if err == nil {
		st.close()
		t.Fatal("openStores accepted an unparsable redis url")
	}
	if !strings.Contains(err.Error(), "connect redis") {
		t.Errorf("error not attributed to redis: %v", err)
	}
	// The URL may carry a password: it must never reach the error, which is logged.
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks the connection string: %v", err)
	}
}

func TestNewReturnPathAppReportsAnUnreachableRedis(t *testing.T) {
	t.Parallel()

	app, err := newReturnPathApp(t.Context(), testConfig(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newReturnPathApp succeeded with an unreachable redis")
	}
	if !strings.Contains(err.Error(), "connect redis") {
		t.Errorf("error not attributed to redis: %v", err)
	}
}

// TestNewReturnPathAppReleasesInDependencyOrder asserts the order on the graph newReturnPathApp
// actually builds, not on a stack a test pushed by hand: the webhook retry loop holds the MO leg's
// pool and the delivery leg's sender, and both legs take the stores.
func TestNewReturnPathAppReleasesInDependencyOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newReturnPathApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newReturnPathApp: %v", err)
	}

	want := []string{"retry", "delivery", "mo", "stores"}
	if got := releaseOrder(app); !slices.Equal(got, want) {
		t.Errorf("release order is %v, want %v", got, want)
	}
}

// releaseOrder runs close() and reports the names in the order the closers ACTUALLY ran. It wraps the
// registered functions rather than reading the slice backwards: a test that reverses the slice itself
// would replay close()'s own loop, and an inverted loop would keep it green.
//
// It releases the app, so a caller must not close it a second time.
func releaseOrder(a *returnPathApp) []string {
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

// TestNewReturnPathAppBuildsTheWholeGraph assembles the real service against test dependencies.
// Kafka, ClickHouse and session-manager are deliberately pointed at a closed port: none of them may
// be touched while the graph is being built, so a boot that reaches them is a regression.
func TestNewReturnPathAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newReturnPathApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newReturnPathApp: %v", err)
	}
	defer app.close()

	for name, component := range map[string]any{
		"ops":           app.ops,
		"dlr":           app.dlr,
		"mo":            app.mo,
		"mo delivery":   app.moDelivery,
		"dlr delivery":  app.dlrDelivery,
		"retryConsumer": app.retryConsumer,
		"retryRunner":   app.retryRunner,
	} {
		if component == nil || reflect.ValueOf(component).IsNil() {
			t.Errorf("component %q was not wired", name)
		}
	}

	// mo.routed and the retry topic are produced before the offset commits: the fail-closed constant
	// bound, never the env one (step-260e).
	if got := app.producer.DeliveryTimeout(); got != kafka.FailClosedProduceTimeout {
		t.Errorf("producer delivery timeout = %s, want the fail-closed constant %s", got, kafka.FailClosedProduceTimeout)
	}

	// Building the graph must not start serving: the ops port is bound by ops.Run, which only the
	// supervisor calls.
	if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.OpsPort)), time.Second); err == nil {
		_ = c.Close()
		t.Errorf("ops port %d is listening after wiring alone", cfg.OpsPort)
	}
}

// testConfig is a valid return-path configuration whose external dependencies all point at a closed
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
		ClickHouse:      config.ClickHouse{Addr: []string{closed}, Database: "gateway", Timeout: time.Second},
		Redis:           config.Redis{URL: "redis://" + closed, Timeout: 500 * time.Millisecond},
		SMPP:            config.SMPP{SessionManagerAddr: closed, PodAddrTemplate: "%s:7000"},
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
