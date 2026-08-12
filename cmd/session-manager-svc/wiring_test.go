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
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// The wiring must fail as a VALUE, never as a process exit: a constructor that log.Fatals cannot be
// tested, and a boot failure that kills the process cannot be reported by the caller either.

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

func TestNewSessionManagerAppReportsAnUnreachableRedis(t *testing.T) {
	t.Parallel()

	app, err := newSessionManagerApp(t.Context(), testConfig(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newSessionManagerApp succeeded with an unreachable redis")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("error not attributed to redis: %v", err)
	}
}

// TestNewSessionManagerAppReleasesInDependencyOrder asserts the release order on the graph
// newSessionManagerApp actually builds. This service holds a single closer today, so the assertion
// names no ordering — it names the ONE step there is, and it breaks the day a second one is registered
// on the wrong side of it.
func TestNewSessionManagerAppReleasesInDependencyOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newSessionManagerApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newSessionManagerApp: %v", err)
	}

	want := []string{"stores"}
	if got := releaseOrder(app); !slices.Equal(got, want) {
		t.Errorf("release order is %v, want %v", got, want)
	}
}

// releaseOrder runs close() and reports the names in the order the closers ACTUALLY ran. It wraps the
// registered functions rather than reading the slice backwards: a test that reverses the slice itself
// would replay close()'s own loop, and an inverted loop would keep it green.
//
// It releases the app, so a caller must not close it a second time.
func releaseOrder(a *sessionManagerApp) []string {
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

func TestNewSessionManagerAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newSessionManagerApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newSessionManagerApp: %v", err)
	}
	defer app.close()

	for name, component := range map[string]any{"ops": app.ops, "grpc": app.grpc} {
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

// testConfig is a valid session-manager-svc configuration whose only external dependency points at a
// closed port.
func testConfig() config.Config {
	closed := "127.0.0.1:1"
	return config.Config{
		ServiceName:     serviceName,
		Environment:     config.EnvDevelopment,
		LogLevel:        "info",
		OpsPort:         freePort(),
		ShutdownTimeout: 5 * time.Second,
		Redis:           config.Redis{URL: "redis://" + closed, Timeout: 500 * time.Millisecond},
		GRPC:            config.GRPC{Port: freePort()},
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
