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

func TestNewConfigSyncAppReportsAnUnreachableRedis(t *testing.T) {
	t.Parallel()

	app, err := newConfigSyncApp(t.Context(), testConfig(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newConfigSyncApp succeeded with an unreachable redis")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("error not attributed to redis: %v", err)
	}
}

func TestAppCloseReleasesInReverseOrderOfOpening(t *testing.T) {
	t.Parallel()

	// This is the invariant the deferred Closes in run() used to guarantee for free: a store must never
	// be released before something built on top of it. The markers are synthetic — this service holds a
	// single closer today — so they name positions, not components: a label like "relay" would claim a
	// release that does not happen (the watcher has nothing to close).
	var released []string
	a := &configSyncApp{}
	a.onClose(func() { released = append(released, "opened first") })
	a.onClose(func() { released = append(released, "opened second") })
	a.close()

	if want := []string{"opened second", "opened first"}; !reflect.DeepEqual(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
}

// config-sync has no business listener at all — the ops port is the only one it can wrongly bind.
func TestNewConfigSyncAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newConfigSyncApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newConfigSyncApp: %v", err)
	}
	defer app.close()

	for name, component := range map[string]any{"ops": app.ops, "relay": app.relay} {
		if component == nil || reflect.ValueOf(component).IsNil() {
			t.Errorf("component %q was not wired", name)
		}
	}

	if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.OpsPort)), time.Second); err == nil {
		_ = c.Close()
		t.Errorf("ops port %d is listening after wiring alone", cfg.OpsPort)
	}
}

// testConfig is a valid config-sync configuration whose only external dependency points at a closed
// port.
func testConfig() config.Config {
	return config.Config{
		ServiceName:     serviceName,
		Environment:     config.EnvDevelopment,
		LogLevel:        "info",
		OpsPort:         freePort(),
		ShutdownTimeout: 5 * time.Second,
		Redis:           config.Redis{URL: "redis://127.0.0.1:1", Timeout: 500 * time.Millisecond},
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
