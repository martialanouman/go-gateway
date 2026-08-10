package main

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
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

func TestNewContentKeyAppReportsAnUnreachablePostgres(t *testing.T) {
	t.Parallel()

	app, err := newContentKeyApp(t.Context(), testConfig(), discardLogger())
	if err == nil {
		app.close()
		t.Fatal("newContentKeyApp succeeded with an unreachable postgres")
	}
	if !strings.Contains(err.Error(), "postgres") {
		t.Errorf("error not attributed to postgres: %v", err)
	}
}

func TestAppCloseReleasesInReverseOrderOfOpening(t *testing.T) {
	t.Parallel()

	// This is the invariant the deferred Closes in run() used to guarantee for free: a store must never
	// be released before something built on top of it.
	var released []string
	a := &contentKeyApp{}
	a.onClose(func() { released = append(released, "postgres") })
	a.onClose(func() { released = append(released, "kms") })
	a.close()

	if want := []string{"kms", "postgres"}; !reflect.DeepEqual(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
}

func TestNewContentKeyAppBuildsTheWholeGraph(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newContentKeyApp(ctx, cfg, discardLogger())
	if err != nil {
		t.Fatalf("newContentKeyApp: %v", err)
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

// testConfig is a valid content-key-svc configuration whose only external dependency points at a closed
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
