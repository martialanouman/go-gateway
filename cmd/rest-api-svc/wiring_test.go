package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
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

func TestNewRestAPIAppReportsAnUnreachablePostgres(t *testing.T) {
	t.Parallel()

	app, err := newRestAPIApp(t.Context(), testConfig(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newRestAPIApp succeeded with an unreachable postgres")
	}
	if !strings.Contains(err.Error(), "postgres") {
		t.Errorf("error not attributed to postgres: %v", err)
	}
}

// TestNewRestAPIAppReleasesInDependencyOrder asserts the release order on the graph
// newRestAPIApp actually builds. This service holds a single closer today, so the assertion
// names no ordering — it names the ONE step there is, and it breaks the day a second one is registered
// on the wrong side of it.
func TestNewRestAPIAppReleasesInDependencyOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newRestAPIApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRestAPIApp: %v", err)
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
func releaseOrder(a *restAPIApp) []string {
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

// TestNewRestAPIAppBuildsTheWholeGraph assembles the real service against test dependencies. Kafka and
// ClickHouse are deliberately pointed at a closed port: neither may be touched while the graph is being
// built, so a boot that reaches them is a regression.
func TestNewRestAPIAppBuildsTheWholeGraph(t *testing.T) {
	cfg := tlsTestConfig(t)
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newRestAPIApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRestAPIApp: %v", err)
	}
	defer app.close()

	for name, component := range map[string]any{"ops": app.ops, "http": app.http} {
		if component == nil || reflect.ValueOf(component).IsNil() {
			t.Errorf("component %q was not wired", name)
		}
	}
	if app.http.TLSConfig == nil {
		t.Error("TLS_ENABLED is true and the wired server carries no TLS configuration")
	}

	// Building the graph must not start serving: both ports are bound by their Run, which only the
	// supervisor calls.
	for name, port := range map[string]int{"ops": cfg.OpsPort, "rest http": cfg.HTTP.Port} {
		if c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second); err == nil {
			_ = c.Close()
			t.Errorf("%s port %d is listening after wiring alone", name, port)
		}
	}
}

// testConfig is a valid rest-api-svc configuration whose external dependencies all point at a closed
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
		ClickHouse:      config.ClickHouse{Addr: []string{closed}, Database: "gateway", Timeout: time.Second},
		Kafka:           config.Kafka{Brokers: []string{closed}, Timeout: time.Second},
		Redis:           config.Redis{URL: "redis://" + closed, Timeout: 500 * time.Millisecond},
		HTTP:            config.HTTP{Port: freePort(), ReadHeaderTimeout: 5 * time.Second},
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

func emptyStores() *stores {
	return &stores{ch: &clickhouse.Conn{}}
}

func tlsTestConfig(t *testing.T) config.Config {
	t.Helper()
	ca := tlstest.NewCA(t)
	cert, key := ca.Issue(t, "rest-api-svc", "rest-api-svc")
	cfg := testConfig()
	cfg.TLS = config.TLS{Enabled: true, CertFile: cert, KeyFile: key, ClientCAFile: ca.CAFile}
	return cfg
}

func httpsClient(t *testing.T, caFile string) *http.Client {
	t.Helper()
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read the CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("the CA file holds no certificate")
	}
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			ForceAttemptHTTP2: true,
			TLSClientConfig: &tls.Config{
				RootCAs:    roots,
				ServerName: "rest-api-svc",
				MinVersion: tls.VersionTLS12,
				MaxVersion: tls.VersionTLS12,
				NextProtos: []string{"h2", "http/1.1"},
			},
		},
	}
}

func serveAndGet(t *testing.T, srv *http.Server, client *http.Client, scheme string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runHTTP(ctx, srv, time.Second, silentLogger()) }()
	t.Cleanup(func() {
		client.CloseIdleConnections()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("runHTTP: %v", err)
		}
	})

	url := scheme + "://127.0.0.1" + srv.Addr + "/nothing-here"
	var resp *http.Response
	var err error
	for range 50 {
		resp, err = client.Get(url)
		if err == nil {
			return resp
		}
		if !strings.Contains(err.Error(), "connection refused") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET %s: %v", url, err)
	return nil
}

func TestTheRestAPIServesHTTPSToAClientWithoutACertificate(t *testing.T) {
	cfg := tlsTestConfig(t)
	srv, err := newHTTPServer(cfg, emptyStores(), silentLogger())
	if err != nil {
		t.Fatalf("newHTTPServer: %v", err)
	}

	resp := serveAndGet(t, srv, httpsClient(t, cfg.TLS.ClientCAFile), "https")
	defer func() { _ = resp.Body.Close() }()

	if resp.TLS == nil {
		t.Fatal("the public API answered in plaintext")
	}
	if got := resp.TLS.NegotiatedProtocol; got != "http/1.1" {
		t.Errorf("ALPN = %q, want http/1.1 — the list never reached the handshake", got)
	}
}

func TestTheRestAPIServesPlaintextWhenTLSIsOff(t *testing.T) {
	srv, err := newHTTPServer(testConfig(), emptyStores(), silentLogger())
	if err != nil {
		t.Fatalf("newHTTPServer: %v", err)
	}
	if srv.TLSConfig != nil {
		t.Fatal("TLS_ENABLED is false and the server still carries a TLS configuration")
	}

	resp := serveAndGet(t, srv, &http.Client{Timeout: 5 * time.Second}, "http")
	defer func() { _ = resp.Body.Close() }()
}

func TestTheRestAPIRefusesToBootOnAnUnreadableIdentity(t *testing.T) {
	absent := filepath.Join(t.TempDir(), "absent.pem")
	for name, breakIt := range map[string]func(*config.TLS){
		"certificate": func(c *config.TLS) { c.CertFile = absent },
		"key":         func(c *config.TLS) { c.KeyFile = absent },
		"CA":          func(c *config.TLS) { c.ClientCAFile = absent },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := tlsTestConfig(t)
			breakIt(&cfg.TLS)
			if _, err := newHTTPServer(cfg, emptyStores(), silentLogger()); err == nil {
				t.Fatal("a missing file booted: the failure must be a value, not a handshake at 3am")
			}
		})
	}
}
