package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/configsecrets"
	configsecretspb "github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/realtime"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/grpctest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
	"github.com/martialanouman/go-gateway/internal/webhook"
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
	cfg, _ := tlsTestConfig(t)
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
	if app.http.TLSConfig == nil {
		t.Error("TLS_ENABLED is true and the wired server carries no TLS configuration")
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

// TestNewAdminAppAuditsAMutation: the booted service — not a test-built API — records a write in
// control_plane.audit_log, under the token's fingerprint. A trail that only exists in handler tests is a
// trail production does not have.
func TestNewAdminAppAuditsAMutation(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	pool := pgtest.Pool(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}
	defer app.close()

	var start time.Time
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&start); err != nil {
		t.Fatalf("read the clock: %v", err)
	}

	body := fmt.Sprintf(`{"name":"audit-%s"}`, uuid.NewString())
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/customers", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	app.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create customer = %d, want 201; body=%s", rec.Code, rec.Body)
	}

	var status *int16
	err = pool.QueryRow(ctx, `SELECT status FROM control_plane.audit_log
		WHERE operation_id = 'create-customer' AND operator = $1 AND at >= $2
		ORDER BY at DESC LIMIT 1`, auth.Fingerprint("test-token"), start).Scan(&status)
	if err != nil {
		t.Fatalf("no audit row for the booted service's write: %v", err)
	}
	if status == nil || *status != http.StatusCreated {
		t.Errorf("audit status = %v, want 201", status)
	}
}

func emptyHTTPDeps() (*stores, *runners, *controlPlaneClients, *realtimeFeed) {
	return &stores{ch: &clickhouse.Conn{}},
		&runners{},
		&controlPlaneClients{},
		&realtimeFeed{hub: realtime.NewHub(realtime.Config{}), quit: make(chan struct{})}
}

func tlsTestConfig(t *testing.T) (config.Config, *tlstest.CA) {
	t.Helper()
	ca := tlstest.NewCA(t)
	cert, key := ca.Issue(t, "admin-api-svc", "admin-api-svc")
	cfg := testConfig()
	cfg.TLS = config.TLS{Enabled: true, CertFile: cert, KeyFile: key, ClientCAFile: ca.CAFile}
	return cfg, ca
}

func adminHTTPServer(t *testing.T, cfg config.Config) *http.Server {
	t.Helper()
	st, runners, clients, feed := emptyHTTPDeps()
	srv, err := newHTTPServer(cfg, silentLogger(), st, nil, runners, clients, feed, nil)
	if err != nil {
		t.Fatalf("newHTTPServer: %v", err)
	}
	return srv
}

func mutualClient(t *testing.T, ca *tlstest.CA, present bool) *http.Client {
	t.Helper()
	caPEM, err := os.ReadFile(ca.CAFile)
	if err != nil {
		t.Fatalf("read the CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("the CA file holds no certificate")
	}
	conf := &tls.Config{
		RootCAs:    roots,
		ServerName: "admin-api-svc",
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"h2", "http/1.1"},
	}
	if present {
		certFile, keyFile := ca.Issue(t, "operator", "operator")
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatalf("load the client pair: %v", err)
		}
		conf.Certificates = []tls.Certificate{pair}
	}
	return &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: conf},
	}
}

func serveAdmin(t *testing.T, srv *http.Server, client *http.Client) {
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
}

func adminGet(client *http.Client, srv *http.Server, scheme string) (*http.Response, error) {
	url := scheme + "://127.0.0.1" + srv.Addr + "/nothing-here"
	var resp *http.Response
	var err error
	for range 50 {
		resp, err = client.Get(url)
		if err == nil || !strings.Contains(err.Error(), "connection refused") {
			return resp, err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return resp, err
}

func TestTheAdminAPIServesAPeerOfTheCAOverHTTP11(t *testing.T) {
	cfg, ca := tlsTestConfig(t)
	srv := adminHTTPServer(t, cfg)
	client := mutualClient(t, ca, true)
	serveAdmin(t, srv, client)

	resp, err := adminGet(client, srv, "https")
	if err != nil {
		t.Fatalf("a peer of our CA was refused: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.TLS == nil {
		t.Fatal("the Admin API answered in plaintext")
	}
	if got := resp.TLS.NegotiatedProtocol; got != "http/1.1" {
		t.Errorf("ALPN = %q, want http/1.1 — under h2 the realtime WebSocket cannot be hijacked", got)
	}
}

func TestTheAdminAPIRefusesAClientWithoutACertificate(t *testing.T) {
	cfg, ca := tlsTestConfig(t)
	srv := adminHTTPServer(t, cfg)
	control := mutualClient(t, ca, true)
	serveAdmin(t, srv, control)

	resp, err := adminGet(control, srv, "https")
	if err != nil {
		t.Fatalf("the control client was refused, so the refusal below would prove nothing: %v", err)
	}
	_ = resp.Body.Close()

	bare := mutualClient(t, ca, false)
	t.Cleanup(bare.CloseIdleConnections)
	resp, err = adminGet(bare, srv, "https")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("a client with no certificate reached the Admin API")
	}
}

func TestTheAdminAPIServesPlaintextWhenTLSIsOff(t *testing.T) {
	srv := adminHTTPServer(t, testConfig())
	if srv.TLSConfig != nil {
		t.Fatal("TLS_ENABLED is false and the server still carries a TLS configuration")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	serveAdmin(t, srv, client)

	resp, err := adminGet(client, srv, "http")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
}

func TestTheAdminAPIRefusesToBootOnAnUnreadableCertificate(t *testing.T) {
	cfg, _ := tlsTestConfig(t)
	cfg.TLS.CertFile = filepath.Join(t.TempDir(), "absent.crt")

	st, runners, clients, feed := emptyHTTPDeps()
	if _, err := newHTTPServer(cfg, silentLogger(), st, nil, runners, clients, feed, nil); err == nil {
		t.Fatal("a missing certificate booted: the failure must be a value, not a handshake at 3am")
	}
}

// TestNewAdminAppWiresTheSecretSealer proves, on the graph newAdminApp actually builds, that the Deps
// literal carries a SecretSealer. Since step-295 every connector and billing-provider write seals its
// secret first, and the handlers answer 500 "no secret sealer configured" without one — so a Deps that
// forgot the line boots, passes its probes, serves the rest of the Admin API, and fails only the four
// routes that write a secret, discovered at the first connector creation in production.
//
// The handler tests cannot see that hole: their harness supplies a sealer of its own. Only the booted
// service can, which is the same shape as TestNewAdminAppInvalidatesTheExactRouteCache above.
//
// ContentKey.Addr points at a closed port here, so the call fails either way — what is asserted is WHICH
// failure. "seal connector password" means the sealer is wired and could not reach the key service;
// "no secret sealer configured" means the wiring forgot it.
func TestNewAdminAppWiresTheSecretSealer(t *testing.T) {
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

	body := `{"name":"smsc-wiring","host":"h","port":2775,"bind_type":"trx","system_id":"s","password":"s3cr3t"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/connectors", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	app.http.Handler.ServeHTTP(w, req)

	// The positive half matters as much: asserting only an ABSENCE would stay green if the route vanished,
	// or if the message were renamed while the Deps line was dropped.
	if !strings.Contains(w.Body.String(), "seal connector password") {
		t.Errorf("want the failure of a WIRED sealer that cannot reach the key service, got %d: %s", w.Code, w.Body)
	}
	// The password must not come back in the failure either, whichever failure it is.
	if strings.Contains(w.Body.String(), "s3cr3t") {
		t.Errorf("the response echoes the password: %s", w.Body)
	}
}

// TestAConnectorPasswordWrittenByTheAdminAPIOpensAgainFromPostgres is the Definition of Done of step-295,
// end to end and in one test: written through the HTTP surface, read back from the column, and OPENED.
//
// It exists because the property was covered in two halves that never met. One half proved the handler
// hands the store something that opens, with an in-memory store. The other proved bytea preserves bytes,
// using a hand-written literal no KMS ever produced and nobody ever opened. Neither says that what the
// Admin API actually stores is recoverable, which is the one thing password_hash could not do and the
// whole reason this step exists.
//
// The key service is served here rather than injected: the Admin API must reach ConfigSecrets over its
// real gRPC client, through cfg.ContentKey.Addr, or the chain is not the chain.
func TestAConnectorPasswordWrittenByTheAdminAPIOpensAgainFromPostgres(t *testing.T) {
	master, err := content.GenerateDataKey()
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	kms, err := content.NewLocalKMS(master, "e2e/v1")
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}
	keySvc := grpc.NewServer()
	configsecretspb.RegisterConfigSecretsServer(keySvc, configsecrets.NewServer(kms))

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.ContentKey = config.ContentKey{Addr: grpctest.Serve(t, keySvc)}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}
	defer app.close()

	const password = "s3cr3t-e2e"
	name := "smsc-e2e-" + uuid.NewString()[:8]
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/connectors",
		strings.NewReader(`{"name":"`+name+`","host":"smsc.example","port":2775,"bind_type":"trx","system_id":"sys","password":"`+password+`"}`))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	app.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("create connector: status = %d; body=%s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), password) {
		t.Errorf("the 201 body echoes the password: %s", w.Body)
	}

	// Straight from the column, not from the handler's return value.
	var sealed []byte
	var keyRef string
	if err := pgtest.Pool(t).QueryRow(ctx,
		`SELECT password_sealed, password_kms_key_ref FROM control_plane.smsc_connectors WHERE name = $1`, name).
		Scan(&sealed, &keyRef); err != nil {
		t.Fatalf("read the stored row: %v", err)
	}
	if bytes.Contains(sealed, []byte(password)) {
		t.Fatal("the password sits in clear in password_sealed")
	}
	if keyRef != kms.KeyRef() {
		t.Errorf("password_kms_key_ref = %q, want %q", keyRef, kms.KeyRef())
	}

	// What connector-pool-svc will do the day it reads its bind password from the control plane.
	opened, err := configsecrets.NewServer(kms).Open(ctx, &configsecretspb.OpenRequest{Sealed: sealed})
	if err != nil {
		t.Fatalf("the stored password does not open — the column is as unusable as password_hash was: %v", err)
	}
	if string(opened.GetPlaintext()) != password {
		t.Errorf("the stored password opens to %q, want %q", opened.GetPlaintext(), password)
	}
}

func TestAWebhookSecretWrittenByTheAdminAPISignsADeliveryTheReceiverVerifies(t *testing.T) {
	master, err := content.GenerateDataKey()
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	kms, err := content.NewLocalKMS(master, "webhook-e2e/v1")
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}
	keySvc := grpc.NewServer()
	configsecretspb.RegisterConfigSecretsServer(keySvc, configsecrets.NewServer(kms))
	keyAddr := grpctest.Serve(t, keySvc)

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.ContentKey = config.ContentKey{Addr: keyAddr}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newAdminApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newAdminApp: %v", err)
	}
	defer app.close()

	pool := pgtest.Pool(t)
	var accountID uuid.UUID
	if err := pool.QueryRow(ctx, `
		WITH c AS (INSERT INTO control_plane.customers (name) VALUES ($1) RETURNING id)
		INSERT INTO control_plane.smpp_accounts (customer_id, name) SELECT id, $2 FROM c RETURNING id`,
		"webhook-e2e-"+uuid.NewString()[:8], "hook-app").Scan(&accountID); err != nil {
		t.Fatalf("seed account: %v", err)
	}

	const secret = "whsec-e2e-long-enough"
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/smpp-accounts/"+accountID.String()+"/webhooks",
		strings.NewReader(`{"event_type":"mo","url":"https://receiver.example/hook","secret":"`+secret+`"}`))
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	app.http.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("create webhook: status = %d; body=%s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), secret) {
		t.Errorf("the 201 body echoes the signing secret: %s", w.Body)
	}

	var sealed []byte
	var keyRef string
	if err := pool.QueryRow(ctx,
		`SELECT secret_sealed, secret_kms_key_ref FROM control_plane.webhooks WHERE account_id = $1`, accountID).
		Scan(&sealed, &keyRef); err != nil {
		t.Fatalf("read the stored row: %v", err)
	}
	if bytes.Contains(sealed, []byte(secret)) {
		t.Fatal("the signing secret sits in clear in secret_sealed")
	}
	if keyRef != kms.KeyRef() {
		t.Errorf("secret_kms_key_ref = %q, want %q", keyRef, kms.KeyRef())
	}

	var gotSig, gotTS string
	var gotBody []byte
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get(webhook.HeaderSignature)
		gotTS = r.Header.Get(webhook.HeaderTimestamp)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	keyConn, err := grpc.NewClient(keyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial key service: %v", err)
	}
	defer func() { _ = keyConn.Close() }()

	sender := webhook.NewSender(receiver.Client(), nil,
		modlrrouter.NewGRPCSecretOpener(configsecretspb.NewConfigSecretsClient(keyConn)), silentLogger())
	wh := cp.Webhook{
		ID: uuid.New(), AccountID: accountID, EventType: cp.WebhookEventMO, URL: receiver.URL,
		Secret: cp.SealedSecret{Sealed: sealed, KMSKeyRef: keyRef}, Status: cp.WebhookActive,
	}
	ev := webhook.Event{ID: "evt-e2e", Payload: []byte(`{"mo":"hello"}`)}
	if err := sender.Send(ctx, wh, ev); err != nil {
		t.Fatalf("Send: %v — the return path cannot deliver with the secret the Admin API stored", err)
	}
	// Recomputed here rather than through webhook.Sign: this is the DoD's "a receiver verifies it", and a
	// receiver implements the scheme, it does not call our function. Every other signature assertion in the
	// repo goes through Sign, so Sign agreeing with itself proves nothing about the wire format.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(gotTS + "."))
	mac.Write(gotBody)
	if want := "sha256=" + hex.EncodeToString(mac.Sum(nil)); gotSig != want {
		t.Errorf("signature = %q, want %q — a receiver holding the operator's secret would reject it", gotSig, want)
	}
}
