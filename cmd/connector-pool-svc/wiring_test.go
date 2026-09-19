package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
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

// TestNewPoolAppReleasesInDependencyOrder asserts the order on the graph newPoolApp actually
// builds, not on a stack a test pushed by hand: newDrainer takes the stores, so the drainer must
// be released before them.
func TestNewPoolAppReleasesInDependencyOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newPoolApp(ctx, cfg, testBindEnv(), silentLogger())
	if err != nil {
		t.Fatalf("newPoolApp: %v", err)
	}

	want := []string{"drainer", "stream", "billing", "stores"}
	if got := releaseOrder(app); !slices.Equal(got, want) {
		t.Errorf("release order is %v, want %v", got, want)
	}
}

// releaseOrder runs close() and reports the names in the order the closers ACTUALLY ran. It wraps the
// registered functions rather than reading the slice backwards: a test that reverses the slice itself
// would replay close()'s own loop, and an inverted loop would keep it green.
//
// It releases the app, so a caller must not close it a second time.
func releaseOrder(a *poolApp) []string {
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

	// Fail-closed producer (mt.outcome after submit_sm): the constant, never the env (step-260e).
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

// TestValidateConnectorEnvRefusesTheDevelopmentPasswordInProduction: CONNECTOR_PASSWORD defaults to
// "gateway", the password of the in-repo fake SMSC. A production pod that kept it either fails its bind
// — loud — or binds successfully, and then the outbound leg is held by a secret published in this
// repository. The second case is the one this guard exists for.
func TestValidateConnectorEnvRefusesTheDevelopmentPasswordInProduction(t *testing.T) {
	defaults := connectorEnv{Addr: "smsc.operator.example:2775", SystemID: "gateway", Password: "gateway"}
	set := defaults
	set.Password = "a-real-secret"

	tests := []struct {
		name    string
		env     config.Environment
		bind    connectorEnv
		wantErr bool
	}{
		{"production keeps the default password", config.EnvProduction, defaults, true},
		{"production sets its own", config.EnvProduction, set, false},
		{"development keeps the default password", config.EnvDevelopment, defaults, false},
		{"staging keeps the default password", config.EnvStaging, defaults, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConnectorEnv(tt.bind, tt.env, config.TLS{})
			if tt.wantErr != (err != nil) {
				t.Fatalf("validateConnectorEnv() error = %v, want error: %v", err, tt.wantErr)
			}
			if err != nil {
				if !strings.Contains(err.Error(), "CONNECTOR_PASSWORD") {
					t.Errorf("error = %q, must name the variable an operator has to set", err)
				}
				if strings.Contains(err.Error(), defaults.Password) {
					t.Errorf("error = %q, must not echo the secret it refuses: it lands in the boot log", err)
				}
			}
		})
	}
}

// TestNewPoolAppRefusesTheDefaultPasswordInProduction proves the guard is wired, not merely written:
// the refusal must come from the construction path the binary uses. It also comes BEFORE any store is
// opened — testConfig() points at nothing reachable, so a guard placed after openStores would report a
// connection failure instead.
func TestNewPoolAppRefusesTheDefaultPasswordInProduction(t *testing.T) {
	cfg := testConfig()
	cfg.Environment = config.EnvProduction

	app, err := newPoolApp(t.Context(), cfg, testBindEnv(), silentLogger())
	if err == nil {
		app.close()
		t.Fatal("newPoolApp built a production pool bound with the development password")
	}
	if !strings.Contains(err.Error(), "CONNECTOR_PASSWORD") {
		t.Errorf("error = %v, want the bind-password refusal before any store is opened", err)
	}
}

// TestTheDeclaredDefaultIsTheOneTheGuardRefuses closes the only way the production guard can be disarmed
// without touching it: the `envDefault:"gateway"` tag and defaultConnectorPassword are two literals, and
// a struct tag cannot hold a constant. Change the tag alone and a production pod binds with the new
// default, refused by nothing.
//
// The environment is supplied empty rather than read from the process, so the test asserts what the tag
// declares and not what the developer's shell happens to export.
func TestTheDeclaredDefaultIsTheOneTheGuardRefuses(t *testing.T) {
	var parsed connectorEnv
	if err := env.ParseWithOptions(&parsed, env.Options{Environment: map[string]string{}}); err != nil {
		t.Fatalf("env.ParseWithOptions() error = %v", err)
	}
	if parsed.Password != defaultConnectorPassword {
		t.Fatalf("CONNECTOR_PASSWORD defaults to %q, but the production guard refuses %q: the guard is "+
			"disarmed", parsed.Password, defaultConnectorPassword)
	}
}

func TestTheOutboundTLSFlagRequiresThePodIdentity(t *testing.T) {
	bind := testBindEnv()
	bind.TLSEnabled = true

	err := validateConnectorEnv(bind, config.EnvDevelopment, config.TLS{Enabled: false})
	if err == nil {
		t.Fatal("CONNECTOR_TLS_ENABLED was accepted without TLS_ENABLED: the bind has no certificate to present")
	}
	if !strings.Contains(err.Error(), "CONNECTOR_TLS_ENABLED") || !strings.Contains(err.Error(), "TLS_ENABLED") {
		t.Errorf("error = %q, must name both variables an operator has to reconcile", err)
	}

	if err := validateConnectorEnv(bind, config.EnvDevelopment, config.TLS{Enabled: true}); err != nil {
		t.Errorf("both set and still refused: %v", err)
	}
}

func TestNewPoolAppRefusesAnUnreadableOutboundIdentity(t *testing.T) {
	ca := tlstest.NewCA(t)
	cert, key := ca.Issue(t, serviceName, serviceName)
	absent := filepath.Join(t.TempDir(), "absent.pem")

	for name, breakIt := range map[string]func(*config.TLS){
		"certificate": func(c *config.TLS) { c.CertFile = absent },
		"key":         func(c *config.TLS) { c.KeyFile = absent },
		"CA":          func(c *config.TLS) { c.ClientCAFile = absent },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			cfg.TLS = config.TLS{Enabled: true, CertFile: cert, KeyFile: key, ClientCAFile: ca.CAFile}
			breakIt(&cfg.TLS)
			bind := testBindEnv()
			bind.TLSEnabled = true

			// The error must be ATTRIBUTED, not merely present: every store in testConfig points at a
			// closed port, so a boot that reached them would fail anyway and a bare non-nil check would
			// pass on a TLS failure that was swallowed.
			_, err := newPoolApp(t.Context(), cfg, bind, silentLogger())
			if err == nil {
				t.Fatal("a missing file booted: the failure must be a value, not a handshake at 3am")
			}
			if !strings.Contains(err.Error(), "tlsconf") {
				t.Fatalf("boot error = %v, want it attributed to the unreadable identity", err)
			}
		})
	}
}

// TestTheWiredBindConfigCarriesTheTLSField guards the one mistake the tests above cannot see: a
// *tls.Config built at boot and never handed to the bind. Proving it by running the pool is not
// available — the consumer tears the bind down as soon as Kafka fails, so the observation races.
func TestTheWiredBindConfigCarriesTheTLSField(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "wiring.go", nil, 0)
	if err != nil {
		t.Fatalf("parse wiring.go: %v", err)
	}

	var seen []string
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "BindConfig" {
			return true
		}
		for _, e := range lit.Elts {
			kv, ok := e.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if k, ok := kv.Key.(*ast.Ident); ok {
				seen = append(seen, k.Name)
			}
		}
		return true
	})

	if len(seen) == 0 {
		t.Fatal("no connectorpool.BindConfig literal found in wiring.go: this guard is watching nothing")
	}
	if !slices.Contains(seen, "TLS") {
		t.Errorf("the wired BindConfig names %v: without TLS the config is built at boot and never dialled with", seen)
	}
}
