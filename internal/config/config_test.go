package config_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
)

// knownVars is every variable Config reads. Tests clear them all so a developer's own shell
// cannot leak into a result.
var knownVars = []string{
	"ENVIRONMENT", "LOG_LEVEL", "OPS_PORT", "SHUTDOWN_TIMEOUT", "SERVICE_NAME",
	"OTEL_SDK_DISABLED", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_INSECURE",
	"OTEL_TRACES_SAMPLER_ARG",
	"POSTGRES_URL", "POSTGRES_MAX_CONNS", "POSTGRES_TIMEOUT",
	"KAFKA_BROKERS", "KAFKA_TIMEOUT",
	"CLICKHOUSE_ADDR", "CLICKHOUSE_DATABASE", "CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_TIMEOUT",
	"HTTP_PORT", "HTTP_READ_HEADER_TIMEOUT", "HTTP_ADMIN_TOKENS",
}

// setEnv installs a clean environment holding exactly kv. Each variable goes through t.Setenv
// first so the testing package registers its restore, then os.Unsetenv makes it genuinely absent
// — "set but empty" and "absent" are different inputs to env.Parse, and defaults only apply to
// the latter.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()

	for _, k := range knownVars {
		t.Setenv(k, "")
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
	}
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t, nil)

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ServiceName != "router-svc" {
		t.Errorf("ServiceName = %q, want router-svc", cfg.ServiceName)
	}
	if cfg.Environment != config.EnvDevelopment {
		t.Errorf("Environment = %q, want development", cfg.Environment)
	}
	if cfg.OpsPort != 9090 {
		t.Errorf("OpsPort = %d, want 9090 (plan §1.4)", cfg.OpsPort)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 30s", cfg.ShutdownTimeout)
	}
	if got, want := cfg.Kafka.Brokers, []string{"localhost:9092"}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("Kafka.Brokers = %v, want %v", got, want)
	}
	if cfg.HTTP.Port != 8081 {
		t.Errorf("HTTP.Port = %d, want 8081 (plan §1.4)", cfg.HTTP.Port)
	}
	if cfg.HTTP.ReadHeaderTimeout != 5*time.Second {
		t.Errorf("HTTP.ReadHeaderTimeout = %s, want 5s", cfg.HTTP.ReadHeaderTimeout)
	}
	if len(cfg.HTTP.AdminTokens) != 0 {
		t.Errorf("HTTP.AdminTokens = %v, want empty by default", cfg.HTTP.AdminTokens)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	setEnv(t, map[string]string{
		"ENVIRONMENT":                 "production",
		"LOG_LEVEL":                   "warn",
		"OPS_PORT":                    "9191",
		"SHUTDOWN_TIMEOUT":            "45s",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317",
		"OTEL_EXPORTER_OTLP_INSECURE": "false",
		"OTEL_TRACES_SAMPLER_ARG":     "0.25",
		"POSTGRES_URL":                "postgres://u:p@db:5432/gw",
		"POSTGRES_MAX_CONNS":          "42",
		"KAFKA_BROKERS":               "k1:9092,k2:9092,k3:9092",
		"KAFKA_TIMEOUT":               "7s",
		"CLICKHOUSE_ADDR":             "ch1:9000,ch2:9000",
		"HTTP_PORT":                   "8090",
		"HTTP_READ_HEADER_TIMEOUT":    "3s",
		"HTTP_ADMIN_TOKENS":           "tok-one:admin:read|admin:write,tok-two:admin:read",
	})

	cfg, err := config.Load("rest-api-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTP.Port != 8090 {
		t.Errorf("HTTP.Port = %d, want 8090", cfg.HTTP.Port)
	}
	if cfg.HTTP.ReadHeaderTimeout != 3*time.Second {
		t.Errorf("HTTP.ReadHeaderTimeout = %s, want 3s", cfg.HTTP.ReadHeaderTimeout)
	}
	if len(cfg.HTTP.AdminTokens) != 2 {
		t.Errorf("HTTP.AdminTokens = %v, want 2 entries", cfg.HTTP.AdminTokens)
	}

	if cfg.Environment != config.EnvProduction {
		t.Errorf("Environment = %q, want production", cfg.Environment)
	}
	if cfg.OpsPort != 9191 {
		t.Errorf("OpsPort = %d, want 9191", cfg.OpsPort)
	}
	if cfg.ShutdownTimeout != 45*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 45s", cfg.ShutdownTimeout)
	}
	if cfg.OTel.SampleRatio != 0.25 {
		t.Errorf("OTel.SampleRatio = %v, want 0.25", cfg.OTel.SampleRatio)
	}
	if cfg.Postgres.MaxConns != 42 {
		t.Errorf("Postgres.MaxConns = %d, want 42", cfg.Postgres.MaxConns)
	}
	if len(cfg.Kafka.Brokers) != 3 {
		t.Errorf("Kafka.Brokers = %v, want 3 entries", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Timeout != 7*time.Second {
		t.Errorf("Kafka.Timeout = %s, want 7s", cfg.Kafka.Timeout)
	}
	if len(cfg.ClickHouse.Addr) != 2 {
		t.Errorf("ClickHouse.Addr = %v, want 2 entries", cfg.ClickHouse.Addr)
	}
}

// TestLoadRejectsInvalid is the strict-boot contract: every one of these must stop the service at
// startup rather than surface later as a runtime surprise.
func TestLoadRejectsInvalid(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{"unknown environment", map[string]string{"ENVIRONMENT": "prod"}, "ENVIRONMENT"},
		{"unknown log level", map[string]string{"LOG_LEVEL": "verbose"}, "LOG_LEVEL"},
		{"ops port zero", map[string]string{"OPS_PORT": "0"}, "OPS_PORT"},
		{"ops port too high", map[string]string{"OPS_PORT": "70000"}, "OPS_PORT"},
		{"ops port negative", map[string]string{"OPS_PORT": "-1"}, "OPS_PORT"},
		// env.Parse reports the Go field name, not the variable name, for type errors.
		{"ops port not a number", map[string]string{"OPS_PORT": "nine thousand"}, "OpsPort"},
		{"shutdown timeout zero", map[string]string{"SHUTDOWN_TIMEOUT": "0s"}, "SHUTDOWN_TIMEOUT"},
		{"shutdown timeout negative", map[string]string{"SHUTDOWN_TIMEOUT": "-5s"}, "SHUTDOWN_TIMEOUT"},
		{"otlp endpoint with scheme", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4317"}, "OTEL_EXPORTER_OTLP_ENDPOINT"},
		{"otlp endpoint empty", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": " "}, "OTEL_EXPORTER_OTLP_ENDPOINT"},
		{"sample ratio above one", map[string]string{"OTEL_TRACES_SAMPLER_ARG": "1.5"}, "OTEL_TRACES_SAMPLER_ARG"},
		{"sample ratio negative", map[string]string{"OTEL_TRACES_SAMPLER_ARG": "-0.1"}, "OTEL_TRACES_SAMPLER_ARG"},
		{"postgres url empty", map[string]string{"POSTGRES_URL": " "}, "POSTGRES_URL"},
		{"postgres max conns zero", map[string]string{"POSTGRES_MAX_CONNS": "0"}, "POSTGRES_MAX_CONNS"},
		{"postgres timeout zero", map[string]string{"POSTGRES_TIMEOUT": "0s"}, "POSTGRES_TIMEOUT"},
		{"kafka brokers blank", map[string]string{"KAFKA_BROKERS": " "}, "KAFKA_BROKERS"},
		{"kafka brokers blank entry", map[string]string{"KAFKA_BROKERS": "k1:9092, "}, "KAFKA_BROKERS"},
		{"kafka timeout zero", map[string]string{"KAFKA_TIMEOUT": "0s"}, "KAFKA_TIMEOUT"},
		{"http port zero", map[string]string{"HTTP_PORT": "0"}, "HTTP_PORT"},
		{"http port too high", map[string]string{"HTTP_PORT": "70000"}, "HTTP_PORT"},
		{"http read header timeout zero", map[string]string{"HTTP_READ_HEADER_TIMEOUT": "0s"}, "HTTP_READ_HEADER_TIMEOUT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)

			_, err := config.Load("router-svc")
			if err == nil {
				t.Fatalf("Load() with %v succeeded, want a boot failure", tc.env)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error %q should name the offending variable %q", err, tc.wantMsg)
			}
		})
	}
}

// TestHTTPPortCollidingWithOpsPortIsRejected: the business API and the ops server are separate
// listeners on separate ports (plan §1.4). Equal ports mean one silently wins the bind and the
// other never comes up — a failure that must surface at boot, not from a probe that never answers.
func TestHTTPPortCollidingWithOpsPortIsRejected(t *testing.T) {
	setEnv(t, map[string]string{
		"OPS_PORT":  "8081",
		"HTTP_PORT": "8081",
	})

	_, err := config.Load("admin-api-svc")
	if err == nil {
		t.Fatal("Load() accepted an HTTP port equal to the ops port")
	}
	if !strings.Contains(err.Error(), "HTTP_PORT") || !strings.Contains(err.Error(), "OPS_PORT") {
		t.Errorf("error %q should name both HTTP_PORT and OPS_PORT", err)
	}
}

// TestProductionRejectsInsecureOTLP pins a security default: traces carry identifiers, so the
// laptop-friendly insecure exporter must not survive a promotion to production.
func TestProductionRejectsInsecureOTLP(t *testing.T) {
	setEnv(t, map[string]string{
		"ENVIRONMENT":                 "production",
		"OTEL_EXPORTER_OTLP_INSECURE": "true",
		"POSTGRES_URL":                "postgres://u:p@db:5432/gw",
		"KAFKA_BROKERS":               "k1:9092",
	})

	_, err := config.Load("router-svc")
	if err == nil {
		t.Fatal("Load() accepted insecure OTLP in production")
	}
	if !strings.Contains(err.Error(), "OTEL_EXPORTER_OTLP_INSECURE") {
		t.Errorf("error %q should name OTEL_EXPORTER_OTLP_INSECURE", err)
	}
}

// TestEmptyValueFallsBackToDefault documents caarlos0/env semantics we rely on and that surprise
// most readers: a variable that is SET but EMPTY resolves to its envDefault, not to "".
func TestEmptyValueFallsBackToDefault(t *testing.T) {
	setEnv(t, map[string]string{
		"KAFKA_BROKERS": "",
		"POSTGRES_URL":  "",
		"OPS_PORT":      "",
	})

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("Kafka.Brokers = %v, want the default to apply to an empty value", cfg.Kafka.Brokers)
	}
	if cfg.OpsPort != 9090 {
		t.Errorf("OpsPort = %d, want the default 9090 to apply to an empty value", cfg.OpsPort)
	}
}

// TestProductionRejectsLocalhostDefaults is the guard that makes the fallback above safe: an
// empty or forgotten variable must not quietly point production at loopback.
func TestProductionRejectsLocalhostDefaults(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{"postgres defaulted", map[string]string{"KAFKA_BROKERS": "k1:9092"}, "POSTGRES_URL"},
		{"kafka defaulted", map[string]string{"POSTGRES_URL": "postgres://u:p@db:5432/gw"}, "KAFKA_BROKERS"},
		{"kafka set empty", map[string]string{
			"POSTGRES_URL":  "postgres://u:p@db:5432/gw",
			"KAFKA_BROKERS": "",
		}, "KAFKA_BROKERS"},
		{"both defaulted", nil, "POSTGRES_URL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			envs := map[string]string{
				"ENVIRONMENT":                 "production",
				"OTEL_EXPORTER_OTLP_INSECURE": "false",
			}
			for k, v := range tc.env {
				envs[k] = v
			}
			setEnv(t, envs)

			_, err := config.Load("router-svc")
			if err == nil {
				t.Fatal("Load() accepted a localhost development default in production")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error %q should name %s", err, tc.wantMsg)
			}
		})
	}
}

// TestLoadValidatesOnlyDeclaredSections is the migrate tool's boot contract. A production
// migration Job sets POSTGRES_URL and nothing else, because that is all the migrator opens: it has
// no Kafka client and no OTLP exporter. Validating the whole Config there refused the boot over a
// defaulted KAFKA_BROKERS, blocking a rollout on a schema migration that never ran.
func TestLoadValidatesOnlyDeclaredSections(t *testing.T) {
	setEnv(t, map[string]string{
		"ENVIRONMENT":  "production",
		"POSTGRES_URL": "postgres://u:p@db:5432/gw",
	})

	cfg, err := config.Load("migrate", config.SectionPostgres)
	if err != nil {
		t.Fatalf("Load() error = %v; migrate declares Postgres only and must boot "+
			"without Kafka or OTel set", err)
	}
	if cfg.Postgres.URL != "postgres://u:p@db:5432/gw" {
		t.Errorf("Postgres.URL = %q, want the configured URL", cfg.Postgres.URL)
	}
}

// TestDeclaredSectionKeepsItsProductionGuard is the other half of the contract above: narrowing
// validation must not weaken the section a binary DID declare. Migrating a localhost database in
// production stays refused.
func TestDeclaredSectionKeepsItsProductionGuard(t *testing.T) {
	// POSTGRES_URL left at its localhost development default.
	setEnv(t, map[string]string{"ENVIRONMENT": "production"})

	_, err := config.Load("migrate", config.SectionPostgres)
	if err == nil {
		t.Fatal("Load() accepted the localhost Postgres default in production")
	}
	if !strings.Contains(err.Error(), "POSTGRES_URL") {
		t.Errorf("error %q should name POSTGRES_URL", err)
	}
}

// TestLoadWithoutSectionsValidatesEverything pins the default: a caller declaring nothing is
// validated in full. Over-validating is a boot failure an operator can read; under-validating is a
// dependency failing mid-traffic.
func TestLoadWithoutSectionsValidatesEverything(t *testing.T) {
	// KAFKA_BROKERS left at its localhost development default.
	setEnv(t, map[string]string{
		"ENVIRONMENT":                 "production",
		"OTEL_EXPORTER_OTLP_INSECURE": "false",
		"POSTGRES_URL":                "postgres://u:p@db:5432/gw",
	})

	_, err := config.Load("router-svc")
	if err == nil {
		t.Fatal("Load() with no declared sections accepted the localhost Kafka default in production")
	}
	if !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Errorf("error %q should name KAFKA_BROKERS", err)
	}
}

// TestDevelopmentAcceptsLocalhostDefaults is the other side: a laptop needs no environment.
func TestDevelopmentAcceptsLocalhostDefaults(t *testing.T) {
	setEnv(t, nil)

	if _, err := config.Load("router-svc"); err != nil {
		t.Fatalf("Load() error = %v; development must boot with no environment set", err)
	}
}

// TestDisabledOTelSkipsExporterValidation: with the SDK off, exporter settings are irrelevant and
// must not block a boot.
func TestDisabledOTelSkipsExporterValidation(t *testing.T) {
	setEnv(t, map[string]string{
		"ENVIRONMENT":                 "production",
		"OTEL_SDK_DISABLED":           "true",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "not a host:port",
		"OTEL_EXPORTER_OTLP_INSECURE": "true",
		"POSTGRES_URL":                "postgres://u:p@db:5432/gw",
		"KAFKA_BROKERS":               "k1:9092",
		"CLICKHOUSE_ADDR":             "ch1:9000",
	})

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v; exporter settings must not matter when the SDK is disabled", err)
	}
	if !cfg.OTel.Disabled {
		t.Error("OTel.Disabled = false, want true")
	}
}

// TestValidateReportsEveryProblem: an operator fixing a broken deployment should see the whole
// list at once, not one problem per restart.
func TestValidateReportsEveryProblem(t *testing.T) {
	setEnv(t, map[string]string{
		"ENVIRONMENT":  "nope",
		"LOG_LEVEL":    "loud",
		"OPS_PORT":     "99999",
		"POSTGRES_URL": " ",
	})

	_, err := config.Load("router-svc")
	if err == nil {
		t.Fatal("Load() succeeded on a thoroughly broken config")
	}

	for _, want := range []string{"ENVIRONMENT", "LOG_LEVEL", "OPS_PORT", "POSTGRES_URL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q omits %s; all problems should be reported at once", err, want)
		}
	}
}

func TestLoadRejectsEmptyServiceName(t *testing.T) {
	setEnv(t, nil)

	for _, name := range []string{"", "   "} {
		if _, err := config.Load(name); err == nil {
			t.Errorf("Load(%q) succeeded, want an error", name)
		}
	}
}

// TestServiceNameIsNotConfigurable: the binary knows what it is. Letting the environment rename it
// would only produce telemetry attributed to the wrong service.
func TestServiceNameIsNotConfigurable(t *testing.T) {
	setEnv(t, map[string]string{"SERVICE_NAME": "impostor-svc"})

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ServiceName != "router-svc" {
		t.Errorf("ServiceName = %q, want router-svc — SERVICE_NAME must not override the binary", cfg.ServiceName)
	}
}

// TestConfigLogValueHidesPostgresPassword: the Postgres URL embeds a credential. Logging the
// config at boot must not put it in the log (guide de codage §10/§11).
func TestConfigLogValueHidesPostgresPassword(t *testing.T) {
	const password = "sup3r-s3cret-canary"
	const adminToken = "operator-token-canary-abcdefgh"
	setEnv(t, map[string]string{
		"POSTGRES_URL":      "postgres://gateway:" + password + "@db:5432/gw",
		"HTTP_ADMIN_TOKENS": adminToken + ":admin:read|admin:write",
	})

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("starting", "config", cfg)

	if strings.Contains(buf.String(), password) {
		t.Fatalf("postgres password leaked into the boot log:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), adminToken) {
		t.Fatalf("admin token leaked into the boot log:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "postgres_url_set") {
		t.Errorf("boot log should still report whether the URL is set:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "http_admin_tokens_set") {
		t.Errorf("boot log should report whether admin tokens are set:\n%s", buf.String())
	}
	if !json.Valid(buf.Bytes()) {
		t.Errorf("boot log is not valid JSON:\n%s", buf.String())
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"debug", slog.LevelDebug, false},
		{"info", slog.LevelInfo, false},
		{"warn", slog.LevelWarn, false},
		{"warning", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"INFO", slog.LevelInfo, false},
		{"  info  ", slog.LevelInfo, false},
		{"trace", 0, true},
		{"", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := config.ParseLogLevel(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseLogLevel(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLogLevel(%q) error = %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseLogLevel(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestEnvironmentValid(t *testing.T) {
	tests := []struct {
		env  config.Environment
		want bool
	}{
		{config.EnvDevelopment, true},
		{config.EnvStaging, true},
		{config.EnvProduction, true},
		{"", false},
		{"prod", false},
		{"PRODUCTION", false},
	}

	for _, tc := range tests {
		t.Run(string(tc.env), func(t *testing.T) {
			if got := tc.env.Valid(); got != tc.want {
				t.Errorf("Environment(%q).Valid() = %v, want %v", tc.env, got, tc.want)
			}
		})
	}

	if !config.EnvProduction.IsProduction() {
		t.Error("EnvProduction.IsProduction() = false")
	}
	if config.EnvStaging.IsProduction() {
		t.Error("EnvStaging.IsProduction() = true")
	}
}
