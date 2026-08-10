package config_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strconv"
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
	"POSTGRES_URL", "POSTGRES_MAX_CONNS", "POSTGRES_MIN_CONNS", "POSTGRES_TIMEOUT",
	"KAFKA_BROKERS", "KAFKA_TIMEOUT",
	"KAFKA_FETCH_MIN_BYTES", "KAFKA_FETCH_MAX_WAIT", "KAFKA_FETCH_MAX_BYTES",
	"KAFKA_FETCH_MAX_PARTITION_BYTES",
	"KAFKA_TOPIC_PARTITIONS", "KAFKA_TOPIC_PARTITIONS_OVERRIDES", "KAFKA_TOPIC_REPLICATION_FACTOR",
	"CLICKHOUSE_ADDR", "CLICKHOUSE_DATABASE", "CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_TIMEOUT",
	"CLICKHOUSE_MAX_OPEN_CONNS", "CLICKHOUSE_MAX_IDLE_CONNS",
	"CLICKHOUSE_CDR_RETENTION", "CLICKHOUSE_RETENTION_INTERVAL", "CLICKHOUSE_ARCHIVE_PREFIX",
	"HTTP_PORT", "HTTP_READ_HEADER_TIMEOUT", "HTTP_ADMIN_TOKENS", "HTTP_EXPORT_DIR",
	"REDIS_URL", "REDIS_TIMEOUT", "GRPC_PORT",
	"SMPP_PORT", "SMPP_SESSION_MANAGER_ADDR", "SMPP_POD_ID", "SMPP_IDLE_TIMEOUT",
	"SMPP_POD_ADDR_TEMPLATE", "SMPP_TRUSTED_PROXY_CIDRS",
	"SMPP_BIND_MAX_FAILURES", "SMPP_BIND_FAILURE_WINDOW", "SMPP_BIND_BACKOFF_BASE", "SMPP_BIND_BACKOFF_MAX",
	"SMPP_MAX_CONNS", "SMPP_QUERY_SM_RATE_PER_SEC", "SMPP_QUERY_SM_BURST", "SMPP_INBOUND_WINDOW",
	"BILLING_ADDR", "BILLING_RESERVE_TIMEOUT", "BILLING_SETTLE_TIMEOUT",
	"BILLING_REAPER_MIN_AGE", "BILLING_REAPER_INTERVAL",
	"CONTENT_KEY_ADDR",
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
	if cfg.SMPP.BindMaxFailures != 5 {
		t.Errorf("SMPP.BindMaxFailures = %d, want 5", cfg.SMPP.BindMaxFailures)
	}
	if cfg.SMPP.BindFailureWindow != 15*time.Minute {
		t.Errorf("SMPP.BindFailureWindow = %s, want 15m", cfg.SMPP.BindFailureWindow)
	}
	if cfg.SMPP.BindBackoffBase != time.Second {
		t.Errorf("SMPP.BindBackoffBase = %s, want 1s", cfg.SMPP.BindBackoffBase)
	}
	if cfg.SMPP.BindBackoffMax != 30*time.Second {
		t.Errorf("SMPP.BindBackoffMax = %s, want 30s", cfg.SMPP.BindBackoffMax)
	}
	if cfg.SMPP.MaxConns != 16384 {
		t.Errorf("SMPP.MaxConns = %d, want 16384", cfg.SMPP.MaxConns)
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
		"REDIS_URL":                   "redis://cache:6379",
		"REDIS_TIMEOUT":               "4s",
		"GRPC_PORT":                   "7100",
		"SMPP_PORT":                   "2825",
		"SMPP_SESSION_MANAGER_ADDR":   "sessionmgr:7000",
		"SMPP_POD_ID":                 "pod-7",
		"SMPP_IDLE_TIMEOUT":           "90s",
		"SMPP_BIND_MAX_FAILURES":      "8",
		"SMPP_BIND_FAILURE_WINDOW":    "20m",
		"SMPP_BIND_BACKOFF_BASE":      "2s",
		"SMPP_BIND_BACKOFF_MAX":       "1m",
		"SMPP_MAX_CONNS":              "9000",
		"BILLING_ADDR":                "billing:7001",
		"CONTENT_KEY_ADDR":            "content-key:7002",
		"BILLING_RESERVE_TIMEOUT":     "350ms",
		"BILLING_SETTLE_TIMEOUT":      "150ms",
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
	if cfg.Billing.Addr != "billing:7001" {
		t.Errorf("Billing.Addr = %q, want billing:7001", cfg.Billing.Addr)
	}
	if cfg.Billing.ReserveTimeout != 350*time.Millisecond {
		t.Errorf("Billing.ReserveTimeout = %s, want 350ms", cfg.Billing.ReserveTimeout)
	}
	if cfg.Billing.SettleTimeout != 150*time.Millisecond {
		t.Errorf("Billing.SettleTimeout = %s, want 150ms", cfg.Billing.SettleTimeout)
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
	if cfg.Redis.URL != "redis://cache:6379" {
		t.Errorf("Redis.URL = %q, want redis://cache:6379", cfg.Redis.URL)
	}
	if cfg.Redis.Timeout != 4*time.Second {
		t.Errorf("Redis.Timeout = %s, want 4s", cfg.Redis.Timeout)
	}
	if cfg.GRPC.Port != 7100 {
		t.Errorf("GRPC.Port = %d, want 7100", cfg.GRPC.Port)
	}
	if cfg.SMPP.Port != 2825 {
		t.Errorf("SMPP.Port = %d, want 2825", cfg.SMPP.Port)
	}
	if cfg.SMPP.SessionManagerAddr != "sessionmgr:7000" {
		t.Errorf("SMPP.SessionManagerAddr = %q, want sessionmgr:7000", cfg.SMPP.SessionManagerAddr)
	}
	if cfg.SMPP.PodID != "pod-7" {
		t.Errorf("SMPP.PodID = %q, want pod-7", cfg.SMPP.PodID)
	}
	if cfg.SMPP.IdleTimeout != 90*time.Second {
		t.Errorf("SMPP.IdleTimeout = %s, want 90s", cfg.SMPP.IdleTimeout)
	}
	if cfg.SMPP.BindMaxFailures != 8 {
		t.Errorf("SMPP.BindMaxFailures = %d, want 8", cfg.SMPP.BindMaxFailures)
	}
	if cfg.SMPP.BindFailureWindow != 20*time.Minute {
		t.Errorf("SMPP.BindFailureWindow = %s, want 20m", cfg.SMPP.BindFailureWindow)
	}
	if cfg.SMPP.BindBackoffBase != 2*time.Second {
		t.Errorf("SMPP.BindBackoffBase = %s, want 2s", cfg.SMPP.BindBackoffBase)
	}
	if cfg.SMPP.BindBackoffMax != time.Minute {
		t.Errorf("SMPP.BindBackoffMax = %s, want 1m", cfg.SMPP.BindBackoffMax)
	}
	if cfg.SMPP.MaxConns != 9000 {
		t.Errorf("SMPP.MaxConns = %d, want 9000", cfg.SMPP.MaxConns)
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
		// Capacity levers (step-201, D5). Every one of these would either be silently swallowed by the
		// library's own "unset" handling or refused by the client itself, at the first boot in production.
		{"kafka fetch min bytes zero", map[string]string{"KAFKA_FETCH_MIN_BYTES": "0"}, "KAFKA_FETCH_MIN_BYTES"},
		{"kafka fetch min bytes negative", map[string]string{"KAFKA_FETCH_MIN_BYTES": "-1"}, "KAFKA_FETCH_MIN_BYTES"},
		{"kafka fetch max bytes zero", map[string]string{"KAFKA_FETCH_MAX_BYTES": "0"}, "KAFKA_FETCH_MAX_BYTES"},
		{"kafka fetch max partition bytes zero", map[string]string{"KAFKA_FETCH_MAX_PARTITION_BYTES": "0"}, "KAFKA_FETCH_MAX_PARTITION_BYTES"},
		{
			"kafka fetch max partition bytes above the response cap",
			map[string]string{"KAFKA_FETCH_MAX_PARTITION_BYTES": "8388609", "KAFKA_FETCH_MAX_BYTES": "8388608"},
			"KAFKA_FETCH_MAX_PARTITION_BYTES",
		},
		{"kafka fetch min bytes above max", map[string]string{
			"KAFKA_FETCH_MIN_BYTES": "2097152",
			"KAFKA_FETCH_MAX_BYTES": "1048576",
		}, "KAFKA_FETCH_MIN_BYTES"},
		{"kafka fetch max wait zero", map[string]string{"KAFKA_FETCH_MAX_WAIT": "0s"}, "KAFKA_FETCH_MAX_WAIT"},
		{"kafka fetch max wait negative", map[string]string{"KAFKA_FETCH_MAX_WAIT": "-1s"}, "KAFKA_FETCH_MAX_WAIT"},
		{"kafka topic partitions zero", map[string]string{"KAFKA_TOPIC_PARTITIONS": "0"}, "KAFKA_TOPIC_PARTITIONS"},
		{"kafka topic partitions negative", map[string]string{"KAFKA_TOPIC_PARTITIONS": "-1"}, "KAFKA_TOPIC_PARTITIONS"},
		{"kafka replication factor zero", map[string]string{"KAFKA_TOPIC_REPLICATION_FACTOR": "0"}, "KAFKA_TOPIC_REPLICATION_FACTOR"},
		{"kafka replication factor broker default sentinel", map[string]string{
			"KAFKA_TOPIC_REPLICATION_FACTOR": "-1",
		}, "KAFKA_TOPIC_REPLICATION_FACTOR"},
		{"overrides malformed entry", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"overrides non-integer count", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=many",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"overrides zero count", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=0",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"overrides negative count", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=-4",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"overrides empty topic", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "=12",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"overrides empty entry", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=12,,mt.routed=24",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"overrides duplicate topic", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=12,mt.inbound=24",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"overrides count out of int32 range", map[string]string{
			"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=2147483648",
		}, "KAFKA_TOPIC_PARTITIONS_OVERRIDES"},
		{"clickhouse max open conns zero", map[string]string{"CLICKHOUSE_MAX_OPEN_CONNS": "0"}, "CLICKHOUSE_MAX_OPEN_CONNS"},
		{"clickhouse max open conns negative", map[string]string{"CLICKHOUSE_MAX_OPEN_CONNS": "-1"}, "CLICKHOUSE_MAX_OPEN_CONNS"},
		{"clickhouse max idle conns zero", map[string]string{"CLICKHOUSE_MAX_IDLE_CONNS": "0"}, "CLICKHOUSE_MAX_IDLE_CONNS"},
		{"clickhouse idle above open", map[string]string{
			"CLICKHOUSE_MAX_OPEN_CONNS": "4",
			"CLICKHOUSE_MAX_IDLE_CONNS": "8",
		}, "CLICKHOUSE_MAX_IDLE_CONNS"},
		{"postgres min conns negative", map[string]string{"POSTGRES_MIN_CONNS": "-1"}, "POSTGRES_MIN_CONNS"},
		{"postgres min conns above max", map[string]string{
			"POSTGRES_MAX_CONNS": "4",
			"POSTGRES_MIN_CONNS": "8",
		}, "POSTGRES_MIN_CONNS"},
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
		{"clickhouse localhost among nodes", map[string]string{
			"POSTGRES_URL":    "postgres://u:p@db:5432/gw",
			"KAFKA_BROKERS":   "k1:9092",
			"CLICKHOUSE_ADDR": "ch1:9000,localhost:9000",
		}, "CLICKHOUSE_ADDR"},
		{"redis defaulted", map[string]string{
			"POSTGRES_URL":    "postgres://u:p@db:5432/gw",
			"KAFKA_BROKERS":   "k1:9092",
			"CLICKHOUSE_ADDR": "ch1:9000",
		}, "REDIS_URL"},
		// The key service is a client dial target like the others: shipping production with the loopback
		// default would silently point content encryption at nothing (step-167).
		{"content key defaulted", map[string]string{
			"POSTGRES_URL":    "postgres://u:p@db:5432/gw",
			"KAFKA_BROKERS":   "k1:9092",
			"CLICKHOUSE_ADDR": "ch1:9000",
			"REDIS_URL":       "redis://redis:6379",
			"BILLING_ADDR":    "billing:7001",
		}, "CONTENT_KEY_ADDR"},
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
		"REDIS_URL":                   "redis://cache:6379",
		"SMPP_SESSION_MANAGER_ADDR":   "sessionmgr:7000",
		"BILLING_ADDR":                "billing:7001",
		"CONTENT_KEY_ADDR":            "content-key:7002",
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

// TestConfigLogValueReportsCapacityLevers: a capacity campaign has to be able to tell, from a pod's
// own boot log, which levers that pod actually got — a knob set in the wrong manifest is otherwise
// indistinguishable from a knob that did nothing. None of them is secret-bearing.
func TestConfigLogValueReportsCapacityLevers(t *testing.T) {
	setEnv(t, map[string]string{
		"KAFKA_FETCH_MIN_BYTES":            "65536",
		"KAFKA_FETCH_MAX_WAIT":             "250ms",
		"KAFKA_FETCH_MAX_BYTES":            "16777216",
		"KAFKA_FETCH_MAX_PARTITION_BYTES":  "131072",
		"KAFKA_TOPIC_PARTITIONS":           "24",
		"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=48",
		"KAFKA_TOPIC_REPLICATION_FACTOR":   "2",
		"CLICKHOUSE_MAX_OPEN_CONNS":        "40",
		"CLICKHOUSE_MAX_IDLE_CONNS":        "20",
		"POSTGRES_MIN_CONNS":               "8",
	})

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("starting", "config", cfg)

	for _, want := range []string{
		`"kafka_fetch_min_bytes":65536`,
		`"kafka_fetch_max_wait":250000000`, // slog renders a Duration as nanoseconds
		`"kafka_fetch_max_bytes":16777216`,
		`"kafka_fetch_max_partition_bytes":131072`,
		`"kafka_topic_partitions":24`,
		`"kafka_topic_partitions_overrides":"mt.inbound=48"`,
		`"kafka_topic_replication_factor":2`,
		`"clickhouse_max_open_conns":40`,
		`"clickhouse_max_idle_conns":20`,
		`"postgres_min_conns":8`,
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("boot log omits %s:\n%s", want, buf.String())
		}
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

// TestKnownVarsIsExhaustive keeps the hermetic-environment list honest. setEnv can only clear what
// knownVars names, so a variable added to Config but forgotten here lets the developer's own shell
// decide a test's result — a fixture that silently stops testing what it claims. Extra entries are
// allowed: SERVICE_NAME is listed on purpose, precisely because a test proves Config ignores it.
func TestKnownVarsIsExhaustive(t *testing.T) {
	known := make(map[string]bool, len(knownVars))
	for _, v := range knownVars {
		known[v] = true
	}

	var missing []string
	var walk func(t reflect.Type, prefix string)
	walk = func(rt reflect.Type, prefix string) {
		for i := range rt.NumField() {
			f := rt.Field(i)
			if f.Type.Kind() == reflect.Struct && f.Type.PkgPath() == rt.PkgPath() {
				walk(f.Type, prefix+f.Tag.Get("envPrefix"))
				continue
			}
			name := f.Tag.Get("env")
			if name == "" || name == "-" {
				continue
			}
			if !known[prefix+name] {
				missing = append(missing, prefix+name)
			}
		}
	}
	walk(reflect.TypeOf(config.Config{}), "")

	if len(missing) > 0 {
		t.Errorf("knownVars is missing %v; setEnv cannot clear what it does not name", missing)
	}
}

// TestCapacityLeverDefaults pins the neutrality of the capacity levers (step-201, D5): every default
// is the behaviour the gateway already had, so exposing the knobs changes nothing until an operator
// turns one. POSTGRES_MIN_CONNS is the single deliberate exception.
func TestCapacityLeverDefaults(t *testing.T) {
	setEnv(t, nil)

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// franz-go defaults, read in kgo/config.go:673-675 (v1.21.5).
	if cfg.Kafka.FetchMinBytes != 1 {
		t.Errorf("Kafka.FetchMinBytes = %d, want 1 (franz-go default)", cfg.Kafka.FetchMinBytes)
	}
	if cfg.Kafka.FetchMaxWait != 5*time.Second {
		t.Errorf("Kafka.FetchMaxWait = %s, want 5s (franz-go default)", cfg.Kafka.FetchMaxWait)
	}
	if cfg.Kafka.FetchMaxBytes != 50<<20 {
		t.Errorf("Kafka.FetchMaxBytes = %d, want %d (franz-go default 50MiB)", cfg.Kafka.FetchMaxBytes, 50<<20)
	}
	if cfg.Kafka.TopicPartitions != 12 {
		t.Errorf("Kafka.TopicPartitions = %d, want 12", cfg.Kafka.TopicPartitions)
	}
	if cfg.Kafka.TopicPartitionsOverrides != "" {
		t.Errorf("Kafka.TopicPartitionsOverrides = %q, want empty", cfg.Kafka.TopicPartitionsOverrides)
	}
	if cfg.Kafka.TopicReplicationFactor != 3 {
		t.Errorf("Kafka.TopicReplicationFactor = %d, want 3 (spec §2.5)", cfg.Kafka.TopicReplicationFactor)
	}
	// clickhouse-go defaults, read in clickhouse_options.go:412-417 (v2.47.0).
	if cfg.ClickHouse.MaxOpenConns != 10 {
		t.Errorf("ClickHouse.MaxOpenConns = %d, want 10 (lib default MaxIdleConns+5)", cfg.ClickHouse.MaxOpenConns)
	}
	if cfg.ClickHouse.MaxIdleConns != 5 {
		t.Errorf("ClickHouse.MaxIdleConns = %d, want 5 (lib default)", cfg.ClickHouse.MaxIdleConns)
	}
	// The one lever that deliberately changes behaviour: pgxpool defaults MinConns to 0
	// (pgxpool/pool.go:20), so nothing is pre-warmed and a peak pays a burst of dials.
	if cfg.Postgres.MinConns != 2 {
		t.Errorf("Postgres.MinConns = %d, want 2", cfg.Postgres.MinConns)
	}
}

func TestCapacityLeversFromEnvironment(t *testing.T) {
	setEnv(t, map[string]string{
		"KAFKA_FETCH_MIN_BYTES":            "65536",
		"KAFKA_FETCH_MAX_WAIT":             "250ms",
		"KAFKA_FETCH_MAX_BYTES":            "16777216",
		"KAFKA_TOPIC_PARTITIONS":           "24",
		"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound=48,mt.routed=48",
		"KAFKA_TOPIC_REPLICATION_FACTOR":   "2",
		"CLICKHOUSE_MAX_OPEN_CONNS":        "40",
		"CLICKHOUSE_MAX_IDLE_CONNS":        "20",
		"POSTGRES_MIN_CONNS":               "8",
	})

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Kafka.FetchMinBytes != 65536 {
		t.Errorf("Kafka.FetchMinBytes = %d, want 65536", cfg.Kafka.FetchMinBytes)
	}
	if cfg.Kafka.FetchMaxWait != 250*time.Millisecond {
		t.Errorf("Kafka.FetchMaxWait = %s, want 250ms", cfg.Kafka.FetchMaxWait)
	}
	if cfg.Kafka.FetchMaxBytes != 16<<20 {
		t.Errorf("Kafka.FetchMaxBytes = %d, want %d", cfg.Kafka.FetchMaxBytes, 16<<20)
	}
	if cfg.Kafka.TopicPartitions != 24 {
		t.Errorf("Kafka.TopicPartitions = %d, want 24", cfg.Kafka.TopicPartitions)
	}
	if cfg.Kafka.TopicReplicationFactor != 2 {
		t.Errorf("Kafka.TopicReplicationFactor = %d, want 2", cfg.Kafka.TopicReplicationFactor)
	}
	if cfg.ClickHouse.MaxOpenConns != 40 {
		t.Errorf("ClickHouse.MaxOpenConns = %d, want 40", cfg.ClickHouse.MaxOpenConns)
	}
	if cfg.ClickHouse.MaxIdleConns != 20 {
		t.Errorf("ClickHouse.MaxIdleConns = %d, want 20", cfg.ClickHouse.MaxIdleConns)
	}
	if cfg.Postgres.MinConns != 8 {
		t.Errorf("Postgres.MinConns = %d, want 8", cfg.Postgres.MinConns)
	}

	overrides, err := cfg.Kafka.PartitionOverrides()
	if err != nil {
		t.Fatalf("PartitionOverrides() error = %v", err)
	}
	want := map[string]int32{"mt.inbound": 48, "mt.routed": 48}
	if len(overrides) != len(want) {
		t.Fatalf("PartitionOverrides() = %v, want %v", overrides, want)
	}
	for topic, n := range want {
		if overrides[topic] != n {
			t.Errorf("PartitionOverrides()[%q] = %d, want %d", topic, overrides[topic], n)
		}
	}
}

// TestKafkaFetchMaxWaitFloor mirrors franz-go's own floor: FetchMaxWait is stored as int32
// milliseconds (kgo/config.go:1497) and a client whose maxWait is under 10ms refuses to start
// (kgo/config.go:373). The truncation is the subtle half — 10ms900µs is 10ms to franz-go, and 999µs
// is 0 — so the check must be on the truncated millisecond value, not on the duration.
func TestKafkaFetchMaxWaitFloor(t *testing.T) {
	tests := []struct {
		value      string
		wantAccept bool
	}{
		{"10ms", true},
		{"10ms900us", true}, // truncates to 10ms: franz-go accepts it, so we must too
		{"9ms", false},
		{"9ms999us", false}, // truncates to 9ms
		{"999us", false},    // truncates to 0
		{"5s", true},
	}

	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			setEnv(t, map[string]string{"KAFKA_FETCH_MAX_WAIT": tc.value})

			_, err := config.Load("router-svc")
			if tc.wantAccept {
				if err != nil {
					t.Fatalf("Load() with KAFKA_FETCH_MAX_WAIT=%s error = %v, want accepted", tc.value, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Load() accepted KAFKA_FETCH_MAX_WAIT=%s; franz-go would refuse the client", tc.value)
			}
			if !strings.Contains(err.Error(), "KAFKA_FETCH_MAX_WAIT") {
				t.Errorf("error %q should name KAFKA_FETCH_MAX_WAIT", err)
			}
		})
	}
}

// TestKafkaFetchMaxBytesCeiling pins the other franz-go refusal: BrokerMaxReadBytes defaults to 100MiB
// (kgo/config.go:646) and the client refuses to start when max fetch bytes exceeds it
// (kgo/config.go:331). We do not expose BrokerMaxReadBytes, so that default is the ceiling — and the
// boundary itself must be accepted, or the knob would be narrower than the library allows.
func TestKafkaFetchMaxBytesCeiling(t *testing.T) {
	const brokerMaxReadBytes = 100 << 20

	setEnv(t, map[string]string{"KAFKA_FETCH_MAX_BYTES": strconv.Itoa(brokerMaxReadBytes)})
	if _, err := config.Load("router-svc"); err != nil {
		t.Fatalf("Load() with KAFKA_FETCH_MAX_BYTES=%d error = %v, want accepted at the ceiling",
			brokerMaxReadBytes, err)
	}

	setEnv(t, map[string]string{"KAFKA_FETCH_MAX_BYTES": strconv.Itoa(brokerMaxReadBytes + 1)})
	_, err := config.Load("router-svc")
	if err == nil {
		t.Fatalf("Load() accepted KAFKA_FETCH_MAX_BYTES=%d; franz-go would refuse the client at boot",
			brokerMaxReadBytes+1)
	}
	if !strings.Contains(err.Error(), "KAFKA_FETCH_MAX_BYTES") {
		t.Errorf("error %q should name KAFKA_FETCH_MAX_BYTES", err)
	}
}

// TestPartitionOverridesReportsEveryBadEntry: the whole point of the list form is that one typo must
// not cost an operator a restart per entry, and must never fall back to the default width in silence.
func TestPartitionOverridesReportsEveryBadEntry(t *testing.T) {
	setEnv(t, map[string]string{
		"KAFKA_TOPIC_PARTITIONS_OVERRIDES": "mt.inbound,mt.routed=many,dlr.events=0",
	})

	_, err := config.Load("router-svc")
	if err == nil {
		t.Fatal("Load() accepted three malformed overrides")
	}
	for _, want := range []string{"mt.inbound", "mt.routed", "dlr.events"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q omits the bad entry %q; all problems should be reported at once", err, want)
		}
	}
}

// TestPartitionOverridesTolerateSpacing: a manifest wraps a long list, so surrounding blanks are part
// of the accepted form. Anything else is refused, not trimmed into a guess.
func TestPartitionOverridesTolerateSpacing(t *testing.T) {
	setEnv(t, map[string]string{
		"KAFKA_TOPIC_PARTITIONS_OVERRIDES": " mt.inbound = 48 ,  mt.routed=24 ",
	})

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	got, err := cfg.Kafka.PartitionOverrides()
	if err != nil {
		t.Fatalf("PartitionOverrides() error = %v", err)
	}
	if got["mt.inbound"] != 48 || got["mt.routed"] != 24 || len(got) != 2 {
		t.Errorf("PartitionOverrides() = %v, want map[mt.inbound:48 mt.routed:24]", got)
	}
}

// TestPartitionOverridesEmptyIsNotAnError: no overrides is the default posture — every topic takes
// KAFKA_TOPIC_PARTITIONS.
func TestPartitionOverridesEmptyIsNotAnError(t *testing.T) {
	setEnv(t, nil)

	cfg, err := config.Load("router-svc")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	got, err := cfg.Kafka.PartitionOverrides()
	if err != nil {
		t.Fatalf("PartitionOverrides() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("PartitionOverrides() = %v, want an empty map", got)
	}
}

// TestProductionRejectsSingleReplicaTopics is the same shape of guard as the loopback defaults: a
// value that is right on a one-broker laptop and a durability hole in production. With
// RequiredAcks(AllISRAcks), replication 1 makes a durable ACK — the thing a REST 202 and a
// submit_sm_resp rest on — a single broker's disk. The spec sizes the cluster for replication 3 (§2.5).
func TestProductionRejectsSingleReplicaTopics(t *testing.T) {
	setEnv(t, map[string]string{
		"ENVIRONMENT":                    "production",
		"OTEL_EXPORTER_OTLP_INSECURE":    "false",
		"POSTGRES_URL":                   "postgres://u:p@db:5432/gw",
		"KAFKA_BROKERS":                  "k1:9092",
		"CLICKHOUSE_ADDR":                "ch1:9000",
		"REDIS_URL":                      "redis://cache:6379",
		"SMPP_SESSION_MANAGER_ADDR":      "sessionmgr:7000",
		"BILLING_ADDR":                   "billing:7001",
		"CONTENT_KEY_ADDR":               "content-key:7002",
		"KAFKA_TOPIC_REPLICATION_FACTOR": "1",
	})

	_, err := config.Load("router-svc")
	if err == nil {
		t.Fatal("Load() accepted replication factor 1 in production")
	}
	if !strings.Contains(err.Error(), "KAFKA_TOPIC_REPLICATION_FACTOR") {
		t.Errorf("error %q should name KAFKA_TOPIC_REPLICATION_FACTOR", err)
	}

	// The same value is exactly right on a single-broker laptop or CI cluster.
	setEnv(t, map[string]string{"KAFKA_TOPIC_REPLICATION_FACTOR": "1"})
	if _, err := config.Load("router-svc"); err != nil {
		t.Fatalf("Load() error = %v; replication 1 must stay legal outside production", err)
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

// The duplication bound of ADR-0012 is a ratified commitment to operators: "at most ~250 subscribers
// per partition can receive the same SMS twice, per pod crash". The default is what makes it true, so
// a change to it is a change to that commitment and must not pass unnoticed.
//
// The value is NOT the ~1KiB-per-record arithmetic it looks like: max.partition.fetch.bytes bounds
// STORED bytes, and franz-go compresses with snappy by default. Measured on realistic mt.routed
// records — 621 bytes raw, 221 compressed, 2.81x — 56KiB is ~250 records. At 256KiB it was ~1187,
// which is how the first version of this lever promised a bound it did not hold.
func TestFetchMaxPartitionBytesHoldsTheRatifiedBound(t *testing.T) {
	setEnv(t, nil)
	cfg, err := config.Load("svc")
	if err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}

	const want = 56 << 10
	if cfg.Kafka.FetchMaxPartitionBytes != want {
		t.Errorf("FetchMaxPartitionBytes = %d, want %d: the ADR-0012 bound is ~250 messages per "+
			"partition per crash at the measured 221 compressed bytes per record",
			cfg.Kafka.FetchMaxPartitionBytes, want)
	}
}

// TestDefaultsIgnoresTheEnvironment: Defaults() must report the configuration as DECLARED, whatever the
// process environment says. Anything deriving a fixed baseline from it — the reference load run pins its
// Kafka client on it (step-201d) — would otherwise silently inherit whatever a developer exported.
func TestDefaultsIgnoresTheEnvironment(t *testing.T) {
	setEnv(t, map[string]string{
		"KAFKA_FETCH_MAX_PARTITION_BYTES": "999",
		"KAFKA_BROKERS":                   "elsewhere:9092",
		"POSTGRES_MAX_CONNS":              "99",
	})

	def := config.Defaults()

	if def.Kafka.FetchMaxPartitionBytes != 56<<10 {
		t.Errorf("Kafka.FetchMaxPartitionBytes = %d, want %d (declared default, not the environment's 999)",
			def.Kafka.FetchMaxPartitionBytes, 56<<10)
	}
	if got, want := def.Kafka.Brokers, []string{"localhost:9092"}; !slices.Equal(got, want) {
		t.Errorf("Kafka.Brokers = %v, want %v (declared default)", got, want)
	}
	if def.Postgres.MaxConns != 10 {
		t.Errorf("Postgres.MaxConns = %d, want 10 (declared default)", def.Postgres.MaxConns)
	}
}

// TestReaperSectionRefusesWhatWouldBreakBillingSvc covers the values step-193d was opened for. Both
// fields are billing-svc's own — its reaper's window and cadence — and until that step nothing validated
// them for any binary, because the only section carrying them was the one describing a CLIENT dial to
// billing-svc, which billing-svc has no reason to declare.
func TestReaperSectionRefusesWhatWouldBreakBillingSvc(t *testing.T) {
	cases := map[string]struct {
		vars map[string]string
		want string
	}{
		// runReap hands the interval straight to time.NewTicker, which panics on a non-positive
		// duration: the pod would die at boot, exactly as it did under the metric-label defect
		// step-193c found.
		"a zero sweep interval": {
			vars: map[string]string{"BILLING_REAPER_INTERVAL": "0"},
			want: "BILLING_REAPER_INTERVAL",
		},
		// billing.WithMinAge ignores a non-positive value and keeps its own 15m default. The knob would
		// therefore report a setting it does not have — the trap CLICKHOUSE_MAX_OPEN_CONNS refuses too.
		"a zero minimum age": {
			vars: map[string]string{"BILLING_REAPER_MIN_AGE": "0"},
			want: "BILLING_REAPER_MIN_AGE",
		},
		// The dangerous direction: a reaper sweeping messages seconds old races connector-pool's settle
		// loop and releases credit for SMS the SMSC actually took.
		"a minimum age under the floor": {
			vars: map[string]string{"BILLING_REAPER_MIN_AGE": "10s"},
			want: "BILLING_REAPER_MIN_AGE",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			setEnv(t, tc.vars)

			_, err := config.Load("billing-svc", config.SectionBillingReaper)
			if err == nil {
				t.Fatalf("Load accepted %v", tc.vars)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not name %s: %v", tc.want, err)
			}
		})
	}
}

// TestReaperSectionIsValidatedBySectionAll is what keeps SectionAll honest for this section. Its own
// godoc requires it to hold every section, "or Validate() would quietly stop being a full check", and a
// caller declaring nothing is precisely the case that relies on it.
func TestReaperSectionIsValidatedBySectionAll(t *testing.T) {
	setEnv(t, map[string]string{"BILLING_REAPER_INTERVAL": "-1s"})

	if _, err := config.Load("billing-svc"); err == nil {
		t.Fatal("Load with no declared section accepted a negative reaper interval: SectionAll is incomplete")
	}
}

// TestReaperSectionIsNotValidatedWhenUndeclared is the other half of the section contract (the migrate
// case): a binary that never runs a reaper must not be refused a boot over its settings.
func TestReaperSectionIsNotValidatedWhenUndeclared(t *testing.T) {
	setEnv(t, map[string]string{"BILLING_REAPER_INTERVAL": "0"})

	if _, err := config.Load("router-svc", config.SectionPostgres); err != nil {
		t.Errorf("Load refused a binary that declared no reaper section: %v", err)
	}
}

// TestReaperDefaultsAreUnchanged pins that extracting the fields into their own section moved no value:
// the defaults are still step-190's, and the variable names an operator sets are still BILLING_REAPER_*.
func TestReaperDefaultsAreUnchanged(t *testing.T) {
	setEnv(t, map[string]string{
		"BILLING_REAPER_MIN_AGE":  "20m",
		"BILLING_REAPER_INTERVAL": "90s",
	})

	cfg, err := config.Load("billing-svc", config.SectionBillingReaper)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.BillingReaper.MinAge, 20*time.Minute; got != want {
		t.Errorf("BillingReaper.MinAge = %s, want %s (BILLING_REAPER_MIN_AGE must keep its name)", got, want)
	}
	if got, want := cfg.BillingReaper.Interval, 90*time.Second; got != want {
		t.Errorf("BillingReaper.Interval = %s, want %s (BILLING_REAPER_INTERVAL must keep its name)", got, want)
	}

	def := config.Defaults()
	if got, want := def.BillingReaper.MinAge, 15*time.Minute; got != want {
		t.Errorf("declared MinAge default = %s, want %s (step-190's value)", got, want)
	}
	if got, want := def.BillingReaper.Interval, 5*time.Minute; got != want {
		t.Errorf("declared Interval default = %s, want %s (step-190's value)", got, want)
	}
}
