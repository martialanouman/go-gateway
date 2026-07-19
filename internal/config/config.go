// Package config loads and validates every service's configuration from the environment.
//
// Configuration is read once at startup (12-factor) and validated strictly: a service refuses to
// boot on invalid configuration rather than failing later, mid-traffic, on a value nobody checked
// (guide de codage §10). Load reports errors; the caller's main is the only place allowed to turn
// one into a log.Fatal.
//
// Secrets (SMSC passwords, API keys) arrive through the environment from a secret manager and are
// never committed. Config values are safe to log; secret-bearing fields must never be added here
// without a redacting type.
package config

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Environment names the deployment tier. It gates defaults that must differ between a laptop and
// production (debug logging, insecure OTLP).
type Environment string

// The deployment tiers.
const (
	EnvDevelopment Environment = "development"
	EnvStaging     Environment = "staging"
	EnvProduction  Environment = "production"
)

// Valid reports whether e is a known tier.
func (e Environment) Valid() bool {
	switch e {
	case EnvDevelopment, EnvStaging, EnvProduction:
		return true
	default:
		return false
	}
}

// IsProduction reports whether e is the production tier.
func (e Environment) IsProduction() bool { return e == EnvProduction }

// Config is the configuration shared by every service. Service-specific sections are added as
// their milestones land; a service ignores the sections it does not use.
type Config struct {
	// ServiceName identifies the binary in logs, traces and metrics. It comes from the binary
	// itself, not the environment: a router that can be told it is something else only creates
	// confusing telemetry.
	ServiceName string `env:"-"`

	Environment Environment `env:"ENVIRONMENT" envDefault:"development"`
	LogLevel    string      `env:"LOG_LEVEL" envDefault:"info"`

	// OpsPort serves /metrics, /healthz and /readyz. Internal only — never exposed publicly and
	// absent from the OpenAPI contracts (plan §1.4).
	OpsPort int `env:"OPS_PORT" envDefault:"9090"`

	// ShutdownTimeout bounds the graceful drain on SIGTERM. Keep it below the pod's
	// terminationGracePeriodSeconds, or the kubelet SIGKILLs mid-drain (guide de codage §5).
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"30s"`

	OTel       OTel       `envPrefix:"OTEL_"`
	Postgres   Postgres   `envPrefix:"POSTGRES_"`
	Kafka      Kafka      `envPrefix:"KAFKA_"`
	ClickHouse ClickHouse `envPrefix:"CLICKHOUSE_"`
	HTTP       HTTP       `envPrefix:"HTTP_"`
	Redis      Redis      `envPrefix:"REDIS_"`
	GRPC       GRPC       `envPrefix:"GRPC_"`
	SMPP       SMPP       `envPrefix:"SMPP_"`
}

// OTel configures tracing export. The variable names follow the OpenTelemetry specification so
// they match what an operator already knows and what the SDK itself reads.
type OTel struct {
	// Disabled turns tracing off entirely; the service then installs a no-op tracer and opens no
	// exporter connection.
	Disabled bool `env:"SDK_DISABLED" envDefault:"false"`

	// Endpoint is the OTLP/gRPC collector address ("host:port", no scheme).
	Endpoint string `env:"EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`

	// Insecure disables TLS to the collector. Acceptable on a laptop; rejected in production,
	// where traces carry identifiers that must not cross the network in clear.
	Insecure bool `env:"EXPORTER_OTLP_INSECURE" envDefault:"true"`

	// SampleRatio is the head-based sampling ratio for traces that are not errors. Errors,
	// rejections and timeouts are always sampled (spec §6.11), independently of this ratio.
	SampleRatio float64 `env:"TRACES_SAMPLER_ARG" envDefault:"1.0"`
}

// Development defaults. They match docker-compose.yml so a laptop needs no environment at all.
// Booting production on one of these would point a live service at a loopback address that does
// not exist there, so Validate rejects them on that tier.
const (
	// #nosec G101 -- not a credential: the throwaway localhost pair from docker-compose.yml.
	// Real deployments must override it, which Validate enforces on the production tier.
	defaultPostgresURL    = "postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"
	defaultKafkaBroker    = "localhost:9092"
	defaultClickHouseAddr = "localhost:9000"
	defaultRedisURL       = "redis://localhost:6379"

	// defaultSessionManagerAddr is the throwaway localhost target from docker-compose.yml. Like the
	// other loopback defaults, Validate refuses it on the production tier.
	defaultSessionManagerAddr = "localhost:7000"
)

// Postgres is the control-plane database (plan §1). It is also the target of the migration
// runner.
type Postgres struct {
	// URL is a libpq/pgx connection string. It carries the password, so it is never logged.
	URL string `env:"URL" envDefault:"postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"`

	MaxConns int32         `env:"MAX_CONNS" envDefault:"10"`
	Timeout  time.Duration `env:"TIMEOUT" envDefault:"5s"`
}

// Kafka is the durable data plane (plan §1.6). Reaching it is what readiness means for the
// pipeline services: with Kafka gone, a message cannot be durably accepted (plan §1.5).
type Kafka struct {
	Brokers []string `env:"BROKERS" envDefault:"localhost:9092" envSeparator:","`

	// Timeout bounds readiness probes and, later, client operations.
	Timeout time.Duration `env:"TIMEOUT" envDefault:"3s"`
}

// ClickHouse is the CDR / analytics sink (plan §1.10). It is a vital dependency of the services
// that read or write the CDR: rest-api-svc reads it for get-message and writes the accepted row,
// connector-pool-svc writes the enroute/rejected/failed row.
type ClickHouse struct {
	// Addr is the native-protocol endpoint list ("host:port", port 9000 by default).
	Addr []string `env:"ADDR" envDefault:"localhost:9000" envSeparator:","`

	Database string `env:"DATABASE" envDefault:"gateway"`
	Username string `env:"USERNAME" envDefault:"gateway"`
	// Password is secret-bearing and never logged.
	Password string `env:"PASSWORD" envDefault:"gateway"`

	// Timeout bounds readiness probes and dial/query operations.
	Timeout time.Duration `env:"TIMEOUT" envDefault:"5s"`
}

// HTTP configures a service's client-facing REST listener. Only the HTTP services declare
// SectionHTTP; the pipeline services carry the defaults and never validate them. The port is
// distinct from OpsPort on purpose: the business API is reached through an ingress, the ops port
// never is (plan §1.4 — admin-api-svc listens on 8081).
type HTTP struct {
	// Port is the client-facing listen port. admin-api-svc defaults to 8081 (plan §1.4).
	Port int `env:"PORT" envDefault:"8081"`

	// ReadHeaderTimeout bounds how long the server waits for request headers. Without it a
	// Slowloris client holds a connection open indefinitely; gosec (G112) refuses an http.Server
	// that leaves it unset.
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"5s"`

	// AdminTokens are the operator bearer tokens the M1 stand-in verifier accepts, as
	// "token:scope|scope" entries (internal/auth). They are secret-bearing and never logged. The
	// real identity provider (OIDC/mTLS) replaces them at M12.
	AdminTokens []string `env:"ADMIN_TOKENS" envSeparator:","`
}

// Redis is the operational-state store (plan §1): sessions, throttling, Bloom filters, the balance
// cache. It is a vital dependency of session-manager-svc, whose /readyz fails when Redis is
// unreachable (plan §1.5): the session registry cannot enforce max_sessions without it.
type Redis struct {
	// URL is a redis:// connection string. It may carry a password, so it is never logged.
	URL string `env:"URL" envDefault:"redis://localhost:6379"`

	// Timeout bounds readiness probes and dial/command operations.
	Timeout time.Duration `env:"TIMEOUT" envDefault:"3s"`
}

// GRPC configures a service's internal gRPC listener. The inter-pod services (session-manager-svc on
// :7000, plan §1.4) serve on it. Like HTTP.Port it is distinct from OpsPort on purpose: the RPC
// surface is reached pod-to-pod, the ops port never is.
type GRPC struct {
	// Port is the gRPC listen port. session-manager-svc defaults to 7000 (plan §1.4).
	Port int `env:"PORT" envDefault:"7000"`
}

// SMPP configures smpp-server-svc: its client-facing SMPP listener and the session-manager it calls
// to enforce max_sessions. Only smpp-server-svc declares SectionSMPP.
type SMPP struct {
	// Port is the SMPP listen port. smpp-server-svc defaults to 2775 (plan §1.4). It is distinct from
	// OpsPort on purpose: ESMEs reach the SMPP port through an ingress, the ops port never is.
	Port int `env:"PORT" envDefault:"2775"`

	// SessionManagerAddr is the host:port of the SessionRegistry gRPC service (session-manager-svc,
	// :7000). The bind path calls it to reserve and release a session token, the socle of invariant d.
	SessionManagerAddr string `env:"SESSION_MANAGER_ADDR" envDefault:"localhost:7000"`

	// PodID identifies this pod in the session registry, so a token can be traced to the pod owning
	// the connection and released when that pod drains. Empty falls back to the OS hostname at startup.
	PodID string `env:"POD_ID"`

	// IdleTimeout drops a bind whose peer has gone silent for this long, reclaiming the goroutine and
	// releasing its registry token. It stands in for the enquire_link keep-alive (deferred) and is
	// aligned with the registry's session TTL so a silent peer's slot is not held past the drop.
	IdleTimeout time.Duration `env:"IDLE_TIMEOUT" envDefault:"60s"`
}

// Section names a configuration group a binary depends on. A binary declares the sections it
// actually uses and Load enforces only those: a tool must never be refused a boot over a
// dependency it does not open a connection to. The migrate command is the case that forced this —
// it reads POSTGRES_URL and nothing else, yet a full validation blocked a production rollout over
// a defaulted KAFKA_BROKERS.
type Section uint8

// The configuration groups a binary can declare.
const (
	SectionOTel Section = 1 << iota
	SectionPostgres
	SectionKafka
	SectionClickHouse
	SectionHTTP
	SectionRedis
	SectionGRPC
	SectionSMPP

	// SectionAll is what a caller declaring nothing gets. It must include every section, or
	// Validate() — which runs validate(SectionAll) — would quietly stop being a full check. The
	// cost of a section a binary does not use is nil: its fields carry valid defaults.
	SectionAll = SectionOTel | SectionPostgres | SectionKafka | SectionClickHouse | SectionHTTP |
		SectionRedis | SectionGRPC | SectionSMPP
)

// Load reads the configuration for serviceName from the environment and validates the sections it
// declares. Every invalid value is reported at once: an operator fixing a bad deployment should see
// the whole list, not discover the next problem on the next restart.
//
// Declaring no section validates everything. That is the safe default: over-validating costs a boot
// failure an operator can read, while under-validating costs a dependency failing mid-traffic.
func Load(serviceName string, sections ...Section) (Config, error) {
	if strings.TrimSpace(serviceName) == "" {
		return Config{}, fmt.Errorf("load config: service name is empty")
	}

	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("load config for %s: %w", serviceName, err)
	}
	cfg.ServiceName = serviceName

	if err := cfg.validate(required(sections)); err != nil {
		return Config{}, fmt.Errorf("load config for %s: %w", serviceName, err)
	}
	return cfg, nil
}

// required folds the declared sections, defaulting to SectionAll when a caller declares none.
func required(sections []Section) Section {
	if len(sections) == 0 {
		return SectionAll
	}
	var s Section
	for _, section := range sections {
		s |= section
	}
	return s
}

// Validate checks every constraint the type system cannot, across every section, and returns all
// violations joined.
func (c Config) Validate() error {
	return c.validate(SectionAll)
}

// validate checks the fields every binary has, plus the declared sections.
func (c Config) validate(sections Section) error {
	problems := c.coreProblems()
	if sections&SectionOTel != 0 {
		problems = append(problems, c.otelProblems()...)
	}
	if sections&SectionPostgres != 0 {
		problems = append(problems, c.postgresProblems()...)
	}
	if sections&SectionKafka != 0 {
		problems = append(problems, c.kafkaProblems()...)
	}
	if sections&SectionClickHouse != 0 {
		problems = append(problems, c.clickhouseProblems()...)
	}
	if sections&SectionHTTP != 0 {
		problems = append(problems, c.httpProblems()...)
	}
	if sections&SectionRedis != 0 {
		problems = append(problems, c.redisProblems()...)
	}
	if sections&SectionGRPC != 0 {
		problems = append(problems, c.grpcProblems()...)
	}
	if sections&SectionSMPP != 0 {
		problems = append(problems, c.smppProblems()...)
	}

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
}

// coreProblems checks the fields every binary has, so they are never skipped: they carry valid
// defaults and cost nothing to verify.
func (c Config) coreProblems() []string {
	var problems []string

	if !c.Environment.Valid() {
		problems = append(problems, fmt.Sprintf(
			"ENVIRONMENT %q is not one of development, staging, production", c.Environment))
	}
	if _, err := ParseLogLevel(c.LogLevel); err != nil {
		problems = append(problems, fmt.Sprintf(
			"LOG_LEVEL %q is not one of debug, info, warn, error", c.LogLevel))
	}
	if c.OpsPort < 1 || c.OpsPort > 65535 {
		problems = append(problems, fmt.Sprintf("OPS_PORT %d is outside 1-65535", c.OpsPort))
	}
	if c.ShutdownTimeout <= 0 {
		problems = append(problems, fmt.Sprintf(
			"SHUTDOWN_TIMEOUT %s must be positive: a service that cannot drain loses in-flight work",
			c.ShutdownTimeout))
	}
	return problems
}

func (c Config) otelProblems() []string {
	if c.OTel.Disabled {
		return nil
	}

	var problems []string

	if strings.TrimSpace(c.OTel.Endpoint) == "" {
		problems = append(problems, "OTEL_EXPORTER_OTLP_ENDPOINT is empty while the SDK is enabled")
	}
	if strings.Contains(c.OTel.Endpoint, "://") {
		problems = append(problems, fmt.Sprintf(
			"OTEL_EXPORTER_OTLP_ENDPOINT %q must be host:port without a scheme", c.OTel.Endpoint))
	}
	if c.OTel.SampleRatio < 0 || c.OTel.SampleRatio > 1 {
		problems = append(problems, fmt.Sprintf(
			"OTEL_TRACES_SAMPLER_ARG %v is outside 0.0-1.0", c.OTel.SampleRatio))
	}
	if c.Environment.IsProduction() && c.OTel.Insecure {
		problems = append(problems, "OTEL_EXPORTER_OTLP_INSECURE must be false in production: "+
			"spans carry identifiers and must not cross the network in clear")
	}
	return problems
}

func (c Config) postgresProblems() []string {
	var problems []string

	if strings.TrimSpace(c.Postgres.URL) == "" {
		problems = append(problems, "POSTGRES_URL is empty")
	}
	if c.Postgres.MaxConns < 1 {
		problems = append(problems, fmt.Sprintf("POSTGRES_MAX_CONNS %d must be at least 1", c.Postgres.MaxConns))
	}
	if c.Postgres.Timeout <= 0 {
		problems = append(problems, fmt.Sprintf("POSTGRES_TIMEOUT %s must be positive", c.Postgres.Timeout))
	}
	// The dev-default guard travels with its section: a binary that declared Postgres is one that
	// will connect to it, and must not connect to loopback in production. See kafkaProblems for why
	// a forgotten variable reaches here as the default rather than as an error.
	if c.Environment.IsProduction() && c.Postgres.URL == defaultPostgresURL {
		problems = append(problems, "POSTGRES_URL is the localhost development default: "+
			"set it explicitly in production")
	}
	return problems
}

func (c Config) kafkaProblems() []string {
	var problems []string

	if len(c.Kafka.Brokers) == 0 {
		problems = append(problems, "KAFKA_BROKERS is empty")
	}
	for _, b := range c.Kafka.Brokers {
		if strings.TrimSpace(b) == "" {
			problems = append(problems, "KAFKA_BROKERS contains an empty entry")
			break
		}
	}
	if c.Kafka.Timeout <= 0 {
		problems = append(problems, fmt.Sprintf("KAFKA_TIMEOUT %s must be positive", c.Kafka.Timeout))
	}

	// caarlos0/env resolves a variable that is set-but-empty to its envDefault (env.go, getOr).
	// A blank KAFKA_BROKERS in a manifest therefore reads as "localhost:9092" rather than as an
	// error. On a laptop that is the intent; in production it would silently point a live service
	// at loopback, so the dev defaults are refused there and must be set explicitly.
	if c.Environment.IsProduction() &&
		len(c.Kafka.Brokers) == 1 && c.Kafka.Brokers[0] == defaultKafkaBroker {
		problems = append(problems, "KAFKA_BROKERS is the localhost development default: "+
			"set it explicitly in production")
	}
	return problems
}

func (c Config) clickhouseProblems() []string {
	var problems []string

	if len(c.ClickHouse.Addr) == 0 {
		problems = append(problems, "CLICKHOUSE_ADDR is empty")
	}
	for _, a := range c.ClickHouse.Addr {
		if strings.TrimSpace(a) == "" {
			problems = append(problems, "CLICKHOUSE_ADDR contains an empty entry")
			break
		}
	}
	if strings.TrimSpace(c.ClickHouse.Database) == "" {
		problems = append(problems, "CLICKHOUSE_DATABASE is empty")
	}
	if c.ClickHouse.Timeout <= 0 {
		problems = append(problems, fmt.Sprintf("CLICKHOUSE_TIMEOUT %s must be positive", c.ClickHouse.Timeout))
	}
	// The dev-default guard travels with its section, as for Postgres/Kafka: a service that declared
	// ClickHouse will connect to it, and must not connect to loopback in production. Addr is a list, so
	// a single loopback entry among several must still be rejected — otherwise a production node
	// silently reads/writes CDRs against a local store.
	if c.Environment.IsProduction() {
		for _, addr := range c.ClickHouse.Addr {
			if addr == defaultClickHouseAddr {
				problems = append(problems, "CLICKHOUSE_ADDR contains the localhost development default "+
					"("+defaultClickHouseAddr+"): set every node explicitly in production")
				break
			}
		}
	}
	return problems
}

func (c Config) httpProblems() []string {
	var problems []string

	if c.HTTP.Port < 1 || c.HTTP.Port > 65535 {
		problems = append(problems, fmt.Sprintf("HTTP_PORT %d is outside 1-65535", c.HTTP.Port))
	}
	// A business port equal to the ops port is not two servers sharing a listener — it is one
	// silently winning the bind and the other failing at boot. Catch it here, where the message is
	// readable, not at 3am from a probe that never answers.
	if c.HTTP.Port == c.OpsPort {
		problems = append(problems, fmt.Sprintf(
			"HTTP_PORT %d collides with OPS_PORT: the business API and the ops server need distinct ports",
			c.HTTP.Port))
	}
	if c.HTTP.ReadHeaderTimeout <= 0 {
		problems = append(problems, fmt.Sprintf(
			"HTTP_READ_HEADER_TIMEOUT %s must be positive: an unbounded header read is a Slowloris vector",
			c.HTTP.ReadHeaderTimeout))
	}
	// AdminTokens is deliberately NOT validated here. It is specific to admin-api-svc's stand-in
	// verifier, yet SectionHTTP is part of SectionAll and rest-api-svc also carries an HTTP section
	// without operator tokens. The "at least one usable token in production" policy therefore lives
	// in cmd/admin-api-svc (the point of use), not in this shared validator.
	return problems
}

func (c Config) redisProblems() []string {
	var problems []string

	if strings.TrimSpace(c.Redis.URL) == "" {
		problems = append(problems, "REDIS_URL is empty")
	}
	if c.Redis.Timeout <= 0 {
		problems = append(problems, fmt.Sprintf("REDIS_TIMEOUT %s must be positive", c.Redis.Timeout))
	}
	// The dev-default guard travels with its section, as for Postgres/Kafka/ClickHouse: a service that
	// declared Redis will connect to it, and must not connect to loopback in production. See
	// kafkaProblems for why a forgotten variable reaches here as the default rather than as an error.
	if c.Environment.IsProduction() && c.Redis.URL == defaultRedisURL {
		problems = append(problems, "REDIS_URL is the localhost development default: "+
			"set it explicitly in production")
	}
	return problems
}

func (c Config) grpcProblems() []string {
	var problems []string

	if c.GRPC.Port < 1 || c.GRPC.Port > 65535 {
		problems = append(problems, fmt.Sprintf("GRPC_PORT %d is outside 1-65535", c.GRPC.Port))
	}
	// As with HTTP.Port, a gRPC port equal to the ops port is not two servers sharing a listener — it
	// is one silently winning the bind and the other failing at boot. Catch it here, where the message
	// is readable, not at 3am from a probe that never answers.
	if c.GRPC.Port == c.OpsPort {
		problems = append(problems, fmt.Sprintf(
			"GRPC_PORT %d collides with OPS_PORT: the gRPC surface and the ops server need distinct ports",
			c.GRPC.Port))
	}
	return problems
}

func (c Config) smppProblems() []string {
	var problems []string

	if c.SMPP.Port < 1 || c.SMPP.Port > 65535 {
		problems = append(problems, fmt.Sprintf("SMPP_PORT %d is outside 1-65535", c.SMPP.Port))
	}
	// As with HTTP/GRPC, an SMPP port equal to the ops port is not two servers sharing a listener — it
	// is one silently winning the bind and the other failing at boot. Catch it here, readably.
	if c.SMPP.Port == c.OpsPort {
		problems = append(problems, fmt.Sprintf(
			"SMPP_PORT %d collides with OPS_PORT: the SMPP listener and the ops server need distinct ports",
			c.SMPP.Port))
	}
	if strings.TrimSpace(c.SMPP.SessionManagerAddr) == "" {
		problems = append(problems, "SMPP_SESSION_MANAGER_ADDR is empty: the bind path cannot enforce max_sessions")
	}
	if strings.Contains(c.SMPP.SessionManagerAddr, "://") {
		problems = append(problems, fmt.Sprintf(
			"SMPP_SESSION_MANAGER_ADDR %q must be host:port without a scheme", c.SMPP.SessionManagerAddr))
	}
	if c.SMPP.IdleTimeout <= 0 {
		problems = append(problems, fmt.Sprintf(
			"SMPP_IDLE_TIMEOUT %s must be positive: a bind with no idle drop leaks its registry token on a dead peer",
			c.SMPP.IdleTimeout))
	}
	// The dev-default guard travels with its section, as for Postgres/Redis: a service that declared
	// SMPP will call session-manager, and must not call loopback in production.
	if c.Environment.IsProduction() && c.SMPP.SessionManagerAddr == defaultSessionManagerAddr {
		problems = append(problems, "SMPP_SESSION_MANAGER_ADDR is the localhost development default: "+
			"set it explicitly in production")
	}
	return problems
}

// ParseLogLevel maps a configured level name to its slog level.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}

// LogValue renders the configuration for a startup log, omitting anything secret-bearing. The
// Postgres URL embeds a password and is reduced to a boolean: knowing it is set is enough to
// diagnose a boot, and printing it would put a credential in the log (guide de codage §10/§11).
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("service", c.ServiceName),
		slog.String("environment", string(c.Environment)),
		slog.String("log_level", c.LogLevel),
		slog.Int("ops_port", c.OpsPort),
		slog.Duration("shutdown_timeout", c.ShutdownTimeout),
		slog.Bool("otel_disabled", c.OTel.Disabled),
		slog.String("otel_endpoint", c.OTel.Endpoint),
		slog.Bool("postgres_url_set", strings.TrimSpace(c.Postgres.URL) != ""),
		slog.Any("kafka_brokers", c.Kafka.Brokers),
		slog.Any("clickhouse_addr", c.ClickHouse.Addr),
		slog.String("clickhouse_database", c.ClickHouse.Database),
		// The ClickHouse password is a secret: log only whether one is set.
		slog.Bool("clickhouse_password_set", c.ClickHouse.Password != ""),
		slog.Int("http_port", c.HTTP.Port),
		// The tokens themselves are secrets and never logged: only whether any are configured, which
		// is all a boot diagnosis needs.
		slog.Bool("http_admin_tokens_set", len(c.HTTP.AdminTokens) > 0),
		// The Redis URL can embed a password, so it is reduced to a boolean, as the Postgres URL is.
		slog.Bool("redis_url_set", strings.TrimSpace(c.Redis.URL) != ""),
		slog.Int("grpc_port", c.GRPC.Port),
		slog.Int("smpp_port", c.SMPP.Port),
		slog.String("smpp_session_manager_addr", c.SMPP.SessionManagerAddr),
		slog.String("smpp_pod_id", c.SMPP.PodID),
		slog.Duration("smpp_idle_timeout", c.SMPP.IdleTimeout),
	)
}
