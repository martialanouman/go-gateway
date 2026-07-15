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

	OTel     OTel     `envPrefix:"OTEL_"`
	Postgres Postgres `envPrefix:"POSTGRES_"`
	Kafka    Kafka    `envPrefix:"KAFKA_"`
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
	defaultPostgresURL = "postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"
	defaultKafkaBroker = "localhost:9092"
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

	// SectionAll is what a pipeline service needs, and what a caller declaring nothing gets.
	SectionAll = SectionOTel | SectionPostgres | SectionKafka
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
	)
}
