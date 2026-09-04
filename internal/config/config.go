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
	"strconv"
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

	// ShutdownTimeout bounds the graceful drain on SIGTERM, once the components start tearing down.
	// Keep DrainDelay + ShutdownTimeout below the pod's terminationGracePeriodSeconds, or the kubelet
	// SIGKILLs mid-drain (guide de codage §5).
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"30s"`

	// DrainDelay is how long a pod keeps serving AFTER marking itself not-ready on /readyz and BEFORE
	// its components tear down. It buys the load balancer time to observe the change and stop routing;
	// without it a rolling deploy hands new work to a pod that is already closing its listeners, which
	// for smpp-server-svc means cutting binds it just accepted (plan §16). It is dead time on every
	// shutdown, so it should be a few seconds, not tens. Zero disables the wait — correct for a
	// service with no load balancer in front of it, and for tests.
	DrainDelay time.Duration `env:"DRAIN_DELAY" envDefault:"5s"`

	OTel       OTel       `envPrefix:"OTEL_"`
	Postgres   Postgres   `envPrefix:"POSTGRES_"`
	Kafka      Kafka      `envPrefix:"KAFKA_"`
	ClickHouse ClickHouse `envPrefix:"CLICKHOUSE_"`
	HTTP       HTTP       `envPrefix:"HTTP_"`
	Redis      Redis      `envPrefix:"REDIS_"`
	GRPC       GRPC       `envPrefix:"GRPC_"`
	SMPP       SMPP       `envPrefix:"SMPP_"`
	Billing    Billing    `envPrefix:"BILLING_"`
	ContentKey ContentKey `envPrefix:"CONTENT_KEY_"`

	// BillingReaper sits under the BILLING_REAPER_ prefix, inside Billing's own BILLING_ space. The
	// nesting is deliberate: the variables an operator sets keep the names step-190 gave them, while
	// the two roles stop sharing a section.
	BillingReaper BillingReaper `envPrefix:"BILLING_REAPER_"`
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

	// defaultBillingAddr is the throwaway localhost target for billing-svc (:7001). Like the other loopback
	// defaults, Validate refuses it on the production tier.
	defaultBillingAddr = "localhost:7001"

	// defaultContentKeyAddr is the throwaway localhost target for content-key-svc (:7002). Like the other
	// loopback defaults, Validate refuses it on the production tier.
	defaultContentKeyAddr = "localhost:7002"
)

// franz-go client-startup refusals, mirrored so a bad capacity lever stops a deployment at config
// load instead of crash-looping a pod. Both values were read in the pinned module,
// github.com/twmb/franz-go v1.21.5; a version bump should re-read them.
const (
	// kafkaBrokerMaxReadBytes is franz-go's BrokerMaxReadBytes default (kgo/config.go:646). A client
	// whose max fetch bytes exceeds it refuses to start (kgo/config.go:331). The option is not exposed,
	// so this default is the ceiling on Kafka.FetchMaxBytes.
	kafkaBrokerMaxReadBytes = 100 << 20

	// kafkaMinFetchMaxWaitMillis is franz-go's floor on the fetch max wait (kgo/config.go:373), expressed
	// in the whole milliseconds the client stores after truncating the duration (kgo/config.go:1497).
	kafkaMinFetchMaxWaitMillis = 10

	// kafkaMinProduceTimeout is franz-go's floor on a non-zero record delivery timeout
	// (kgo/config.go:363-368); zero is refused here on top, since it would mean no bound.
	kafkaMinProduceTimeout = time.Second
)

// minReaperMinAge is the floor under BillingReaper.MinAge. It is a guard against catastrophe, not a
// policy: a settle lands milliseconds after the SMSC responds, so a minute is orders of magnitude above
// any nominal time in flight, and fifteen times under the default. It refuses the values that would make
// the reaper refund sent messages (1s, 10s, 30s) without constraining an operator who has measured the
// real window — the direction the godoc invites is to WIDEN it.
const minReaperMinAge = time.Minute

// Postgres is the control-plane database (plan §1). It is also the target of the migration
// runner.
type Postgres struct {
	// URL is a libpq/pgx connection string. It carries the password, so it is never logged.
	URL string `env:"URL" envDefault:"postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"`

	MaxConns int32         `env:"MAX_CONNS" envDefault:"10"`
	Timeout  time.Duration `env:"TIMEOUT" envDefault:"5s"`

	// MinConns is the number of connections the pool keeps warm. pgxpool defaults it to 0
	// (pgxpool/pool.go:20 in pgx v5.10.0), so a pod that has been idle answers a traffic peak by
	// dialling and authenticating from scratch, several connections at once — the latency spike lands
	// exactly when the load does. Pre-warming a couple costs two idle sessions per pod.
	//
	// It is capacity, not correctness: 0 is accepted and restores pgxpool's own behaviour.
	MinConns int32 `env:"MIN_CONNS" envDefault:"2"`
}

// Kafka is the durable data plane (plan §1.6). Reaching it is what readiness means for the
// pipeline services: with Kafka gone, a message cannot be durably accepted (plan §1.5).
type Kafka struct {
	Brokers []string `env:"BROKERS" envDefault:"localhost:9092" envSeparator:","`

	// Timeout bounds readiness probes and each broker dial (kgo.DialTimeout). It is not a produce or
	// fetch deadline: produces are bounded by ProduceTimeout, fetches by the caller's context.
	Timeout time.Duration `env:"TIMEOUT" envDefault:"3s"`

	// ProduceTimeout bounds a record's delivery on a fault (kgo RecordDeliveryTimeout, unbounded by
	// default; step-260e). It governs the ingress producers only (rest-api, smpp-server): under the SMPP
	// session's 15s, so the client is told why. The fail-closed producers ignore it for
	// kafka.FailClosedProduceTimeout — an expired record there is a redelivery, and a duplicate SMS.
	ProduceTimeout time.Duration `env:"PRODUCE_TIMEOUT" envDefault:"5s"`

	// FetchMinBytes is how many bytes a broker waits to accumulate before answering a fetch, unless
	// FetchMaxWait fires first. It is the batch-size lever of the ClickHouse CDR writer (step-201, D8):
	// the batch is exactly what one poll returned, so raising this raises the insert size. The default
	// is franz-go's own (kgo/config.go:674): 1, meaning "answer as soon as anything is there".
	FetchMinBytes int32 `env:"FETCH_MIN_BYTES" envDefault:"1"`

	// FetchMaxWait bounds how long a broker holds a fetch open waiting for FetchMinBytes. It is
	// therefore the latency ceiling of a batch at low traffic. The default is franz-go's resolved
	// default for a non-share consumer (kgo/config.go:229): 5s.
	//
	// franz-go stores it as int32 milliseconds (kgo/config.go:1497) and refuses a client below 10ms
	// (kgo/config.go:373), so anything under 10ms — a sub-millisecond value reaches the client as 0 — is
	// refused here rather than at the client's first boot.
	FetchMaxWait time.Duration `env:"FETCH_MAX_WAIT" envDefault:"5s"`

	// FetchMaxBytes caps a single fetch response. It is PER BROKER: the client fetches every broker
	// concurrently and can buffer up to brokers × FetchMaxBytes (kgo/config.go:1500-1504), which is what
	// makes it a memory lever as much as a throughput one. The default is franz-go's own
	// (kgo/config.go:675): 50MiB.
	//
	// It must stay at or below franz-go's BrokerMaxReadBytes, whose default is 100MiB
	// (kgo/config.go:646); above it the client refuses to start (kgo/config.go:331). BrokerMaxReadBytes
	// is not exposed, so the ceiling here is that default.
	FetchMaxBytes int32 `env:"FETCH_MAX_BYTES" envDefault:"52428800"` // 50 << 20

	// FetchMaxPartitionBytes caps a fetch response FOR ONE PARTITION. It is the duplication bound of
	// ADR-0012, and the only knob that sets it.
	//
	// A connector pool sends a message to the SMSC, publishes the outcome, and commits its offset. A
	// crash between the send and the commit re-delivers everything the poll had already sent — so the
	// number of subscribers who can receive the same SMS twice is the number of records one poll holds
	// per partition, and nothing else.
	//
	// The bound is in bytes, and those bytes are COMPRESSED: this caps stored batches, and franz-go
	// compresses with snappy by default (kgo/config.go:659) with nothing in this repository overriding
	// it. Measured on 500 realistic mt.routed records — distinct ids and MSISDNs, a 130-character GSM-7
	// body — 621 bytes raw became 221 compressed, a 2.81x ratio. 56KiB is therefore ~250 records, which
	// is the figure ADR-0012 commits to. franz-go's own default of 1MiB (kgo/config.go:678) is ~4750.
	//
	// The record count is an estimate for typical traffic, neither a ceiling nor a floor: bodies that
	// compress better raise it, UCS-2 and binary lower it. Re-measure before restating the figure.
	FetchMaxPartitionBytes int32 `env:"FETCH_MAX_PARTITION_BYTES" envDefault:"57344"` // 56 << 10

	// TopicPartitions is the partition count the provisioner creates a topic with (step-201, D7). It is
	// the ceiling on how many pods of one consumer group can work a topic in parallel: past it, the
	// extra pods idle. 12 divides by 1, 2, 3, 4 and 6, so a group can be sized without leaving
	// partitions unevenly spread.
	//
	// Nothing reads it at a service's boot: a topic is provisioned by a deliberate operator act, like a
	// migration, never by a racing replica at startup.
	TopicPartitions int32 `env:"TOPIC_PARTITIONS" envDefault:"12"`

	// TopicPartitionsOverrides raises TopicPartitions for named topics, as "topic=n,topic=n" — the hot
	// topics of the MT path do not need the same width as, say, a dead-letter topic. Empty (the default)
	// means every topic gets TopicPartitions.
	//
	// Parse it with PartitionOverrides; validation refuses a malformed list at boot rather than letting
	// a typo silently fall back to the default width. Whether a named topic actually exists is checked
	// by the provisioner, which owns the topic registry (internal/storage/kafka imports this package, so
	// the reverse would be an import cycle).
	TopicPartitionsOverrides string `env:"TOPIC_PARTITIONS_OVERRIDES"`

	// TopicReplicationFactor is the replication factor the provisioner creates a topic with. The spec
	// sizes the cluster for replication 3 (§2.5), which is what a durably-ACKed message rests on: with
	// RequiredAcks(AllISRAcks) a factor of 1 means one broker's disk decides whether an accepted message
	// survives. A single-broker laptop or CI cluster sets 1 explicitly; production may not.
	//
	// kadm takes it as an int16 (kadm/topics.go:161). Its -1 "let the broker choose" sentinel
	// (kadm/topics.go:149) is deliberately not accepted: the durability of the data plane is not a value
	// to inherit silently from a cluster's config.
	TopicReplicationFactor int16 `env:"TOPIC_REPLICATION_FACTOR" envDefault:"3"`
}

// PartitionOverrides parses TopicPartitionsOverrides into topic → partition count. It reports every
// malformed entry at once, naming the variable, so a boot failure is readable. An empty override list
// yields an empty map and no error.
//
// A topic absent from the map takes TopicPartitions.
func (k Kafka) PartitionOverrides() (map[string]int32, error) {
	overrides, problems := parseTopicPartitionOverrides(k.TopicPartitionsOverrides)
	if len(problems) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return overrides, nil
}

// parseTopicPartitionOverrides parses "topic=n,topic=n" and returns the entries it understood plus a
// problem per entry it did not. It refuses loudly — a bad entry is never dropped in favour of the
// default width, because a topic silently provisioned at 12 partitions instead of 48 is a capacity
// ceiling nobody would think to look for.
func parseTopicPartitionOverrides(raw string) (map[string]int32, []string) {
	overrides := make(map[string]int32)
	if strings.TrimSpace(raw) == "" {
		return overrides, nil
	}

	var problems []string
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			problems = append(problems, "KAFKA_TOPIC_PARTITIONS_OVERRIDES has an empty entry")
			continue
		}

		topic, count, found := strings.Cut(entry, "=")
		topic, count = strings.TrimSpace(topic), strings.TrimSpace(count)
		if !found || topic == "" || count == "" {
			problems = append(problems, fmt.Sprintf(
				"KAFKA_TOPIC_PARTITIONS_OVERRIDES entry %q is malformed: want topic=n", entry))
			continue
		}
		if strings.Contains(count, "=") {
			problems = append(problems, fmt.Sprintf(
				"KAFKA_TOPIC_PARTITIONS_OVERRIDES entry %q has more than one '=': want topic=n", entry))
			continue
		}
		if _, dup := overrides[topic]; dup {
			problems = append(problems, fmt.Sprintf(
				"KAFKA_TOPIC_PARTITIONS_OVERRIDES names topic %q twice", topic))
			continue
		}

		n, err := strconv.ParseInt(count, 10, 32)
		if err != nil {
			problems = append(problems, fmt.Sprintf(
				"KAFKA_TOPIC_PARTITIONS_OVERRIDES entry %q has a non-integer partition count %q", entry, count))
			continue
		}
		if n < 1 {
			problems = append(problems, fmt.Sprintf(
				"KAFKA_TOPIC_PARTITIONS_OVERRIDES entry %q must have at least 1 partition", entry))
			continue
		}
		overrides[topic] = int32(n)
	}
	return overrides, problems
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

	// MaxOpenConns caps concurrent connections to ClickHouse. It matters for admin-api-svc, where
	// search-messages queries and CDR exports run concurrently, far more than for the CDR writer, which
	// is a single insert loop. The default is the library's own (clickhouse_options.go:415-417):
	// MaxIdleConns + 5, so 10.
	//
	// The library treats a value of 0 or less as "unset" and silently substitutes its default
	// (clickhouse_options.go:412-417); a knob whose 0 quietly means 10 is a trap, so 0 is refused here.
	MaxOpenConns int `env:"MAX_OPEN_CONNS" envDefault:"10"`

	// MaxIdleConns is how many connections are kept in the idle pool between queries. The default is
	// the library's own (clickhouse_options.go:412-413): 5. Like MaxOpenConns, a non-positive value is
	// refused rather than silently defaulted.
	MaxIdleConns int `env:"MAX_IDLE_CONNS" envDefault:"5"`

	// CDRRetention is how long a CDR partition is kept before it is archived and dropped (§6.14,
	// step-165). It matches the table's own TTL by default; the message body expires far earlier, on the
	// per-column TTL, so this governs the metadata's lifetime, not the content's.
	CDRRetention time.Duration `env:"CDR_RETENTION" envDefault:"2160h"` // 90 days

	// RetentionInterval is how often the retention pass runs. 0 disables the pass entirely, leaving purge
	// to an external scheduler (the partition-drop is the same either way).
	RetentionInterval time.Duration `env:"RETENTION_INTERVAL" envDefault:"24h"`

	// ArchivePrefix enables cold-storage tiering: an expired partition is archived as
	// "<prefix>-<YYYY-MM-DD>.parquet" before it is dropped, and never dropped if archiving failed. Empty
	// (the default) drops without archiving — the real cold bucket is an infrastructure decision, so
	// tiering is opt-in rather than silently filling the ClickHouse server's own disk.
	ArchivePrefix string `env:"ARCHIVE_PREFIX"`
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

	// ExportDir is where asynchronous CDR exports write their artefacts (step-187). Empty — the
	// default — means the deployment has no export storage, and create-message-export answers 503
	// rather than queueing a job nothing can fulfil. Like the cold-archive prefix, the real object
	// destination is an infrastructure decision, so the capability is opt-in rather than silently
	// filling a pod's disk.
	ExportDir string `env:"EXPORT_DIR"`
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

// Billing configures a service's CLIENT connection to billing-svc (the gRPC server on :7001, step-144).
// Its two declarants are router-svc, which reserves MT credit (step-145), and connector-pool-svc, which
// captures and releases it (step-146). It is a client dial target, not a listen port — distinct from GRPC,
// which is a server's own listener. billing-svc itself does NOT declare it: it is the server, and it dials
// nobody (step-193d).
type Billing struct {
	// Addr is the host:port of the billing gRPC service (billing-svc, :7001). The credit stage dials it
	// lazily to reserve credit; billing being disabled for a customer is a cached-boolean decision upstream
	// of the RPC, so a healthy router that bills nobody never opens this connection.
	Addr string `env:"ADDR" envDefault:"localhost:7001"`

	// ReserveTimeout bounds a single credit-reserve RPC. It is short so a hung billing-svc turns into a
	// retryable error rather than stalling the consumer past its session timeout. It must stay comfortably
	// above billing-svc's synchronous durable-write latency, though: a deadline shorter than a legitimate
	// reserve commit would spuriously fail-closed and retry under load — hence a knob ops can widen without a
	// redeploy. Reserve is idempotent by message_id, so a deadline that fires after the server committed heals
	// on redelivery without double-charging.
	ReserveTimeout time.Duration `env:"RESERVE_TIMEOUT" envDefault:"200ms"`

	// SettleTimeout bounds a single capture/release RPC in the connector pool (step-146). Like
	// ReserveTimeout it is short so a slow billing-svc degrades to a fast fail-open rather than stalling the
	// send pipeline; capture/release do a synchronous durable write, so it must stay above that commit
	// latency, and is a knob ops can widen without a redeploy. Capture/release are idempotent by message_id.
	SettleTimeout time.Duration `env:"SETTLE_TIMEOUT" envDefault:"200ms"`
}

// BillingReaper configures billing-svc's own orphan-reservation reaper (step-190) — the net under the
// fail-open settle of step-146. Where Billing is a CLIENT dial target, these are the SERVER's knobs:
// billing-svc is the only binary that declares SectionBillingReaper, and the only one that reads them.
//
// They lived in Billing until step-193d, which is how they went unvalidated: billing-svc is the server,
// so it never declared the client section, so nothing checked what an operator put in these two.
type BillingReaper struct {
	// MinAge is how long a reservation must sit unsettled before the reaper reconciles it. The nominal
	// settle lands milliseconds after the SMSC responds, so anything still open past this window is
	// genuinely stuck rather than in flight. Too short is the dangerous direction — the reaper would race
	// connector-pool and settle live messages — hence the floor below. Ops widens it per deployment once
	// the real time-to-terminal-outcome is measured under load (step-200/201).
	MinAge time.Duration `env:"MIN_AGE" envDefault:"15m"`

	// Interval is how often the reaper sweeps. Reconciliation is not urgent — the money is already
	// recorded, only its settlement is late — so a slow cadence keeps the sweep well off the hot path.
	Interval time.Duration `env:"INTERVAL" envDefault:"5m"`
}

// ContentKey configures a service's CLIENT connection to content-key-svc (the gRPC server on :7002,
// step-167). admin-api-svc declares it to rotate, read and shred content keys; router-svc declares it to
// fetch the data key that seals a body at CDR write. It is a client dial target, not a listen port.
type ContentKey struct {
	// Addr is the host:port of the content-key gRPC service. It is dialled lazily, so a key service that
	// is briefly down does not block a caller's startup.
	Addr string `env:"ADDR" envDefault:"localhost:7002"`
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

	// PodAddrTemplate formats a smpp-server pod_id into a dialable gRPC address for the return-path
	// delivery (step-048): the return router Looks up an account's live binds, then dials the owning
	// pod's Deliver server. A single "%s" is replaced by the pod_id (its hostname). The K8s form is a
	// per-pod headless-service address; real at-scale pod discovery is M12. Empty disables bind
	// delivery (webhook only).
	PodAddrTemplate string `env:"POD_ADDR_TEMPLATE" envDefault:"%s.smpp-server-headless:7000"`

	// PodID identifies this pod in the session registry, so a token can be traced to the pod owning
	// the connection and released when that pod drains. Empty falls back to the OS hostname at startup.
	PodID string `env:"POD_ID"`

	// IdleTimeout drops a bind whose peer has gone silent for this long, reclaiming the goroutine and
	// releasing its registry token. It stands in for the enquire_link keep-alive (deferred) and is
	// aligned with the registry's session TTL so a silent peer's slot is not held past the drop.
	IdleTimeout time.Duration `env:"IDLE_TIMEOUT" envDefault:"60s"`

	// TrustedProxyCIDRs lists the load-balancer ranges whose PROXY protocol header is believed, e.g.
	// "10.0.0.0/8,10.1.0.0/16". Empty (the default) disables the protocol and keeps the transport peer
	// address — correct for a direct or L7-terminated deployment.
	//
	// It MUST be set behind an L4/TCP balancer: without it every bind appears to come from the balancer,
	// so the per-IP bind throttle degenerates into a global one and a single abusive client locks out all
	// the others (step-191). Setting it also closes the SMPP port to anything outside these ranges — the
	// intended posture behind a balancer, and the reason it stays off by default.
	TrustedProxyCIDRs []string `env:"TRUSTED_PROXY_CIDRS" envSeparator:","`

	// BindMaxFailures is how many bind failures a system_id or a source IP may accumulate within
	// BindFailureWindow before the next attempt is throttled — refused with a backoff, before argon2id
	// runs, so a brute-force flood cannot make the server pay that CPU cost (step-026).
	BindMaxFailures int `env:"BIND_MAX_FAILURES" envDefault:"5"`

	// BindFailureWindow is how long a bind failure is remembered. Each new failure slides the window
	// forward, so a lockout lifts only after a full quiet window.
	BindFailureWindow time.Duration `env:"BIND_FAILURE_WINDOW" envDefault:"15m"`

	// BindBackoffBase is the delay applied to the first throttled bind (at the threshold); it doubles
	// once per failure past the threshold.
	BindBackoffBase time.Duration `env:"BIND_BACKOFF_BASE" envDefault:"1s"`

	// BindBackoffMax caps the progressive bind backoff, so the tarpit delay never grows without bound.
	BindBackoffMax time.Duration `env:"BIND_BACKOFF_MAX" envDefault:"30s"`

	// MaxConns caps concurrent accepted SMPP connections. max_sessions is only enforced after a
	// successful bind, so this is the ceiling that bounds the goroutines and file descriptors an
	// unauthenticated flood (notably of tarpitted binds) can pin. Size it to the file-descriptor ulimit.
	MaxConns int `env:"MAX_CONNS" envDefault:"16384"`

	// QuerySMRatePerSec is the per-account query_sm rate limit (§6.22, step-087) — a token bucket
	// dedicated to query_sm, separate from the submit_sm budget so an intensive querier cannot eat the
	// send allowance. Zero disables the query_sm limit.
	QuerySMRatePerSec int `env:"QUERY_SM_RATE_PER_SEC" envDefault:"20"`
	// QuerySMBurst is the query_sm burst capacity; zero lets the limiter default it to one second's worth.
	QuerySMBurst int `env:"QUERY_SM_BURST" envDefault:"40"`

	// InboundWindow bounds how many submit_sm one bind processes concurrently (step-088), decoupling a
	// submit's produce from the read goroutine. Zero uses the session default.
	InboundWindow int `env:"INBOUND_WINDOW" envDefault:"256"`
}

// Section names a configuration group a binary depends on. A binary declares the sections it
// actually uses and Load enforces only those: a tool must never be refused a boot over a
// dependency it does not open a connection to. The migrate command is the case that forced this —
// it reads POSTGRES_URL and nothing else, yet a full validation blocked a production rollout over
// a defaulted KAFKA_BROKERS.
type Section uint16

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
	SectionBilling
	SectionContentKey
	SectionBillingReaper

	// SectionAll is what a caller declaring nothing gets. It must include every section, or
	// Validate() — which runs validate(SectionAll) — would quietly stop being a full check. The
	// cost of a section a binary does not use is nil: its fields carry valid defaults.
	SectionAll = SectionOTel | SectionPostgres | SectionKafka | SectionClickHouse | SectionHTTP |
		SectionRedis | SectionGRPC | SectionSMPP | SectionBilling | SectionContentKey |
		SectionBillingReaper
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

// Defaults returns the configuration exactly as DECLARED — every envDefault, nothing read from the
// process environment. It is what a service boots with when an operator sets nothing, and it never
// varies with the shell that happens to be running.
//
// Use it as a baseline a test or a harness must not drift from. A test bench that instead writes its
// own struct literal silently lands every unset field on the zero value, and a client library then
// substitutes its OWN default: the reference load run measured Kafka at franz-go's 1MiB
// FetchMaxPartitionBytes for two milestones because of exactly that, against the 56KiB ADR-0012
// commits to (step-201d). Deriving from here makes such a gap a test failure rather than a footnote.
//
// It is deliberately NOT validated: it is a declaration, not a boot. Load is the only path that boots
// a service.
func Defaults() Config {
	var cfg Config
	// An explicit (empty) environment is what makes this independent of os.Environ: only envDefault
	// tags feed the result.
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: map[string]string{}}); err != nil {
		// Unreachable in practice: the same struct parses on every Load. A malformed envDefault tag is a
		// programming error in this package, caught by its tests, not an operator's mistake.
		panic(fmt.Sprintf("config: parse declared defaults: %v", err))
	}
	return cfg
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
	if sections&SectionBilling != 0 {
		problems = append(problems, c.billingProblems()...)
	}
	if sections&SectionContentKey != 0 {
		problems = append(problems, c.contentKeyProblems()...)
	}
	if sections&SectionBillingReaper != 0 {
		problems = append(problems, c.billingReaperProblems()...)
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
	// Zero is legal and means "do not wait" — a service with no load balancer in front of it, or a
	// test. Negative is a typo, and silently accepting it would make the drain skip the wait it was
	// configured to take.
	if c.DrainDelay < 0 {
		problems = append(problems, fmt.Sprintf(
			"DRAIN_DELAY %s must not be negative: use 0 to drain without waiting for the load balancer",
			c.DrainDelay))
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
	// pgxpool documents pool_min_conns as "integer 0 or greater" (pgxpool/pool.go:346). It does not check
	// it against MaxConns, though: a min above the max is a target the pool can never reach, so its
	// health check gives up every round and the pre-warming silently never happens.
	if c.Postgres.MinConns < 0 {
		problems = append(problems, fmt.Sprintf("POSTGRES_MIN_CONNS %d must not be negative", c.Postgres.MinConns))
	}
	if c.Postgres.MinConns > c.Postgres.MaxConns {
		problems = append(problems, fmt.Sprintf(
			"POSTGRES_MIN_CONNS %d exceeds POSTGRES_MAX_CONNS %d: the pool could never reach it",
			c.Postgres.MinConns, c.Postgres.MaxConns))
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
	problems = append(problems, c.kafkaCapacityProblems()...)

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

// kafkaCapacityProblems checks the fetch and topic-provisioning levers (step-201, D5). Two of these
// bounds are franz-go's own client-startup refusals: catching them here turns a production boot loop
// into a message an operator reads while deploying.
func (c Config) kafkaCapacityProblems() []string {
	var problems []string

	// Zero would mean "no bound", the defect step-260e closes: refused like a value under the kgo floor.
	if c.Kafka.ProduceTimeout < kafkaMinProduceTimeout {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_PRODUCE_TIMEOUT %s must be at least %s: franz-go refuses a record delivery timeout under "+
				"1s, and a produce must always have a bound", c.Kafka.ProduceTimeout, kafkaMinProduceTimeout))
	}

	if c.Kafka.FetchMinBytes < 1 {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_FETCH_MIN_BYTES %d must be at least 1 byte", c.Kafka.FetchMinBytes))
	}
	if c.Kafka.FetchMaxBytes < 1 {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_FETCH_MAX_BYTES %d must be positive", c.Kafka.FetchMaxBytes))
	}
	if c.Kafka.FetchMaxBytes > kafkaBrokerMaxReadBytes {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_FETCH_MAX_BYTES %d exceeds franz-go's BrokerMaxReadBytes default of %d: the client would "+
				"refuse to start", c.Kafka.FetchMaxBytes, kafkaBrokerMaxReadBytes))
	}
	// franz-go does not check these two against each other. A minimum above the maximum is not fatal to
	// the client, it is worse: the response cap keeps a fetch from ever accumulating the minimum, so every
	// poll waits the full FetchMaxWait and throughput quietly collapses to one batch per max-wait.
	if c.Kafka.FetchMinBytes > c.Kafka.FetchMaxBytes {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_FETCH_MIN_BYTES %d exceeds KAFKA_FETCH_MAX_BYTES %d: every fetch would wait out "+
				"KAFKA_FETCH_MAX_WAIT", c.Kafka.FetchMinBytes, c.Kafka.FetchMaxBytes))
	}
	if c.Kafka.FetchMaxPartitionBytes < 1 {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_FETCH_MAX_PARTITION_BYTES %d must be positive", c.Kafka.FetchMaxPartitionBytes))
	}
	// franz-go clamps a partition cap above the response cap down to it silently (kgo/config.go:235-237).
	// Silence is the problem: this knob is the duplication bound of ADR-0012, so a value that does not
	// take effect is a bound an operator believes in and does not have.
	if c.Kafka.FetchMaxPartitionBytes > c.Kafka.FetchMaxBytes {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_FETCH_MAX_PARTITION_BYTES %d exceeds KAFKA_FETCH_MAX_BYTES %d: franz-go would clamp it "+
				"down in silence, leaving the ADR-0012 duplication bound unenforced",
			c.Kafka.FetchMaxPartitionBytes, c.Kafka.FetchMaxBytes))
	}
	// franz-go stores the wait as whole milliseconds and compares those to its floor. Truncating first is
	// the same test as comparing the duration to 10ms — floor(d/1ms) < 10 exactly when d < 10ms — but the
	// message then reports the value franz-go would see: 999µs reaches the client as 0.
	if ms := c.Kafka.FetchMaxWait.Milliseconds(); ms < kafkaMinFetchMaxWaitMillis {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_FETCH_MAX_WAIT %s truncates to %dms, below franz-go's %dms floor: the client would refuse "+
				"to start", c.Kafka.FetchMaxWait, ms, kafkaMinFetchMaxWaitMillis))
	}

	if c.Kafka.TopicPartitions < 1 {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_TOPIC_PARTITIONS %d must be at least 1", c.Kafka.TopicPartitions))
	}
	if c.Kafka.TopicReplicationFactor < 1 {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_TOPIC_REPLICATION_FACTOR %d must be at least 1 (kadm's -1 broker-default sentinel is not "+
				"accepted: the data plane's durability is set here, not inherited)", c.Kafka.TopicReplicationFactor))
	}
	// The same shape of guard as the loopback defaults: replication 1 is right on a one-broker laptop and
	// a durability hole in production, where a durably-ACKed message would rest on a single disk.
	if c.Environment.IsProduction() && c.Kafka.TopicReplicationFactor < 2 {
		problems = append(problems, fmt.Sprintf(
			"KAFKA_TOPIC_REPLICATION_FACTOR %d is a single-broker development value: production needs at "+
				"least 2 (the spec sizes the cluster for 3, §2.5)", c.Kafka.TopicReplicationFactor))
	}

	_, overrideProblems := parseTopicPartitionOverrides(c.Kafka.TopicPartitionsOverrides)
	return append(problems, overrideProblems...)
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
	// clickhouse-go reads a non-positive value as "unset" and substitutes its own default
	// (clickhouse_options.go:412-417), so a 0 here would look like a deliberate "no limit" and silently
	// become 5 or 10. Refuse it instead of shipping a knob that lies.
	if c.ClickHouse.MaxOpenConns < 1 {
		problems = append(problems, fmt.Sprintf(
			"CLICKHOUSE_MAX_OPEN_CONNS %d must be at least 1 (the driver would silently default a "+
				"non-positive value)", c.ClickHouse.MaxOpenConns))
	}
	if c.ClickHouse.MaxIdleConns < 1 {
		problems = append(problems, fmt.Sprintf(
			"CLICKHOUSE_MAX_IDLE_CONNS %d must be at least 1 (the driver would silently default a "+
				"non-positive value)", c.ClickHouse.MaxIdleConns))
	}
	// Not a driver refusal: the idle pool simply can never hold more than the open cap, and database/sql
	// clamps it silently on the std interface (clickhouse_std.go:140-141). An operator who wrote the two
	// numbers that way meant something else.
	if c.ClickHouse.MaxIdleConns > c.ClickHouse.MaxOpenConns {
		problems = append(problems, fmt.Sprintf(
			"CLICKHOUSE_MAX_IDLE_CONNS %d exceeds CLICKHOUSE_MAX_OPEN_CONNS %d: the extra idle slots can "+
				"never be filled", c.ClickHouse.MaxIdleConns, c.ClickHouse.MaxOpenConns))
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
	// Anti-brute-force thresholds (step-026). A non-positive threshold or window would either throttle
	// every bind or never throttle one; a non-positive backoff would defeat the tarpit. A window under a
	// second truncates to a zero EXPIRE, so it is rejected as well.
	if c.SMPP.BindMaxFailures < 1 {
		problems = append(problems, fmt.Sprintf(
			"SMPP_BIND_MAX_FAILURES %d must be positive", c.SMPP.BindMaxFailures))
	}
	if c.SMPP.BindFailureWindow < time.Second {
		problems = append(problems, fmt.Sprintf(
			"SMPP_BIND_FAILURE_WINDOW %s must be at least 1s", c.SMPP.BindFailureWindow))
	}
	if c.SMPP.BindBackoffBase <= 0 {
		problems = append(problems, fmt.Sprintf(
			"SMPP_BIND_BACKOFF_BASE %s must be positive", c.SMPP.BindBackoffBase))
	}
	if c.SMPP.BindBackoffMax < c.SMPP.BindBackoffBase {
		problems = append(problems, fmt.Sprintf(
			"SMPP_BIND_BACKOFF_MAX %s must be at least SMPP_BIND_BACKOFF_BASE %s",
			c.SMPP.BindBackoffMax, c.SMPP.BindBackoffBase))
	}
	// Sanity ceilings: a very long window enlarges Redis key lifetime (memory), and a very long tarpit
	// pins a connection slot far past any legitimate retry cadence.
	if c.SMPP.BindFailureWindow > time.Hour {
		problems = append(problems, fmt.Sprintf(
			"SMPP_BIND_FAILURE_WINDOW %s exceeds the 1h ceiling", c.SMPP.BindFailureWindow))
	}
	if c.SMPP.BindBackoffMax > 5*time.Minute {
		problems = append(problems, fmt.Sprintf(
			"SMPP_BIND_BACKOFF_MAX %s exceeds the 5m ceiling", c.SMPP.BindBackoffMax))
	}
	if c.SMPP.MaxConns < 1 {
		problems = append(problems, fmt.Sprintf(
			"SMPP_MAX_CONNS %d must be positive", c.SMPP.MaxConns))
	}
	return problems
}

// contentKeyProblems validates the client dial target of content-key-svc.
func (c Config) contentKeyProblems() []string {
	var problems []string
	if strings.TrimSpace(c.ContentKey.Addr) == "" {
		problems = append(problems, "CONTENT_KEY_ADDR is empty: content encryption cannot reach content-key-svc")
	}
	if strings.Contains(c.ContentKey.Addr, "://") {
		problems = append(problems, fmt.Sprintf("CONTENT_KEY_ADDR %q must be host:port without a scheme", c.ContentKey.Addr))
	}
	if c.Environment.IsProduction() && c.ContentKey.Addr == defaultContentKeyAddr {
		problems = append(problems, "CONTENT_KEY_ADDR is the localhost development default: set it explicitly in production")
	}
	return problems
}

// billingProblems checks the billing client dial target (step-145). A service that declared SectionBilling
// will call billing-svc, so the address must be a scheme-less host:port and must not stay the localhost
// development default in production (the same discipline as SMPP_SESSION_MANAGER_ADDR). The two RPC
// deadlines are checked here too: a non-positive one would fail every call it bounds.
func (c Config) billingProblems() []string {
	var problems []string
	if strings.TrimSpace(c.Billing.Addr) == "" {
		problems = append(problems, "BILLING_ADDR is empty: the credit stage cannot reach billing-svc")
	}
	if strings.Contains(c.Billing.Addr, "://") {
		problems = append(problems, fmt.Sprintf("BILLING_ADDR %q must be host:port without a scheme", c.Billing.Addr))
	}
	if c.Environment.IsProduction() && c.Billing.Addr == defaultBillingAddr {
		problems = append(problems, "BILLING_ADDR is the localhost development default: set it explicitly in production")
	}
	if c.Billing.ReserveTimeout <= 0 {
		problems = append(problems, fmt.Sprintf(
			"BILLING_RESERVE_TIMEOUT %s must be positive: a non-positive deadline would fail every reserve",
			c.Billing.ReserveTimeout))
	}
	if c.Billing.SettleTimeout <= 0 {
		problems = append(problems, fmt.Sprintf(
			"BILLING_SETTLE_TIMEOUT %s must be positive: a non-positive deadline would fail every capture/release",
			c.Billing.SettleTimeout))
	}
	return problems
}

// billingReaperProblems checks billing-svc's reaper knobs (step-190). Each refusal below describes a
// failure the running service would otherwise show only in production: one kills the pod at boot, the
// two others give back money for messages that were sent, or pretend to hold a setting they dropped.
func (c Config) billingReaperProblems() []string {
	var problems []string

	// runReap hands this straight to time.NewTicker, which panics on a non-positive duration. A config
	// refusal is a message an operator reads; a panic is a crash-loop they have to diagnose.
	if c.BillingReaper.Interval <= 0 {
		problems = append(problems, fmt.Sprintf(
			"BILLING_REAPER_INTERVAL %s must be positive: time.NewTicker panics on a non-positive interval, "+
				"so billing-svc would die at boot", c.BillingReaper.Interval))
	}
	// billing.WithMinAge ignores a non-positive value and keeps its own 15m default, so accepting one
	// would ship a knob that reports a setting it does not have — the same trap CLICKHOUSE_MAX_OPEN_CONNS
	// refuses. Reported separately from the floor so the message matches what the operator actually wrote.
	switch {
	case c.BillingReaper.MinAge <= 0:
		problems = append(problems, fmt.Sprintf(
			"BILLING_REAPER_MIN_AGE %s must be positive: billing.WithMinAge silently ignores a non-positive "+
				"value and the reaper keeps its own default, so the knob would lie", c.BillingReaper.MinAge))
	case c.BillingReaper.MinAge < minReaperMinAge:
		problems = append(problems, fmt.Sprintf(
			"BILLING_REAPER_MIN_AGE %s is below the %s floor: the reaper would race connector-pool's settle "+
				"loop and release credit for messages the SMSC actually took", c.BillingReaper.MinAge, minReaperMinAge))
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
		slog.Duration("drain_delay", c.DrainDelay),
		slog.Bool("otel_disabled", c.OTel.Disabled),
		slog.String("otel_endpoint", c.OTel.Endpoint),
		slog.Bool("postgres_url_set", strings.TrimSpace(c.Postgres.URL) != ""),
		slog.Any("kafka_brokers", c.Kafka.Brokers),
		// The capacity levers (step-201). A campaign has to be able to tell from a pod's own boot log
		// which knobs it actually got: a value set in the wrong manifest otherwise looks exactly like a
		// value that had no effect. None of them is secret-bearing.
		slog.Int64("kafka_fetch_min_bytes", int64(c.Kafka.FetchMinBytes)),
		slog.Duration("kafka_fetch_max_wait", c.Kafka.FetchMaxWait),
		slog.Int64("kafka_fetch_max_bytes", int64(c.Kafka.FetchMaxBytes)),
		slog.Int64("kafka_fetch_max_partition_bytes", int64(c.Kafka.FetchMaxPartitionBytes)),
		slog.Duration("kafka_produce_timeout", c.Kafka.ProduceTimeout),
		slog.Int64("kafka_topic_partitions", int64(c.Kafka.TopicPartitions)),
		slog.String("kafka_topic_partitions_overrides", c.Kafka.TopicPartitionsOverrides),
		slog.Int64("kafka_topic_replication_factor", int64(c.Kafka.TopicReplicationFactor)),
		slog.Int64("postgres_max_conns", int64(c.Postgres.MaxConns)),
		slog.Int64("postgres_min_conns", int64(c.Postgres.MinConns)),
		slog.Any("clickhouse_addr", c.ClickHouse.Addr),
		slog.Int("clickhouse_max_open_conns", c.ClickHouse.MaxOpenConns),
		slog.Int("clickhouse_max_idle_conns", c.ClickHouse.MaxIdleConns),
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
		slog.String("content_key_addr", c.ContentKey.Addr),
		slog.Int("smpp_port", c.SMPP.Port),
		slog.String("smpp_session_manager_addr", c.SMPP.SessionManagerAddr),
		slog.String("smpp_pod_id", c.SMPP.PodID),
		slog.Duration("smpp_idle_timeout", c.SMPP.IdleTimeout),
		slog.Int("smpp_bind_max_failures", c.SMPP.BindMaxFailures),
		slog.Duration("smpp_bind_failure_window", c.SMPP.BindFailureWindow),
		slog.Duration("smpp_bind_backoff_base", c.SMPP.BindBackoffBase),
		slog.Duration("smpp_bind_backoff_max", c.SMPP.BindBackoffMax),
		slog.Int("smpp_max_conns", c.SMPP.MaxConns),
	)
}
