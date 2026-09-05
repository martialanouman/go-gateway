package connectorpool

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Consumer reads mt.routed a batch at a time so the pool can shard a batch across its parallel binds
// (step-124). *kafka.Consumer satisfies it.
type Consumer interface {
	RunBatch(ctx context.Context, handle kafka.BatchHandler) error
}

// CDRWriter writes a CDR row directly. Since step-201c it is used ONLY for the outcomes that precede the
// irreversible effect — a message cancelled before dispatch, a reroute, a dead-letter — whose write
// failure can redeliver without duplicating an SMS. The post-submit outcome goes to mt.outcome instead
// (D1/D6). *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// CancelFlags arbitrates the single-winner cancel token a message shares with the cancel_sm front
// door (ADR-0013). The connector claims it as cancel.HolderDispatched before submit_sm; the returned
// holder is who actually owns it. *cancel.RedisFlags satisfies it.
//
// The two methods are NOT interchangeable, and which one a path may use follows from one question:
// does a submit_sm depend on the answer?
//
//   - Before a send, only Claim will do. That is what closes step-209: between a read and the send a
//     cancel_sm can win, so a reader would dispatch a message the customer was told would not go out.
//     The claim, not the lagging CDR projection, is what decides whether a cancel_sm still may cancel.
//   - On a path that has already decided NOT to send — the max-age expiry branch (step-245) — Peek is
//     the right call, and claiming would be the wrong one: a dispatched token there would refuse
//     legitimate cancel_sm for its whole TTL on a message that is never going out.
//
// New defaults a nil CancelFlags to a no-op that always concedes the token, so the hot path never
// branches on nil and a missing wiring cannot silently disable the check without the no-op being
// explicit.
type CancelFlags interface {
	Claim(ctx context.Context, messageID uuid.UUID, as cancel.Holder) (cancel.Holder, error)
	Peek(ctx context.Context, messageID uuid.UUID) (cancel.Holder, error)
}

// noopCancelFlags is the New default when no token store is wired: it reports the token as free, so
// the connector dispatches normally. Tests that do not exercise cancellation rely on it.
type noopCancelFlags struct{}

func (noopCancelFlags) Claim(context.Context, uuid.UUID, cancel.Holder) (cancel.Holder, error) {
	return cancel.HolderNone, nil
}

func (noopCancelFlags) Peek(context.Context, uuid.UUID) (cancel.Holder, error) {
	return cancel.HolderNone, nil
}

// DLRMap remembers smsc_msg_id -> message_id after a successful submit, so a later deliver_sm
// (delivery receipt) can be correlated back to the message (step-044 reads it). *dlrmap.RedisMap
// satisfies it. New defaults a nil DLRMap to a no-op, so the hot path never branches on nil and a
// missing wiring is explicit rather than a silent panic.
type DLRMap interface {
	Put(ctx context.Context, smscMsgID string, r pipeline.RoutedMT) error
}

// noopDLRMap is the New default when no DLR map is wired: it records nothing. Tests that do not
// exercise DLR correlation rely on it.
type noopDLRMap struct{}

func (noopDLRMap) Put(context.Context, string, pipeline.RoutedMT) error { return nil }

// ThrottleMetric observes the adaptive throttle (step-086): the connector's current send rate after
// each submit and each ESME_RTHROTTLED event. A wrapper over Prometheus satisfies it; New defaults a
// nil one to a no-op. It carries no label — a pod binds one connector, so the metric is per-pod
// (cardinality-bounded, never a message id or MSISDN).
type ThrottleMetric interface {
	SetRate(rate float64)
	IncThrottled()
}

// noopThrottle is the New default when no throttle metric is wired.
type noopThrottle struct{}

func (noopThrottle) SetRate(float64) {}
func (noopThrottle) IncThrottled()   {}

// DeadLetterMetric counts messages parked on mt.dead-letter, by reason — so a dead-lettered message is
// always counted, never silently lost (§1.11, step-129). A Prometheus CounterVec satisfies it; New
// defaults a nil one to a no-op. The reason label is a bounded gateway code (fallback_exhausted,
// retries_exhausted, delivery_expired), never a message id or MSISDN.
type DeadLetterMetric interface {
	Inc(reason string)
}

// noopDeadLetter is the New default when no dead-letter metric is wired.
type noopDeadLetter struct{}

func (noopDeadLetter) Inc(string) {}

// BillingSettler closes the MT billing loop (step-146): Capture confirms the reservation of a sent message
// (returning billed + credits_charged for the CDR), Release refunds it when a message terminally fails or is
// cancelled. It gates on the reservation flag pinned on mt.routed (billing disabled → ZERO billing call) and
// FAILS OPEN — neither method returns an error, so a billing fault can never leak into processOne's transient
// contract and redeliver a sent message (a duplicate SMS). *settle.Settler satisfies it; New defaults a nil
// one to a no-op so a pool with no billing wired never bills. Declared consumer-side (convention §2).
type BillingSettler interface {
	Capture(ctx context.Context, r pipeline.RoutedMT) (billed bool, creditsCharged *int32)
	Release(ctx context.Context, r pipeline.RoutedMT)
}

// noopSettler is the New default when no billing is wired: it settles nothing (billing opt-in). Tests that
// do not exercise billing rely on it.
type noopSettler struct{}

func (noopSettler) Capture(context.Context, pipeline.RoutedMT) (bool, *int32) { return false, nil }
func (noopSettler) Release(context.Context, pipeline.RoutedMT)                {}

// ConfigSource loads a connector's live pool config (bind_pool_size + reconnect policy) from the control
// plane, so a rebind / resize / policy change takes effect on the next re-dial (step-128b). A nil
// ConfigSource keeps the static BindConfig.BindPoolSize + Deps.Reconnect (no hot reload).
type ConfigSource interface {
	Load(ctx context.Context, connectorID uuid.UUID) (bindPoolSize int, rc reconnect.Config, err error)
}

// StatusControl is the runtime-status side of the pool (step-128b): it publishes this pod's per-bind
// link_status + in_flight for the Admin API to read, and reads the reconfigure generation the pool polls
// to pick up a rebind / resize / policy change. A nil StatusControl disables both (no runtime status, no
// hot reconfigure). *status.Reader satisfies it.
type StatusControl interface {
	PublishBind(ctx context.Context, connectorID uuid.UUID, podID string, bindIndex int, linkStatus string, inFlight int) error
	Gen(ctx context.Context, connectorID uuid.UUID) (int64, error)
}

// BreakerAggregator publishes one sub-bind's circuit-breaker state into the cross-pod connector
// aggregate (breaker:state, step-122), so the router (step-123) and the reroute path (step-125) can see
// a connector go open. *breaker.Aggregator satisfies it. A nil BreakerAggregator disables breaker
// reporting entirely (the pre-M8 behaviour): the pool still delivers, it just publishes no health.
type BreakerAggregator interface {
	Report(ctx context.Context, connectorID string, bindIndex int, s breaker.State) (breaker.State, error)
}

// Producer publishes the return path (mo.inbound, dlr.events), the reroute/dead-letter records, and —
// since step-201c — every send outcome on mt.outcome. *kafka.Producer satisfies it, and its ProduceSync
// is the acked-before-commit boundary the CDR's durability now rests on.
//
// It is REQUIRED: New panics on a nil Producer (step-201c D8). It used to default to a no-op, which was
// harmless while that only swallowed a deliver_sm or a reroute; it stopped being harmless when the
// outcome became the only record that a message left for the SMSC. A test that asserts nothing about
// the return path wires a discarding fake, explicitly.
type Producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// StreamEmitter records live figures for the realtime feed (internal/metricstream implements it). Its
// methods return nothing: the send path must not be able to branch on a dashboard failure.
type StreamEmitter interface {
	Add(kind string, labels metricstream.Labels, delta float64)
	Set(kind string, labels metricstream.Labels, value float64)
	// SetOneHot publishes an enum atomically; separate Set calls could be snapshotted mid-update and show
	// a connector both open and closed.
	SetOneHot(kind string, base metricstream.Labels, dimension string, values []string, current string)
}

// BreakerGauge is the Prometheus side of the breaker state, declared consumer-side
// (internal/observability/metrics.Catalog implements it).
type BreakerGauge interface {
	SetConnectorBreakerState(connectorID, state string)
}

// Deps are the connector pool's collaborators.
type Deps struct {
	Consumer    Consumer
	CDR         CDRWriter
	CancelFlags CancelFlags
	DLRMap      DLRMap
	Producer    Producer
	// ConnectorID identifies the SMSC link this pool binds, stamped onto every mo.inbound / dlr.events
	// record so the return-path router can correlate a receipt (step-044). At M2 it is injected from
	// env; M3+ sources it from the connectors control plane.
	ConnectorID uuid.UUID
	Bind        BindConfig
	// MaxSendRate is the connector's throughput_limit_per_sec — the ceiling for the adaptive throttle
	// (step-086). Zero disables the AIMD pacing (the pre-M6 behaviour).
	MaxSendRate float64
	// Throttle observes the adaptive throttle. New defaults a nil one to a no-op.
	Throttle ThrottleMetric
	// DeadLetter counts messages parked on mt.dead-letter by reason (step-129). New defaults nil to no-op.
	DeadLetter DeadLetterMetric
	// Billing captures/releases the MT reservation on the send outcome (step-146). New defaults a nil one
	// to a no-op that settles nothing (billing opt-in), so a pool with no billing wired never bills.
	Billing BillingSettler
	// Stream feeds the realtime dashboard (metrics.stream, step-182). Optional and best-effort: a nil
	// Stream disables emission, and nothing here may ever fail or delay a send.
	Stream StreamEmitter
	// BreakerGauge mirrors the breaker state onto Prometheus. Optional; nil disables it.
	BreakerGauge BreakerGauge
	// Metrics is the Prometheus side of the stream figures, fed from the same call sites with the same
	// names (step-180). Optional; nil disables it.
	Metrics *metrics.Catalog
	// RetryWindow is how long a connector-health failure (NOT a throttle) is redelivered before the
	// message is dead-lettered as retries_exhausted (step-129). Zero disables the window (redeliver
	// forever, the pre-step-129 behaviour) — the drainer/reroute still bound most cases.
	RetryWindow time.Duration
	// MaxMessageAge dead-letters a message older than this (max(SubmittedAt, ReplayedAt)) as
	// delivery_expired, a gateway SLA (step-129). Zero disables it.
	MaxMessageAge time.Duration
	// Breaker publishes each bind's circuit-breaker state to the connector aggregate (step-121/122).
	// Nil disables breaker reporting. When set, each bind runs a local breaker fed by its submit
	// outcomes, and a heartbeat reports every bind's state every BreakerHeartbeat.
	Breaker BreakerAggregator
	// BreakerConfig tunes the per-bind breaker state machine; the zero value uses breaker defaults.
	BreakerConfig breaker.Config
	// BreakerHeartbeat is how often each bind's state is (re)published. It MUST be shorter than the
	// aggregator's sub-bind TTL so a live bind is never swept from the quorum. Zero uses 2s.
	BreakerHeartbeat time.Duration
	// BreakerState reads other connectors' breaker aggregate to skip open candidates during a reroute
	// (step-125). Nil = never skip (advance to the next chain entry regardless).
	BreakerState BreakerState
	// RerouteLimiter gates a reroute on the target's throughput ceiling: no capacity → park on
	// mt.reroute-park (step-126). Nil disables parking (every reroute goes straight to mt.routed).
	RerouteLimiter RerouteLimiter
	// Reconnect is the opt-in auto-reconnection policy (step-127). The zero value is disabled: a dropped
	// bind is not retried in-process (link_status stays down until a manual rebind, step-128).
	Reconnect reconnect.Config
	// ConfigSource re-reads bind_pool_size + reconnect policy on each (re)dial so an Admin change takes
	// effect (step-128b). Nil = static config from Bind/Reconnect above.
	ConfigSource ConfigSource
	// StatusControl publishes per-bind link_status and reads the reconfigure generation (step-128b). Nil
	// disables runtime status + hot reconfigure.
	StatusControl StatusControl
	// PodID identifies this replica in the per-bind status hash (defaults to the hostname).
	PodID           string
	StatusHeartbeat time.Duration // per-bind status publish cadence; zero uses BreakerHeartbeat/2s
	Tracer          trace.Tracer
	Logger          *slog.Logger
}
