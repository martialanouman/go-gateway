// Package connectorpool is the outbound SMSC leg: a pool of parallel SMPP binds to one connector. It
// consumes mt.routed, submits each message with submit_sm, and records the outcome in the CDR (enroute
// on ESME_ROK, failed otherwise). It shards a poll batch across bind_pool_size binds by
// hash(message_id) % bind_pool_size, so every segment of a message lands on one bind in order (§7.3);
// each bind is processed by its own worker so the binds submit concurrently (step-124). No fallback and
// no reroute yet — those are later milestones.
package connectorpool

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/connector/status"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/encoding"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// errBindNotReady is reported by the readiness probe while the SMSC bind is not established.
var errBindNotReady = errors.New("connectorpool: smsc bind not established")

// errTransientReject marks an SMSC submit_sm_resp whose command_status is retryable (throttled,
// system error, queue full). The handler returns it so the record is not committed and is
// redelivered, rather than recording the message as a terminal failure and losing it.
var errTransientReject = errors.New("connectorpool: transient smsc rejection")

// errShardHalted marks a batch record left unprocessed because an earlier record in the same shard
// failed. It is not committed, so redelivery replays the shard in order behind the failure (§7.3).
var errShardHalted = errors.New("connectorpool: shard halted after an earlier failure")

// errReconfigure is returned by a dial cycle when the reconfigure generation changed (an Admin rebind /
// resize / policy change): the caller cleanly tears the cycle down and re-dials with fresh config
// (step-128b). It is not a fault — the outer loop retries immediately, resetting the reconnect backoff.
var errReconfigure = errors.New("connectorpool: reconfigure requested")

// Consumer reads mt.routed a batch at a time so the pool can shard a batch across its parallel binds
// (step-124). *kafka.Consumer satisfies it.
type Consumer interface {
	RunBatch(ctx context.Context, handle kafka.BatchHandler) error
}

// CDRWriter records the send outcome. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// CancelFlags reports whether a message was cancelled (via cancel_sm) before it reached the SMSC.
// *cancel.RedisFlags satisfies it. New defaults a nil CancelFlags to a no-op that reports nothing
// cancelled, so the hot path never branches on nil and a missing wiring cannot silently disable the
// check without the no-op being explicit.
type CancelFlags interface {
	Exists(ctx context.Context, messageID uuid.UUID) (bool, error)
}

// noopCancelFlags is the New default when no flag store is wired: it reports nothing cancelled, so the
// connector dispatches normally. Tests that do not exercise cancellation rely on it.
type noopCancelFlags struct{}

func (noopCancelFlags) Exists(context.Context, uuid.UUID) (bool, error) { return false, nil }

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

// Producer publishes the return path (mo.inbound, dlr.events) durably. *kafka.Producer satisfies it.
// New defaults a nil Producer to a no-op, so a bind with no producer wired acknowledges deliver_sm as
// before (the M2 behaviour) rather than panicking.
type Producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// noopProducer is the New default when no producer is wired: it drops the record. With it, a
// deliver_sm is acknowledged without publishing (the pre-M4 behaviour), which the MT-only tests rely
// on.
type noopProducer struct{}

func (noopProducer) Produce(context.Context, kafka.Record) error { return nil }

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

// Service is the connector pool.
type Service struct {
	deps Deps

	// aimd is the adaptive send-rate throttle (step-086), nil when MaxSendRate is 0.
	aimd *aimd

	// breakers holds one local circuit-breaker per bind_index, fed by that bind's submit outcomes; a
	// heartbeat publishes their state via deps.Breaker. Nil (len 0) when breaker reporting is disabled.
	breakers []*breaker.Breaker

	// bound reports whether the SMSC bind is currently established. It gates the readiness probe, so
	// a bind that drops — including an idle-time drop no in-flight Submit would notice — takes the
	// pod out of rotation until Run re-dials.
	bound atomic.Bool

	// link is the 3-state link_status (step-128): up while serving, reconnecting while backing off /
	// re-dialling, down while parked. Distinct from the breaker (application health).
	link atomic.Int32

	// poolSize and reconnectCfg are the live config for the current dial cycle, refreshed from the
	// control plane before each cycle (step-128b). They are read and written only from the single Run
	// goroutine, sequentially between cycles, so they need no synchronisation.
	poolSize     int
	reconnectCfg reconnect.Config
	podID        string

	// retryFirstFail records, per (partition, offset), when a message first hit a connector-health
	// failure, so the retry window can dead-letter a message that keeps failing past RetryWindow
	// (step-129). Keyed by the immutable offset (stable across redeliveries). processOne clears the key
	// on every committed outcome (its deferred cleanup), so it never outlives the record.
	retryFirstFail sync.Map // "partition:offset" -> time.Time
}

// link_status states (atomic int).
const (
	linkDown         int32 = iota // dropped or parked
	linkUp                        // a bind is established and serving
	linkReconnecting              // backing off / re-dialling after a drop or a reconfigure
)

// breakerHeartbeat is the default cadence for republishing every bind's breaker state.
const breakerHeartbeat = 2 * time.Second

// New builds a Service. A nil logger defaults to slog.Default; a nil CancelFlags defaults to a no-op
// that reports nothing cancelled.
func New(deps Deps) *Service {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.CancelFlags == nil {
		deps.CancelFlags = noopCancelFlags{}
	}
	if deps.DLRMap == nil {
		deps.DLRMap = noopDLRMap{}
	}
	if deps.Producer == nil {
		deps.Producer = noopProducer{}
	}
	if deps.Throttle == nil {
		deps.Throttle = noopThrottle{}
	}
	if deps.DeadLetter == nil {
		deps.DeadLetter = noopDeadLetter{}
	}
	if deps.Billing == nil {
		deps.Billing = noopSettler{}
	}
	s := &Service{deps: deps, podID: deps.PodID}
	// Seed the live config with the static env values; reloadConfig overwrites from the control plane
	// only on a successful load, so a Postgres blip keeps the last-good config rather than reverting.
	s.poolSize = deps.Bind.BindPoolSize
	if s.poolSize < 1 {
		s.poolSize = 1
	}
	s.reconnectCfg = deps.Reconnect
	if deps.MaxSendRate > 0 {
		s.aimd = newAIMD(deps.MaxSendRate, nil)
	}
	return s
}

// BindReady is the readiness probe for the SMSC bind: nil while the bind is established, an error
// once it is down. The connector pool cannot deliver a single message without a live bind, so this
// is a vital readiness dependency (plan §1.5) — register it alongside Kafka and ClickHouse.
func (s *Service) BindReady(context.Context) error {
	if s.bound.Load() {
		return nil
	}
	return errBindNotReady
}

// LinkStatus reports the SMSC link state — "up" while a bind is established, "down" while it is not
// (dropped, or backing off between reconnect attempts). It is the runtime link_status (§6.13), kept
// strictly distinct from the connector's breaker_state (application health, step-121/122): a live link
// can carry an open breaker and vice versa.
func (s *Service) LinkStatus() string {
	switch s.link.Load() {
	case linkUp:
		return status.LinkUp
	case linkReconnecting:
		return status.LinkReconnecting
	default:
		return status.LinkDown
	}
}

// setLink updates the 3-state link status.
func (s *Service) setLink(state int32) { s.link.Store(state) }

// Run keeps the bind pool alive across reconnects AND reconfigures (step-127/128b). Each outer iteration
// re-reads the connector's live config (bind_pool_size + reconnect policy) from the control plane, runs a
// reconnect loop over one dial-and-consume cycle, and reacts to how it ended:
//   - clean ctx shutdown → return nil;
//   - reconfigure requested (Admin rebind / resize / policy) → re-read config and re-dial immediately;
//   - reconnection disabled or exhausted → PARK (stay alive, link down) until a reconfigure or shutdown
//     — never exit, so k8s does not turn into a harsher reconnect loop than the one the operator chose.
func (s *Service) Run(ctx context.Context) error {
	for {
		s.reloadConfig(ctx)
		loop := reconnect.New(s.reconnectCfg)
		err := loop.Run(ctx, s.runOnce, isLinkDrop)
		if ctx.Err() != nil {
			s.setLink(linkDown)
			return nil
		}
		if errors.Is(err, errReconfigure) {
			continue // Admin change: re-read config and re-dial (backoff reset)
		}
		if err == nil {
			s.setLink(linkDown)
			return nil
		}
		// Reconnection gave up (disabled, exhausted, or a permanent bind rejection). With a control plane
		// wired, PARK — stay alive with a down link and wait for an Admin rebind — rather than exit (which
		// would let k8s restart into a harsher reconnect loop than the operator chose). Without one there
		// is nothing to un-park us, so surface the error and let the supervisor restart (the pre-128b
		// behaviour).
		s.setLink(linkDown)
		if s.deps.StatusControl == nil {
			return err
		}
		s.deps.Logger.WarnContext(ctx, "connector: link down, parking until reconfigure", "err", err)
		if perr := s.park(ctx); perr != nil {
			return perr // ctx cancelled
		}
		// park returned nil → a reconfigure arrived; re-read config and re-dial.
	}
}

// reloadConfig refreshes the pool size and reconnect policy from the control plane. On a load error it
// KEEPS the current (last-good) config — set in New to the env defaults and updated only on success — so
// a transient Postgres blip during a reconfigure never silently reverts a live-configured pool to env.
func (s *Service) reloadConfig(ctx context.Context) {
	if s.deps.ConfigSource == nil {
		return // static config (already seeded in New)
	}
	n, rc, err := s.deps.ConfigSource.Load(ctx, s.deps.ConnectorID)
	if err != nil {
		s.deps.Logger.WarnContext(ctx, "connector: config reload failed, keeping current config", "err", err)
		return
	}
	if n >= 1 {
		s.poolSize = n
	}
	s.reconnectCfg = rc
}

// park keeps the pod alive with a down link, polling the reconfigure generation until it changes (an
// Admin rebind) or ctx is cancelled. It returns nil on a generation change (the caller re-dials) and the
// ctx error on shutdown. With no StatusControl wired there is nothing to poll, so it blocks until ctx.
func (s *Service) park(ctx context.Context) error {
	if s.deps.StatusControl == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	ticker := time.NewTicker(s.statusInterval())
	defer ticker.Stop()
	baseline, err := s.deps.StatusControl.Gen(ctx, s.deps.ConnectorID)
	haveBaseline := err == nil
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			g, err := s.deps.StatusControl.Gen(ctx, s.deps.ConnectorID)
			if err != nil {
				continue // a Redis blip is not a reconfigure
			}
			if !haveBaseline {
				baseline, haveBaseline = g, true // first successful read establishes the baseline
				continue
			}
			if g != baseline {
				return nil // reconfigure requested while parked
			}
		}
	}
}

// statusInterval is the per-bind status / generation-poll cadence.
func (s *Service) statusInterval() time.Duration {
	if s.deps.StatusHeartbeat > 0 {
		return s.deps.StatusHeartbeat
	}
	return breakerHeartbeat
}

// isLinkDrop reports whether an error is a live bind dropping (the only thing the reconnect loop backs
// off and retries). Everything else propagates: a bind handshake REJECTION — a permanent bad password
// (ESME_RINVPASWD), or a transient SMSC system error — and a non-link fault such as a Kafka error, so a
// transient consumer blip restarts the pod rather than churning healthy SMSC binds.
func isLinkDrop(err error) bool {
	return errors.Is(err, errBindClosed)
}

// runOnce brings up the bind pool, then consumes mt.routed until ctx is cancelled, unbinding cleanly on
// exit. A failure to bind any member returns an error (the reconnect loop decides whether to retry); a
// per-message infrastructure failure leaves the offset uncommitted for reprocessing.
//
// The binds are watched independently of the consumer: if one drops while idle — no mt.routed flowing,
// so no Submit is in flight to surface the failure — the consumer would otherwise block on Kafka forever
// with a dead bind while the pod stayed Ready. When any bind dies runOnce flips readiness, tears the
// consumer down and returns an error so the caller re-dials the whole pool (never re-sharding a live
// message onto another bind, which would break §7.3 ordering).
func (s *Service) runOnce(ctx context.Context) error {
	n := s.poolSize
	if n < 1 {
		n = 1
	}
	binds := make([]*bind, 0, n)
	for i := 0; i < n; i++ {
		b, err := dialAndBind(ctx, s.deps.Bind, s.deps.Logger, s.handleDeliver)
		if err != nil {
			for _, prev := range binds {
				prev.Close() //nolint:contextcheck // cleanup unbind detaches from ctx, like the shutdown path
			}
			return fmt.Errorf("connectorpool: bind %d/%d: %w", i+1, n, err)
		}
		binds = append(binds, b)
	}
	// Close detaches from ctx on purpose: the unbind must be sent AFTER ctx is cancelled (that is
	// what triggers the drain), on its own bounded context, exactly like observability's tracing drain.
	defer func() { //nolint:contextcheck // deliberate detach for the shutdown unbind (see Close)
		for _, b := range binds {
			b.Close()
		}
	}()

	s.bound.Store(true)
	s.setLink(linkUp)
	defer s.bound.Store(false)

	consumerCtx, cancel := context.WithCancel(ctx)
	// The heartbeats read s.breakers and binds; join them BEFORE this cycle returns (and thus before the
	// next cycle reassigns s.breakers or Close()s the binds). Deferred first so it runs AFTER cancel (LIFO)
	// — cancel stops the heartbeats, hbWG.Wait joins them, then the bind-Close defer runs.
	var hbWG sync.WaitGroup
	defer hbWG.Wait()
	defer cancel()

	// Per-bind circuit breakers (step-121), fed by each bind's submit outcomes, published to the
	// cross-pod aggregate by a heartbeat (step-122). Created here because the pool size is known now.
	if s.deps.Breaker != nil {
		s.breakers = make([]*breaker.Breaker, n)
		for i := range s.breakers {
			s.breakers[i] = breaker.New(s.deps.BreakerConfig, nil)
		}
		hbWG.Add(1)
		go func() { defer hbWG.Done(); s.runBreakerHeartbeat(consumerCtx) }()
	}

	// Runtime-status heartbeat + reconfigure poll (step-128b): publishes each bind's link_status +
	// in_flight for the Admin API, and closes reconfigure when the generation changes (an Admin rebind /
	// resize / policy change), so the select below tears the cycle down cleanly and re-dials.
	reconfigure := make(chan struct{})
	if s.deps.StatusControl != nil {
		var once sync.Once
		hbWG.Add(1)
		go func() {
			defer hbWG.Done()
			s.runStatusHeartbeat(consumerCtx, binds, func() { once.Do(func() { close(reconfigure) }) })
		}()
	}

	// A single signal fired by whichever bind dies first (idle drop, enquire_link timeout, peer close).
	anyDropped := make(chan struct{})
	var dropOnce sync.Once
	for _, b := range binds {
		go func(b *bind) {
			<-b.done
			dropOnce.Do(func() { close(anyDropped) })
		}(b)
	}

	consumerErr := make(chan error, 1)
	go func() { consumerErr <- s.deps.Consumer.RunBatch(consumerCtx, s.batchHandler(binds)) }()

	select {
	case err := <-consumerErr:
		return err
	case <-anyDropped:
		// A bind died on its own. Take the pod out of rotation, unwind the consumer, and surface the
		// failure so the reconnect loop backs off and re-dials the whole pool.
		s.bound.Store(false)
		s.setLink(linkReconnecting)
		cancel()
		<-consumerErr
		return fmt.Errorf("connectorpool: smsc bind dropped: %w", errBindClosed)
	case <-reconfigure:
		// An Admin change: tear down cleanly (cancel consumer → drain/commit the batch → unbind on
		// defer), then re-dial with fresh config. The clean path — not the "bind dead" path — avoids a
		// deliberate duplicate on a forced rebind.
		s.bound.Store(false)
		s.setLink(linkReconnecting)
		cancel()
		<-consumerErr
		return errReconfigure
	}
}

// runStatusHeartbeat publishes each bind's link_status + in_flight on a fixed cadence and polls the
// reconfigure generation, calling onReconfigure once when it changes. It stops on ctx (one dial cycle).
func (s *Service) runStatusHeartbeat(ctx context.Context, binds []*bind, onReconfigure func()) {
	ticker := time.NewTicker(s.statusInterval())
	defer ticker.Stop()
	id := s.deps.ConnectorID
	publish := func() {
		for i, b := range binds {
			if err := s.deps.StatusControl.PublishBind(ctx, id, s.podID, i, s.LinkStatus(), b.inFlight()); err != nil && ctx.Err() == nil {
				s.deps.Logger.WarnContext(ctx, "connector: publish bind status failed", "bind_index", i, "err", err)
			}
		}
	}
	publish() // publish immediately so status is fresh without waiting a full tick
	// The baseline is this cycle's OWN first successful generation read, so a Redis blip during
	// reloadConfig cannot make us re-dial healthy binds in a tight loop.
	baseline, err := s.deps.StatusControl.Gen(ctx, id)
	haveBaseline := err == nil
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
			g, err := s.deps.StatusControl.Gen(ctx, id)
			if err != nil {
				continue
			}
			if !haveBaseline {
				baseline, haveBaseline = g, true
				continue
			}
			if g != baseline {
				onReconfigure()
				return
			}
		}
	}
}

// batchHandler shards a poll batch across the pool: each record goes to bind[hash(message_id) % N], and
// each shard's records are processed sequentially by a dedicated goroutine so the binds run
// concurrently while a message's segments stay ordered on one bind. A record that fails halts the rest
// of ITS shard (later records marked errored, not committed) so a segment can never overtake an earlier
// one on redelivery (§7.3); other shards are unaffected.
func (s *Service) batchHandler(binds []*bind) kafka.BatchHandler {
	n := len(binds)
	return func(ctx context.Context, recs []kafka.Record) []error {
		results := make([]error, len(recs))
		shards := make(map[int][]int, n) // shard index -> record indices, in batch (offset) order
		for i, rec := range recs {
			sh := shardIndex(rec.Key, n)
			shards[sh] = append(shards[sh], i)
		}
		var wg sync.WaitGroup
		for sh, idxs := range shards {
			wg.Add(1)
			go func(sh int, idxs []int) {
				defer wg.Done()
				for pos, i := range idxs {
					if err := s.processOne(ctx, binds[sh], sh, recs[i]); err != nil {
						results[i] = err
						// Stop this shard: leave every later record of this shard unprocessed and uncommitted
						// so redelivery replays them in order behind the failure.
						for _, j := range idxs[pos+1:] {
							results[j] = errShardHalted
						}
						return
					}
				}
			}(sh, idxs)
		}
		wg.Wait()
		return results
	}
}

// runBreakerHeartbeat republishes every bind's current breaker state on a fixed cadence until ctx is
// cancelled. One periodic report (rather than one per submit) keeps the hot path off Redis while still
// keeping each sub-bind alive in the aggregate quorum and surfacing time-driven transitions (a State()
// read advances open → half_open). It starts no work when no breaker is wired.
func (s *Service) runBreakerHeartbeat(ctx context.Context) {
	interval := s.deps.BreakerHeartbeat
	if interval <= 0 {
		interval = breakerHeartbeat
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	connectorID := s.deps.ConnectorID.String()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i, b := range s.breakers {
				if _, err := s.deps.Breaker.Report(ctx, connectorID, i, b.State()); err != nil && ctx.Err() == nil {
					s.deps.Logger.WarnContext(ctx, "connector: breaker state report failed", "bind_index", i, "err", err)
				}
			}
		}
	}
}

// feedBreaker records a submit outcome into the bind's local breaker. status is the submit_sm_resp
// command_status; a submitErr (transport failure, no response) is a connector-health failure. It is a
// no-op when no breaker is wired.
func (s *Service) feedBreaker(bindIndex int, status uint32, submitErr bool) {
	if s.breakers == nil {
		return
	}
	b := s.breakers[bindIndex]
	if submitErr {
		b.RecordFailure()
		return
	}
	b.Record(status)
}

// shardIndex maps a record's partition key (the message id, shared by every segment) to a bind, so all
// of a message's segments hash to the same bind and stay ordered. FNV-1a is enough — the property
// needed is only a stable, uniform in-run mapping, not cryptographic strength or cross-run stability.
func shardIndex(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n)) //nolint:gosec // n is 1..32, the modulo result fits an int
}

// expired reports whether a routed message has outlived the gateway's max-age SLA and must be
// dead-lettered instead of submitted (step-129). The age base is max(SubmittedAt, ReplayedAt): a
// replayed message uses its replay time, so an operator replay after a long outage is not instantly
// re-expired on the immutable SubmittedAt. A zero MaxMessageAge disables the check.
func (s *Service) expired(r pipeline.RoutedMT) bool {
	if s.deps.MaxMessageAge <= 0 {
		return false
	}
	base := r.SubmittedAt
	if r.ReplayedAt != nil && r.ReplayedAt.After(base) {
		base = *r.ReplayedAt
	}
	return time.Since(base) > s.deps.MaxMessageAge
}

// healthRetry handles a connector-health failure for a message with NO viable fallback chain (a dead
// bind, a submit timeout, or a failover-class SMSC rejection). It leaves the record uncommitted for
// redelivery until the SAME record has been failing longer than RetryWindow, then dead-letters it as
// retries_exhausted and commits, so a persistently-failing message is not retried without end (step-129).
// Throttle / queue-full NEVER reach here: those are pure backpressure, bounded only by the max-age SLA.
//
// Redelivery is driven by the reconnect/re-dial cycle (a dropped bind) or by the pod restart the
// supervisor performs when reconnection gives up — NOT a tight in-process loop, so no pacing is needed
// here (the reconnect loop and k8s each apply their own backoff). The first-failure time is keyed by the
// record's immutable (partition, offset) and accumulates across redeliveries for as long as this Service
// lives (a re-dial keeps it; a process restart resets the window). A zero RetryWindow disables
// dead-lettering, and the max-age SLA is the ultimate backstop in every case.
func (s *Service) healthRetry(ctx context.Context, rec kafka.Record, r pipeline.RoutedMT, cause error) error {
	if s.deps.RetryWindow > 0 {
		first, _ := s.retryFirstFail.LoadOrStore(retryKey(rec), time.Now())
		if time.Since(first.(time.Time)) > s.deps.RetryWindow {
			return s.deadLetterWith(ctx, r, errs.ErrRetriesExhausted)
		}
	}
	return fmt.Errorf("connectorpool: connector-health redelivery: %w", cause)
}

// retryKey identifies a record for the retry window by its immutable (partition, offset).
func retryKey(rec kafka.Record) string { return fmt.Sprintf("%d:%d", rec.Partition, rec.Offset) }

// processOne submits a single routed segment on the given bind and records its outcome. It returns a
// non-nil error only on a transient fault (bad decode, dead bind, transient SMSC rejection) so the
// record is left uncommitted for redelivery; a terminal SMSC failure is written to the CDR and returns
// nil. It is the per-record body the batch handler runs, one shard at a time.
func (s *Service) processOne(ctx context.Context, b *bind, bindIndex int, rec kafka.Record) (err error) {
	ctx, span := s.deps.Tracer.Start(ctx, "connector.submit")
	defer span.End()

	// A committed outcome (nil return) means this offset advances and will not be redelivered, so any
	// retry-window entry for it is dead weight — clear it. healthRetry keeps its entry only by returning a
	// non-nil error (redelivery); every nil path drops it, so the map never outlives a record (step-129).
	defer func() {
		if err == nil {
			s.retryFirstFail.Delete(retryKey(rec))
		}
	}()

	routed, err := pipeline.DecodeRouted(rec)
	if err != nil {
		return fmt.Errorf("connectorpool: decode mt.routed: %w", err)
	}

	// Per-connector addressing (step-125, option B): the pool's group consumes ALL of mt.routed, so a
	// record for another connector — including one another pool just rerouted — is not ours to send.
	// Skip and commit it. A message rerouted TO this connector carries our id and is processed normally.
	// A pool with no ConnectorID configured (uuid.Nil) filters nothing and processes every record (the
	// pre-step-125 single-connector behaviour).
	if s.deps.ConnectorID != uuid.Nil && routed.ConnectorID != s.deps.ConnectorID {
		return nil
	}

	// Gateway max-age SLA (step-129): a message that has outlived MaxMessageAge — whether it aged out in
	// throttle backpressure or churned across reconnects — is dead-lettered as delivery_expired rather
	// than submitted, so nothing lingers on the data plane forever. Checked before any submit work.
	if s.expired(routed) {
		return s.deadLetterWith(ctx, routed, errs.ErrDeliveryExpired)
	}

	// A cancel_sm may have flagged this message before it reached the SMSC. Redis is best-effort
	// here: cancellation is itself best-effort (an already-dispatched message cannot be recalled), so
	// a flag-read failure fails OPEN — we log and dispatch rather than halt all outbound delivery on a
	// Redis outage.
	cancelled, err := s.deps.CancelFlags.Exists(ctx, routed.MessageID)
	if err != nil {
		s.deps.Logger.WarnContext(ctx, "connector: cancel-flag check failed, dispatching anyway",
			"message_id", routed.MessageID, "err", err)
	} else if cancelled {
		// Honour the cancel: record the cancelled outcome and commit without submitting. Writing the
		// row here (not only in the Canceller) is what makes the skip safe: it is idempotent under
		// ReplacingMergeTree (rank 60, collapsing with the Canceller's row) and closes the window where
		// the Canceller crashed after flagging but before writing the row — otherwise the message would
		// be neither sent nor recorded, leaving the CDR stuck on accepted.
		// A cancelled message is never sent, so release its reservation (step-146). Fail-open, gated on
		// Billable — a billing-disabled or unreserved message makes no call.
		s.deps.Billing.Release(ctx, routed)
		if err := s.deps.CDR.Insert(ctx, cancelledRow(routed)); err != nil {
			return fmt.Errorf("connectorpool: write cancelled cdr: %w", err)
		}
		s.deps.Logger.InfoContext(ctx, "connector: message cancelled before dispatch", "message_id", routed.MessageID)
		return nil
	}

	// Reroute before submitting if this connector's own breaker is already open and the message carries a
	// fallback chain (step-125): no point pacing and submitting to a connector we know is down — advance
	// the chain now. Uses the LOCAL breaker (this pod's view), so the hot path never reads Redis.
	if len(routed.FallbackChain) > 0 && s.breakers != nil && s.breakers[bindIndex].State() == breaker.Open {
		return s.reroute(ctx, routed, errs.ErrServiceUnavailable)
	}

	// Adaptive throttle (step-086): pace to the connector's current AIMD send rate before submitting,
	// so a throttled SMSC slows our outbound rather than being hammered. It blocks at most one send
	// interval and honours ctx; it NEVER cuts the bind (that is the circuit breaker's job, M8).
	if s.aimd != nil {
		if err := s.aimd.acquire(ctx); err != nil {
			return fmt.Errorf("connectorpool: throttle wait: %w", err)
		}
	}

	resp, err := b.Submit(ctx, buildSubmit(routed))
	if err != nil {
		// A dead bind, a write failure or a timeout is transient and a connector-health failure for the
		// breaker (no response came back). With a fallback chain, reroute to the next connector; without
		// one, do not commit so the message is reprocessed after a restart (at-least-once).
		s.feedBreaker(bindIndex, 0, true)
		if len(routed.FallbackChain) > 0 {
			return s.reroute(ctx, routed, errs.ErrServiceUnavailable)
		}
		return s.healthRetry(ctx, rec, routed, fmt.Errorf("connectorpool: submit_sm: %w", err))
	}

	// Feed the outcome to this bind's circuit breaker (step-121): a system error / bind failure is a
	// health failure, a throttle/queue-full is transient (ignored), a success clears it.
	s.feedBreaker(bindIndex, resp.Status, false)

	// Feed the submit_sm_resp back to the adaptive throttle: an ESME_RTHROTTLED halves the send rate,
	// a success nudges it back up toward the ceiling (step-086).
	if s.aimd != nil {
		if s.aimd.observe(resp.Status) {
			s.deps.Throttle.IncThrottled()
		}
		s.deps.Throttle.SetRate(s.aimd.currentRate())
	}

	// Reroute on a connector-health rejection when a fallback chain is carried (step-125): this connector
	// is sick, so try the next healthy one rather than redeliver here or fail. A throttle stays a
	// redeliver (below) and a permanent per-message reject stays a terminal CDR — only failover-class
	// statuses reroute, and only when a chain exists.
	if resp.Status != smpp.StatusOK && len(routed.FallbackChain) > 0 && classifyReroute(resp.Status) == failover {
		return s.reroute(ctx, routed, errs.CodeFromSMPPStatus(resp.Status))
	}

	// A transient SMSC rejection (throttled, system error, queue full) is backpressure, not a
	// terminal outcome: do not write a failed CDR and do not commit, so the message is redelivered
	// rather than lost. Permanent rejections (invalid address, submit_fail) fall through to the CDR
	// write below. Proper rate-limited backoff is M7; this reuses the same "return error → no commit
	// → reprocess" path the submit errors above use.
	if resp.Status != smpp.StatusOK && errs.Retryable(errs.CodeFromSMPPStatus(resp.Status)) {
		// A failover-class health rejection with no fallback chain runs through the retry window, so a
		// persistently-sick connector eventually dead-letters (retries_exhausted) instead of redelivering
		// without end. Throttle / queue-full is pure backpressure: redeliver, bounded only by the max-age
		// SLA checked at the top on the next redelivery.
		if classifyReroute(resp.Status) == failover {
			return s.healthRetry(ctx, rec, routed, errTransientReject)
		}
		return fmt.Errorf("connectorpool: submit_sm rejected transiently (status 0x%08x): %w", resp.Status, errTransientReject)
	}

	// Settle the reservation on the terminal outcome (step-146): capture a sent message, release a
	// permanently-failed one. Both FAIL OPEN — neither returns an error — so a billing fault can never turn
	// this committed outcome into a redelivery that re-submits the message (a duplicate SMS). A
	// billing-disabled message makes no call. Capture fills billed/credits_charged on the CDR; the failed
	// path leaves them false/nil (the reserve refund happens durably in billing-svc, not on this row).
	row := cdrRow(routed, resp)
	if resp.Status == smpp.StatusOK {
		row.Billed, row.CreditsCharged = s.deps.Billing.Capture(ctx, routed)
	} else {
		s.deps.Billing.Release(ctx, routed)
	}
	if err := s.deps.CDR.Insert(ctx, row); err != nil {
		return fmt.Errorf("connectorpool: write cdr: %w", err)
	}
	s.recordDLRMapping(ctx, routed, resp)
	return nil
}

// recordDLRMapping remembers smsc_msg_id -> message_id after a successful submit, so a later
// deliver_sm (delivery receipt) can be correlated back to this message (step-044). It is best-effort:
// the message is already enroute, so a mapping-write failure — or a non-ROK response, or a response
// carrying no smsc_msg_id — must never fail the record. A write error is logged and counted only by
// the log; the consequence (a later receipt arriving uncorrelated) is handled in step-044. The log
// carries the ids, never the body (invariant a).
func (s *Service) recordDLRMapping(ctx context.Context, r pipeline.RoutedMT, resp smpp.PDU) {
	if resp.Status != smpp.StatusOK {
		return
	}
	body, ok := resp.Body.(*smpp.SubmitSMResp)
	if !ok || body.MessageID == "" {
		return
	}
	if err := s.deps.DLRMap.Put(ctx, body.MessageID, r); err != nil {
		s.deps.Logger.WarnContext(ctx, "connector: dlr mapping write failed, a later receipt will be uncorrelated",
			"message_id", r.MessageID, "connector_id", r.ConnectorID, "err", err)
	}
}

// buildSubmit maps one routed SEGMENT onto a submit_sm. Body already carries the segment's wire
// short_message — the concatenation UDH followed by the encoded content when the message spans several
// segments, the bare encoded content when it does not (internal/pipeline/encoding.Split produced it in
// the resolved encoding), so the connector no longer encodes: it puts the bytes on the wire verbatim.
// Revealing the body here is an audited egress (like the Kafka payload): the plaintext goes onto the
// SMSC wire, never into a log or span. When the segment begins with a UDH, esm_class's UDH indicator is
// set so the SMSC and the handset parse and reassemble it.
func buildSubmit(r pipeline.RoutedMT) *smpp.SubmitSM {
	source, sourceTON, sourceNPI := sourceAddr(r.From)
	sm := &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddr:      source,
		DestinationAddr: r.To,
		DataCoding:      submitDataCoding(r),
	}}
	sm.SourceAddrTON, sm.SourceAddrNPI = sourceTON, sourceNPI
	sm.DestAddrTON, sm.DestAddrNPI = smpp.TONInternational, smpp.NPIISDN
	if r.RegisteredDelivery {
		sm.RegisteredDelivery = smpp.RegisteredDeliveryReceipt
	}
	if r.HasUDH {
		sm.ESMClass = smpp.ESMClassUDHIndicator
	}
	// The SMPP validity_period is a 16-char C-Octet String; a longer value would marshal a PDU with no
	// NUL terminator, which the SMSC rejects by dropping the connection — poisoning the partition on
	// redelivery. REST bounds it (maxLength 16), but guard here too so a malformed mt.routed record can
	// never crash the bind: an over-length value is dropped rather than sent.
	if r.ValidityPeriod != nil && len(*r.ValidityPeriod) <= 16 {
		sm.ValidityPeriod = *r.ValidityPeriod
	}

	body := r.Body.Reveal() // audited: segment wire bytes -> SMSC wire, never logged
	if len(body) > 254 {
		// A segment normally fits in short_message: UCS-2 (<=133 octets) and binary (<=133) always do,
		// and GSM-7 does once bit-packed. Until packing lands, an accented GSM-7 segment carried as
		// unpacked UTF-8 can exceed 254 octets; fall back to message_payload so an over-length PDU never
		// poisons the bind. Concatenation is degraded in that path — the fix is GSM-7 packing (follow-up).
		sm.TLVs.Set(smpp.TagMessagePayload, body)
	} else {
		sm.ShortMessage = body
	}
	return sm
}

// submitDataCoding is the wire data_coding byte. An explicit, in-range client override wins (the
// client is driving the DCS directly); otherwise it is derived from the resolved encoding.
func submitDataCoding(r pipeline.RoutedMT) uint8 {
	if dc := r.DataCoding; dc != nil && *dc >= 0 && *dc <= 255 {
		return uint8(*dc) //nolint:gosec // bounded to 0..255 on the line above
	}
	return dataCoding(r.Encoding)
}

// cdrRow builds the enroute (or failed) CDR row from the submit_sm_resp.
func cdrRow(r pipeline.RoutedMT, resp smpp.PDU) clickhouse.CDRRow {
	connectorID := r.ConnectorID
	status, errorCode := outcome(resp.Status)
	return clickhouse.CDRRow{
		MessageID:    r.MessageID,
		TraceID:      r.TraceID,
		AccountID:    r.AccountID,
		CustomerID:   r.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   r.From,
		DestAddr:     r.To,
		ConnectorID:  &connectorID,
		RouteID:      r.RouteID,
		SubmittedAt:  r.SubmittedAt,
		Status:       status,
		ErrorCode:    errorCode,
		SegmentCount: segmentCount(r.SegmentCount),
		SegmentSeq:   segmentSeq(r.SegmentSeq),
		Encoding:     clickhouse.EncodingOf(r.Encoding),
		Billed:       false,
	}
}

// cancelledRow builds the cancelled CDR row (rank 60) for a message a cancel_sm flagged before
// dispatch. It mirrors cdrRow's identifier projection but records no connector outcome (the message
// was never submitted). It is written in addition to the Canceller's own row: idempotent under
// ReplacingMergeTree (same ORDER BY key and rank), it closes the crash window between flag and row.
func cancelledRow(r pipeline.RoutedMT) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID:    r.MessageID,
		TraceID:      r.TraceID,
		AccountID:    r.AccountID,
		CustomerID:   r.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   r.From,
		DestAddr:     r.To,
		SubmittedAt:  r.SubmittedAt,
		Status:       clickhouse.StatusCancelled,
		SegmentCount: segmentCount(r.SegmentCount),
		SegmentSeq:   segmentSeq(r.SegmentSeq),
		Encoding:     clickhouse.EncodingOf(r.Encoding),
		Billed:       false,
	}
}

// outcome maps a submit_sm_resp command_status to a CDR status. ESME_ROK is enroute; anything else
// is a failed send, with an error_code drawn from the shared platform/errors contract (never an
// ad-hoc string), so a client reading GET /messages sees a documented code.
func outcome(cmdStatus uint32) (clickhouse.Status, *string) {
	if cmdStatus == smpp.StatusOK {
		return clickhouse.StatusEnroute, nil
	}
	code := string(errs.CodeFromSMPPStatus(cmdStatus))
	return clickhouse.StatusFailed, &code
}

// dataCoding derives the SMPP data_coding byte from the resolved encoding, using the shared encoding
// vocabulary (internal/platform/encoding) so the value set does not drift across the pipeline.
func dataCoding(enc string) uint8 {
	switch enc {
	case encoding.UCS2:
		return smpp.DataCodingUCS2
	case encoding.Binary:
		return smpp.DataCodingBinary
	default:
		return smpp.DataCodingGSM7
	}
}

// sourceAddr maps a submitted source address to its wire form and TON/NPI. A numeric MSISDN
// (optionally "+"-prefixed) becomes a plus-stripped international/ISDN address; anything else passes
// through as an alphanumeric sender id. Source normalization proper is an M5 concern — this is only
// the wire typing the SMSC requires, so a "+1206…" MSISDN is not mistyped as an alphanumeric sender.
func sourceAddr(addr string) (wire string, ton, npi uint8) {
	digits := strings.TrimPrefix(addr, "+")
	if digits != "" && isAllDigits(digits) {
		return digits, smpp.TONInternational, smpp.NPIISDN
	}
	return addr, smpp.TONAlphanumeric, smpp.NPIUnknown
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func segmentCount(n int) uint16 {
	if n < 1 {
		return 1
	}
	return uint16(n) //nolint:gosec // segment count is a small positive integer
}

// segmentSeq maps a routed segment's 1-based sequence to the CDR column. A connector row is always a
// dispatched segment, so a missing/zero value defaults to 1 (never 0, which the read path reserves for
// the pre-dispatch message-level row).
func segmentSeq(n int) uint16 {
	if n < 1 {
		return 1
	}
	return uint16(n) //nolint:gosec // segment sequence is a small positive integer
}
