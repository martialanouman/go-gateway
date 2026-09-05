// Package connectorpool is the outbound SMSC leg: a pool of parallel SMPP binds to one connector. It
// consumes mt.routed, submits each message with submit_sm, and PUBLISHES the outcome on mt.outcome
// (enroute on ESME_ROK, failed otherwise) for the CDR projection to write — it no longer writes that row
// itself, because four synchronous ClickHouse round trips per message on the consumption path capped the
// pool at a fraction of its SMSC throughput, and batching them in place would have made a write failure
// re-submit messages already on the wire (step-201c, D1). It shards a poll batch across bind_pool_size binds by
// hash(message_id) % bind_pool_size, so every segment of a message lands on one bind in order (§7.3);
// each bind is processed by its own worker so the binds submit concurrently (step-124). No fallback and
// no reroute yet — those are later milestones.
package connectorpool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/connector/status"
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
	retryFirstFail sync.Map // retryKey -> time.Time
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
	// Required, not defaulted: since step-201c the outcome publish is the only record that a message
	// left for the SMSC, and billing.Reaper settles orphan reservations against that record. A no-op
	// here would let a pool send SMS it never accounts for — and refusing the publish instead would be
	// worse, because the publish happens after the submit_sm: the send would be redelivered and the
	// same SMS would go out in a loop (D8).
	//
	// The rule this follows: a no-op default is legitimate only when the resulting service is a mode
	// someone would deliberately run. Billing nil is billing opt-out, a documented mode; Throttle or
	// DeadLetter nil is one metric less. A pool that sends without recording is nobody's mode.
	if deps.Producer == nil {
		panic("connectorpool: Deps.Producer is required — mt.outcome carries the CDR's durability and " +
			"the billing reaper settles against it (step-201c D8)")
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
