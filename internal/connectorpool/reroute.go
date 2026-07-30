package connectorpool

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// RerouteLimiter gates a reroute on the target connector's throughput ceiling (step-126). AllowConnector
// consumes `segments` of the target's token bucket and reports whether they were available; a reroute
// whose target has no capacity is PARKED on mt.reroute-park instead of piling onto it, and the drainer
// replays parked messages at the same ceiling. *ratelimit.Enforcer satisfies it; a nil RerouteLimiter
// disables parking (every reroute goes straight to mt.routed, the step-125 behaviour).
type RerouteLimiter interface {
	AllowConnector(ctx context.Context, connectorID uuid.UUID, segments int) bool
}

// BreakerState reports whether a connector's cross-pod breaker aggregate is open (breaker:state, step-122).
// The connector pool consults it ONLY on a reroute (a rare event), to skip chain candidates that are
// themselves open — never on the per-message hot path. A *redis-backed reader satisfies it; a nil
// BreakerState is treated as "nothing is open", so reroute simply advances to the next chain entry.
type BreakerState interface {
	IsOpen(ctx context.Context, connectorID uuid.UUID) (bool, error)
}

// rerouteClass decides what a submit outcome means for the fallback chain (step-125). It is deliberately
// an explicit table, distinct from both the breaker health classification and the Kafka-redelivery
// retryable flag: the business rule "which SMSC rejections warrant trying another connector" lives here.
type rerouteClass int

const (
	// keepSame: transient backpressure (throttled) — redeliver on the SAME connector (the pre-step-125
	// path). Not a fallback-chain event.
	keepSame rerouteClass = iota
	// failover: a connector-health fault — this connector is sick, try the next one in the chain.
	failover
	// terminal: a permanent per-message rejection — no connector would accept it, so fail outright.
	terminal
)

// classifyReroute maps a submit_sm_resp command_status to a fallback-chain decision. Connector-health
// faults (system error, submit failure, bind failure) fail over to another connector. A rate-limit
// throttle OR a full SMSC queue is transient backpressure on an otherwise-healthy connector, so it
// redelivers in place (bouncing every queue-full to a sibling would split traffic and churn, and it
// matches the no-chain redelivery path). An invalid destination / source / length is permanent
// everywhere, so it fails; any unclassified status also fails rather than hammering the whole fleet with
// an outcome we cannot reason about.
func classifyReroute(status uint32) rerouteClass {
	switch status {
	case errs.StatusThrottled, errs.StatusMsgQueueFull:
		return keepSame
	case errs.StatusSysErr, errs.StatusSubmitFail, errs.StatusBindFail:
		return failover
	default:
		return terminal
	}
}

// nextTarget returns the next connector to try after current in chain, skipping any whose breaker is
// open (so a cascade of open connectors is resolved in one hop, not a Kafka ping-pong per step). It
// returns the remaining chain AFTER the chosen target (to carry forward) and ok=false when the chain is
// exhausted. A BreakerState read error is treated as "not open" — better to attempt a reroute than to
// dead-letter on a transient Redis blip.
func (s *Service) nextTarget(ctx context.Context, current uuid.UUID, chain []uuid.UUID) (target uuid.UUID, rest []uuid.UUID, ok bool) {
	// Position just after the current connector; if current is absent from the chain, start at the top.
	start := 0
	for i, id := range chain {
		if id == current {
			start = i + 1
			break
		}
	}
	for i := start; i < len(chain); i++ {
		cand := chain[i]
		if cand == current {
			continue
		}
		if s.deps.BreakerState != nil {
			if open, err := s.deps.BreakerState.IsOpen(ctx, cand); err == nil && open {
				continue // skip a candidate that is itself open
			}
		}
		return cand, chain[i+1:], true
	}
	return uuid.Nil, nil, false
}

// reroute handles a degraded target: it advances the fallback chain to the next healthy connector,
// records the reroute in the CDR, and republishes the message there. When the chain is exhausted it
// dead-letters instead. Either way the original record is committed by the caller (return nil) so a
// redelivery does not re-send on the dead connector. The order is CDR → durable produce (acked) →
// commit: a crash between produce and commit redelivers — at-least-once, so the SMSC may see a duplicate
// submit (a possible duplicate SMS, the accepted §7.3 property; the CDR collapses under
// ReplacingMergeTree and billing is idempotent by message_id, but the extra submit itself is not undone)
// — but never loses. reason is the gateway code recorded on the reroute row.
func (s *Service) reroute(ctx context.Context, r pipeline.RoutedMT, reason errs.Code) error {
	target, rest, ok := s.nextTarget(ctx, r.ConnectorID, r.FallbackChain)
	if !ok {
		return s.deadLetter(ctx, r)
	}
	if err := s.deps.CDR.Insert(ctx, reroutedRow(r, reason)); err != nil {
		return fmt.Errorf("connectorpool: write rerouted cdr: %w", err)
	}
	next := r
	next.ConnectorID = target
	next.FallbackChain = rest
	rec, err := pipeline.EncodeRouted(next)
	if err != nil {
		return fmt.Errorf("connectorpool: encode reroute: %w", err)
	}
	// Park the excess (step-126): if the target has no send capacity now, durably queue the reroute on
	// mt.reroute-park instead of piling it onto an already-saturated connector. The bounded drainer
	// replays it to mt.routed at the target's ceiling. Same key (message_id) → order preserved.
	parked := s.deps.RerouteLimiter != nil && !s.deps.RerouteLimiter.AllowConnector(ctx, target, r.SegmentCount)
	if parked {
		rec.Topic = kafka.TopicMTReroutePark
	}
	if err := s.deps.Producer.Produce(ctx, rec); err != nil {
		return fmt.Errorf("connectorpool: produce reroute: %w", err)
	}
	s.deps.Logger.InfoContext(ctx, "connector: rerouted to next fallback connector",
		"message_id", r.MessageID, "from_connector", r.ConnectorID, "to_connector", target,
		"reason", string(reason), "parked", parked)
	return nil
}

// deadLetter parks a message on mt.dead-letter for the fallback-chain-exhausted reason.
func (s *Service) deadLetter(ctx context.Context, r pipeline.RoutedMT) error {
	return s.deadLetterWith(ctx, r, errs.ErrFallbackExhausted)
}

// deadLetterWith parks the message on mt.dead-letter with the given reason: a final failed CDR row, the
// reason carried in the dead_letter_reason HEADER (so a replay can strip it and re-record), a counted
// metric and a span event — so a dead-lettered message is never silently lost (§1.11, step-129). CDR →
// produce (acked) → the caller commits (return nil): a crash redelivers, never loses.
func (s *Service) deadLetterWith(ctx context.Context, r pipeline.RoutedMT, reason errs.Code) error {
	// A dead-lettered message is terminally undelivered (fallback exhausted, retries exhausted, expired), so
	// release its reservation (step-146). Fail-open, gated on Billable. A later dead-letter replay that
	// succeeds would capture against an already-released reservation (idempotent → credits_charged=0, a free
	// delivery); the replay tool's re-reserve semantics are a documented follow-up (step-129).
	s.deps.Billing.Release(ctx, r)
	if err := s.deps.CDR.Insert(ctx, failedRow(r, reason)); err != nil {
		return fmt.Errorf("connectorpool: write failed cdr: %w", err)
	}
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		return fmt.Errorf("connectorpool: encode dead-letter: %w", err)
	}
	rec.Topic = kafka.TopicMTDeadLetter
	rec.Headers = append(rec.Headers, kafka.Header{Key: kafka.HeaderDeadLetterReason, Value: []byte(reason)})
	if err := s.deps.Producer.Produce(ctx, rec); err != nil {
		return fmt.Errorf("connectorpool: produce dead-letter: %w", err)
	}
	s.deps.DeadLetter.Inc(string(reason))
	trace.SpanFromContext(ctx).AddEvent("dead_letter", trace.WithAttributes(attribute.String("reason", string(reason))))
	s.deps.Logger.WarnContext(ctx, "connector: message dead-lettered",
		"message_id", r.MessageID, "from_connector", r.ConnectorID, "reason", string(reason))
	return nil
}

// reroutedRow is the CDR row for a reroute off a degraded connector: rank 30 (rerouted), superseded by
// the final outcome on the new connector. It records the FAULTY connector and the reason; the target
// connector appears on its own enroute row written by the receiving pool.
func reroutedRow(r pipeline.RoutedMT, reason errs.Code) clickhouse.CDRRow {
	return outcomeRow(r, clickhouse.StatusRerouted, reason)
}

// failedRow is the CDR row for a terminal outcome (fallback chain exhausted): rank 50 (failed).
func failedRow(r pipeline.RoutedMT, reason errs.Code) clickhouse.CDRRow {
	return outcomeRow(r, clickhouse.StatusFailed, reason)
}

// outcomeRow projects a routed message onto a CDR row with the given status and error code, mirroring
// cdrRow's identifier projection (the faulty connector, the segment coordinates). It carries no body.
func outcomeRow(r pipeline.RoutedMT, status clickhouse.Status, reason errs.Code) clickhouse.CDRRow {
	connectorID := r.ConnectorID
	code := string(reason)
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
		ErrorCode:    &code,
		SegmentCount: segmentCount(r.SegmentCount),
		SegmentSeq:   segmentSeq(r.SegmentSeq),
		Encoding:     clickhouse.EncodingOf(r.Encoding),
		Billed:       false,
	}
}
