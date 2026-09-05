package connectorpool

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// ageBase is the instant a message's age is counted from: its immutable accept time, or its replay time
// when an operator re-injected it off the dead-letter topic (step-129).
//
// Both the max-age SLA and the end-to-end latency histogram read it, and they must agree: a message the
// SLA considers seconds old cannot be hours old to the metric. It is also what keeps a drain of parked
// messages from burying the p99 — SubmittedAt on a replay can be arbitrarily far in the past, well past
// the histogram's own ceiling.
func ageBase(r pipeline.RoutedMT) time.Time {
	base := r.SubmittedAt
	if r.ReplayedAt != nil && r.ReplayedAt.After(base) {
		base = *r.ReplayedAt
	}
	return base
}

// expired reports whether a routed message has outlived the gateway's max-age SLA and must be
// dead-lettered instead of submitted (step-129). A zero MaxMessageAge disables the check.
func (s *Service) expired(r pipeline.RoutedMT) bool {
	if s.deps.MaxMessageAge <= 0 {
		return false
	}
	return time.Since(ageBase(r)) > s.deps.MaxMessageAge
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
// heldByACancellation reports whether a token holder means "a cancel_sm owns this message".
//
// Anything that is not a free token and not OUR OWN is a cancellation: HolderCancel, or a holder this
// build cannot name — including the plain "1" the previous build wrote into this very key, which a
// rolling deploy can still surface. Reading either as free would put a cancelled message on the wire
// (ADR-0013, DN8: when in doubt, refuse).
//
// HolderDispatched is deliberately excluded: that is our own token, re-read after a Kafka redelivery,
// and treating it as a cancellation would record a cancellation of a message we have already sent.
func heldByACancellation(holder cancel.Holder) bool {
	return holder != cancel.HolderNone && holder != cancel.HolderDispatched
}

// cancelBeforeDispatch records a cancellation and commits the record without submitting.
//
// Writing the row HERE (not only in the Canceller) is what makes the skip safe: it is idempotent under
// ReplacingMergeTree (rank 60, collapsing with the Canceller's row) and closes the window where the
// Canceller crashed — or failed its ClickHouse write — after claiming the token but before writing the
// row. Otherwise the message would be neither sent nor recorded, leaving the CDR stuck on accepted.
//
// A cancelled message is never sent, so its reservation is released (step-146). Fail-open, gated on
// Billable — a billing-disabled or unreserved message makes no call.
func (s *Service) cancelBeforeDispatch(ctx context.Context, r pipeline.RoutedMT) error {
	s.deps.Billing.Release(ctx, r)
	if err := s.deps.CDR.Insert(ctx, cancelledRow(r)); err != nil {
		return fmt.Errorf("connectorpool: write cancelled cdr: %w", err)
	}
	s.deps.Logger.InfoContext(ctx, "connector: message cancelled before dispatch", "message_id", r.MessageID)
	return nil
}

func (s *Service) healthRetry(ctx context.Context, rec kafka.Record, r pipeline.RoutedMT, cause error) error {
	if s.deps.RetryWindow > 0 {
		first, _ := s.retryFirstFail.LoadOrStore(retryKeyOf(rec), time.Now())
		if time.Since(first.(time.Time)) > s.deps.RetryWindow {
			return s.deadLetterWith(ctx, r, errs.ErrRetriesExhausted)
		}
	}
	return fmt.Errorf("connectorpool: connector-health redelivery: %w", cause)
}

// retryKey identifies a record for the retry window by its immutable (partition, offset).
type retryKey struct {
	partition int32
	offset    int64
}

func retryKeyOf(rec kafka.Record) retryKey {
	return retryKey{partition: rec.Partition, offset: rec.Offset}
}

// processOne submits a single routed segment on the given bind and records its outcome. It returns a
// non-nil error only on a transient fault (bad decode, dead bind, transient SMSC rejection, or a failed
// outcome publish) so the record is left uncommitted for redelivery; a terminal SMSC failure is
// published on mt.outcome and returns nil. It is the per-record body the batch handler runs, one shard
// at a time.
func (s *Service) processOne(ctx context.Context, b *bind, bindIndex int, rec kafka.Record) (err error) {
	ctx, span := s.deps.Tracer.Start(ctx, "connector.submit")
	defer span.End()
	// A transient fault leaves the record uncommitted for redelivery; marking the span failed is what makes
	// it survive head sampling (step-181) and what get-message-trace looks for.
	defer func() { observability.RecordSpanError(span, err) }()

	// A committed outcome (nil return) means this offset advances and will not be redelivered, so any
	// retry-window entry for it is dead weight — clear it. healthRetry keeps its entry only by returning a
	// non-nil error (redelivery); every nil path drops it, so the map never outlives a record (step-129).
	defer func() {
		if err == nil {
			s.retryFirstFail.Delete(retryKeyOf(rec))
		}
	}()

	routed, err := pipeline.DecodeRouted(rec)
	if err != nil {
		return fmt.Errorf("connectorpool: decode mt.routed: %w", err)
	}

	if done, err := s.preDispatch(ctx, span, bindIndex, routed); done {
		return err
	}

	resp, err := b.Submit(ctx, buildSubmit(routed))
	if err != nil {
		// A dead bind, a write failure or a timeout is transient and a connector-health failure for the
		// breaker (no response came back). With a fallback chain, reroute to the next connector; without
		// one, do not commit so the message is reprocessed after a restart (at-least-once).
		s.feedBreaker(bindIndex, 0, true)
		if len(routed.FallbackChain) > 0 {
			observability.RecordSpanError(span, errs.ErrServiceUnavailable)
			return s.reroute(ctx, routed, errs.ErrServiceUnavailable)
		}
		return s.healthRetry(ctx, rec, routed, fmt.Errorf("connectorpool: submit_sm: %w", err))
	}

	// The end-to-end span (spec §1.2, "submission → SMSC delivery attempt") closes HERE, on the
	// submit_sm_resp — the last instant that belongs to the delivery. Everything below it (breaker,
	// throttle, billing settle, CDR write) is our own bookkeeping, and folding a stalled ClickHouse
	// writer into a delivery-latency budget would make the gateway look slow for someone else's fault.
	// One nanotime read per submit, no allocation; it is only OBSERVED on a terminal outcome below.
	// Clamped at zero: the accept stamp was written by another pod and survived a JSON round trip, so
	// it carries no monotonic reading and time.Since falls back to the wall clock. A pod whose clock
	// trails the ingest pod's by more than the send took yields a negative duration — which Prometheus
	// accepts into the lowest bucket, so the p99 would read "under 10 ms" and the budget would pass
	// trivially. Clamping keeps that skew from flattering the figure; it still inflates _sum on the
	// other side, which is the honest direction.
	e2e := max(time.Since(ageBase(routed)), 0)

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
		observability.RecordSpanError(span, errs.CodeFromSMPPStatus(resp.Status))
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

	// The SMS is on the SMSC's wire from here on, so remember smsc_msg_id -> message_id BEFORE any
	// bookkeeping that can fail: the SMSC will send its receipt whether or not our settle and our publish
	// succeed, and a receipt that arrives with no mapping is orphaned. It sat after the (fail-closed) CDR
	// write until step-201c, which meant a storage fault also cost us the receipt of a message that really
	// was delivered.
	s.recordDLRMapping(ctx, routed, resp)

	// Settle the reservation on the terminal outcome (step-146): capture a sent message, release a
	// permanently-failed one. Both FAIL OPEN — neither returns an error — so a billing fault can never turn
	// this committed outcome into a redelivery that re-submits the message (a duplicate SMS). A
	// billing-disabled message makes no call. Capture fills billed/credits_charged on the outcome; the
	// failed path leaves them false/nil (the reserve refund happens durably in billing-svc, not here).
	event := submitOutcome(routed, resp)
	if resp.Status == smpp.StatusOK {
		event.Billed, event.CreditsCharged = s.deps.Billing.Capture(ctx, routed)
	} else {
		// A permanent SMSC rejection: a failed CDR is written and the offset commits, so this is the only
		// place the span learns the message was refused.
		observability.RecordSpanError(span, errs.CodeFromSMPPStatus(resp.Status))
		s.deps.Billing.Release(ctx, routed)
	}
	s.observeSubmit(resp, e2e)
	// Publish the outcome instead of writing the CDR here (step-201c, D1). The row is now a PROJECTION:
	// a dedicated consumer batches mt.outcome into ClickHouse, which is what moves the batching to where a
	// redelivery only rewrites a row instead of re-submitting an SMS.
	//
	// The produce is fail-closed and acked before the offset commits — the guarantee the synchronous CDR
	// write used to provide, on a failure domain DECORRELATED from ClickHouse saturation (a Kafka the pool
	// cannot produce to is a Kafka it is not consuming mt.routed from either, so there is nothing in flight
	// to duplicate). It is not fail-open: without a recorded outcome the billing reaper (step-190) settles
	// nothing, and a customer's reservation would be held for good. The residual window — a crash between
	// the submit_sm and this ack — is the bounded duplicate ADR-0012 assumes.
	outRec, err := pipeline.EncodeOutcome(event)
	if err != nil {
		return fmt.Errorf("connectorpool: encode mt.outcome: %w", err)
	}
	if err := s.deps.Producer.Produce(ctx, outRec); err != nil {
		return fmt.Errorf("connectorpool: publish mt.outcome: %w", err)
	}
	return nil
}

// preDispatch settles everything that decides a record's fate WITHOUT putting it on the wire: the
// connector filter, the max-age SLA, the cancel token, a reroute off an open breaker, the AIMD wait.
// done reports that the record is settled and processOne must return err as it is.
func (s *Service) preDispatch(ctx context.Context, span trace.Span, bindIndex int, routed pipeline.RoutedMT) (done bool, err error) {
	// Per-connector addressing (step-125, option B): the pool's group consumes ALL of mt.routed, so a
	// record for another connector — including one another pool just rerouted — is not ours to send.
	// Skip and commit it. A message rerouted TO this connector carries our id and is processed normally.
	// A pool with no ConnectorID configured (uuid.Nil) filters nothing and processes every record (the
	// pre-step-125 single-connector behaviour).
	if s.deps.ConnectorID != uuid.Nil && routed.ConnectorID != s.deps.ConnectorID {
		return true, nil
	}

	// Gateway max-age SLA (step-129): a message that has outlived MaxMessageAge — whether it aged out in
	// throttle backpressure or churned across reconnects — is dead-lettered as delivery_expired rather
	// than submitted, so nothing lingers on the data plane forever. Checked before any submit work.
	if s.expired(routed) {
		// Ask who holds the cancel token before parking it — READ, never claim (step-245). A cancelled
		// message must be recorded as cancelled, not as expired: parking it is what left the residual the
		// replay guard cannot see, because the Canceller claims the token BEFORE writing its CDR row, so a
		// failed write leaves the token as the only evidence the message was ever cancelled. The dispatch
		// path below repairs exactly that case; this branch used to be the one that skipped the repair.
		//
		// Reading is admissible HERE and nowhere before a send. ADR-0013 requires the claim because
		// between a read and a submit_sm a cancel_sm can win, and the message goes out anyway. This branch
		// has already decided not to send, so a raced read can only misfile a message that is going
		// nowhere. Claiming instead would put a `dispatched` token on it and refuse legitimate cancel_sm
		// for the whole 5-minute TTL — on the message that most needs cancelling.
		//
		// Fail-open on a Redis fault, like the claim below: cancellation is best-effort and an outage must
		// not stop the pool from clearing its backlog. The replay guard (step-240) is the net behind this.
		if holder, perr := s.deps.CancelFlags.Peek(ctx, routed.MessageID); perr != nil {
			s.deps.Logger.WarnContext(ctx, "connector: cancel-token peek failed, dead-lettering as expired",
				"message_id", routed.MessageID, "err", perr)
		} else if heldByACancellation(holder) {
			return true, s.cancelBeforeDispatch(ctx, routed)
		}
		// Terminal outcomes commit the offset, so processOne returns nil and the deferred recorder above
		// sees nothing. They are marked here instead — the step asks for error/REJECT/timeout, and a
		// permanent reject is exactly what an operator goes looking for.
		observability.RecordSpanError(span, errs.ErrDeliveryExpired)
		return true, s.deadLetterWith(ctx, routed, errs.ErrDeliveryExpired)
	}

	// Claim the cancel token before putting anything on the wire (ADR-0013). Claiming, not reading, is
	// what makes this decisive: a cancel_sm arriving from here on loses the token and refuses, instead
	// of reading a stale `accepted` off the CDR projection and recording a cancellation of a message
	// already gone (step-209).
	//
	// Taking it HERE — before the reroute check and the AIMD wait — costs a few false negatives: a
	// message that ends up rerouted, or that waits on the throttle, is no longer cancellable. That
	// bias is deliberate. A false negative costs the ESME an ESME_RCANCELFAIL and writes nothing
	// false; the opposite false positive is the bug this closes. When in doubt, refuse.
	//
	// Redis is best-effort here: cancellation is itself best-effort (an already-dispatched message
	// cannot be recalled), so a claim failure fails OPEN — we log and dispatch rather than halt all
	// outbound delivery on a Redis outage. Residual, accepted: the cancel_sm may then win a token it
	// should not have and write the wrong row again, bounded to Redis outages.
	holder, err := s.deps.CancelFlags.Claim(ctx, routed.MessageID, cancel.HolderDispatched)
	if err != nil {
		s.deps.Logger.WarnContext(ctx, "connector: cancel-token claim failed, dispatching anyway",
			"message_id", routed.MessageID, "err", err)
	} else if heldByACancellation(holder) {
		return true, s.cancelBeforeDispatch(ctx, routed)
	}

	// Reroute before submitting if this connector's own breaker is already open and the message carries a
	// fallback chain (step-125): no point pacing and submitting to a connector we know is down — advance
	// the chain now. Uses the LOCAL breaker (this pod's view), so the hot path never reads Redis.
	if len(routed.FallbackChain) > 0 && s.breakers != nil && s.breakers[bindIndex].State() == breaker.Open {
		observability.RecordSpanError(span, errs.ErrServiceUnavailable)
		return true, s.reroute(ctx, routed, errs.ErrServiceUnavailable)
	}

	// Adaptive throttle (step-086): pace to the connector's current AIMD send rate before submitting,
	// so a throttled SMSC slows our outbound rather than being hammered. It blocks at most one send
	// interval and honours ctx; it NEVER cuts the bind (that is the circuit breaker's job, M8).
	if s.aimd != nil {
		if err := s.aimd.acquire(ctx); err != nil {
			return true, fmt.Errorf("connectorpool: throttle wait: %w", err)
		}
	}
	return false, nil
}

// recordDLRMapping remembers smsc_msg_id -> message_id after a successful submit, so a later
// deliver_sm (delivery receipt) can be correlated back to this message (step-044). It is called as soon
// as the outcome is terminal, BEFORE the settle and the outcome publish, because the SMSC will send its
// receipt regardless of how our own bookkeeping fares. It is best-effort:
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

// stream runs fn against the emitter when one is configured. One nil check in one place, so no call site can
// forget it and no emission can be mistaken for something the send path depends on.
func (s *Service) stream(fn func(StreamEmitter)) {
	if s.deps.Stream == nil {
		return
	}
	fn(s.deps.Stream)
}
