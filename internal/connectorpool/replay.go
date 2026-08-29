package connectorpool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// ReplayConsumer consumes mt.dead-letter for the replay tool. *kafka.Consumer satisfies it.
type ReplayConsumer interface {
	Run(ctx context.Context, handle kafka.Handler) error
}

// CancelledReader reads a parked message's current lifecycle snapshot, so the replay can tell a message
// that merely failed from one the customer cancelled. *clickhouse.CDRReader satisfies it.
//
// Current, not MessageStatus: the dead-letter envelope carries CustomerID and AccountID, and Current
// filters on those — the sorting-key prefix — where MessageStatus resolves by message id alone and is
// documented as a scan ("rare, non-hot-path admin operation"). A drain reads once per record, so the
// scoped reader is the one to use.
type CancelledReader interface {
	Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (clickhouse.CDRRow, bool, error)
}

// unwiredReader is the default when no CDR reader is injected. It REFUSES rather than allowing: a
// binary wired without a reader would otherwise replay cancelled messages in silence, which is the
// exact defect this guard exists to prevent. Failing on the first record is loud, immediate, and
// cannot be mistaken for a healthy drain.
type unwiredReader struct{}

func (unwiredReader) Current(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (clickhouse.CDRRow, bool, error) {
	return clickhouse.CDRRow{}, false, errors.New("replayer has no CDR reader: it cannot tell a cancelled message from a failed one")
}

// Replayer re-injects dead-lettered MT messages back onto mt.routed for an operator (step-129). It
// consumes mt.dead-letter and, for each record, drops the dead_letter_reason header (a later
// dead-lettering records a fresh reason), stamps a replayed_at header, and republishes verbatim to
// mt.routed under the same message_id key. The replay therefore stays correlated — same message_id and
// trace_id, so billing remains idempotent (invariant c) — and ordered (same partition key). The body
// rides only the record value, never a header (invariant a): a plain DecodeRouted → EncodeRouted
// round-trip carries it unchanged and cannot leak it.
//
// The pool's max-age SLA bases expiry on max(SubmittedAt, replayed_at), so a replay after a long outage
// is NOT instantly re-expired on the immutable SubmittedAt — the reason the replayed_at stamp exists.
//
// It does NOT replay every record. A message the customer cancelled before it was parked stays parked,
// and so does one whose CDR row has gone (step-240) — see mayReplay for the three verdicts and why the
// CDR, not the cancel token, is what can answer this. The token expires after 72h, which is precisely
// the delay past which a replay becomes dangerous.
//
// One-shot in spirit: a replay produces to mt.routed, never back to mt.dead-letter, so the tool cannot
// feed its own tail. Fresh dead-letters that arrive from live failures during a run are genuinely dead
// and get replayed too; the operator stops the tool (ctx cancel) once the parked backlog is drained.
type Replayer struct {
	producer Producer
	cdr      CancelledReader
	logger   *slog.Logger
	now      func() time.Time
	count    atomic.Int64
	refused  atomic.Int64
	absent   atomic.Int64
}

// ReplayerDeps are the replayer's collaborators.
type ReplayerDeps struct {
	Producer Producer
	// CDR tells whether a parked message was cancelled before it was parked. Nil wires a reader that
	// always errors, so a miswired binary stops instead of replaying cancellations.
	CDR    CancelledReader
	Logger *slog.Logger
	// Now overrides the replay clock (tests). Nil uses time.Now.
	Now func() time.Time
}

// NewReplayer builds a replayer. A nil producer is a programming error (the caller wires a real one);
// a nil logger defaults to slog.Default and a nil clock to time.Now. A nil CDR reader does NOT default
// to "no guard" — see unwiredReader.
func NewReplayer(deps ReplayerDeps) *Replayer {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.CDR == nil {
		deps.CDR = unwiredReader{}
	}
	return &Replayer{producer: deps.Producer, cdr: deps.CDR, logger: deps.Logger, now: deps.Now}
}

// Replayed reports how many records have been replayed so far (for the tool's final summary).
func (r *Replayer) Replayed() int64 { return r.count.Load() }

// Refused reports how many records were left parked because the message had been cancelled.
func (r *Replayer) Refused() int64 { return r.refused.Load() }

// RefusedAbsent reports how many records were left parked because no CDR row could be found for them.
// A non-zero count means the invariant behind the guard has been broken somewhere — see handle.
func (r *Replayer) RefusedAbsent() int64 { return r.absent.Load() }

// Run consumes mt.dead-letter until ctx is cancelled, replaying each record to mt.routed. It is a
// supervised worker with a context stop condition — it starts no unbounded goroutine.
func (r *Replayer) Run(ctx context.Context, consumer ReplayConsumer) error {
	return consumer.Run(ctx, r.handle)
}

func (r *Replayer) handle(ctx context.Context, rec kafka.Record) error {
	routed, err := pipeline.DecodeRouted(rec)
	if err != nil {
		// Skip-and-commit a record we cannot decode: an already-dead, malformed record must not wedge the
		// drain of the rest of the backlog. Logged (ids unavailable — the value did not parse), never a body.
		r.logger.WarnContext(ctx, "connector: skipping undecodable dead-letter record", "partition", rec.Partition, "offset", rec.Offset, "err", err)
		return nil
	}
	if replay, err := r.mayReplay(ctx, routed); err != nil || !replay {
		return err
	}
	// Stamp the replay time: EncodeRouted emits it as the replayed_at header and omits the
	// dead_letter_reason header entirely (it is not a RoutedMT field), so the round-trip both strips the
	// old reason and rebases the max-age clock in one step.
	now := r.now()
	routed.ReplayedAt = &now
	out, err := pipeline.EncodeRouted(routed)
	if err != nil {
		return fmt.Errorf("connectorpool: replay encode: %w", err)
	}
	if err := r.producer.Produce(ctx, out); err != nil {
		return fmt.Errorf("connectorpool: replay produce: %w", err)
	}
	r.count.Add(1)
	r.logger.InfoContext(ctx, "connector: replayed a dead-lettered message",
		"message_id", routed.MessageID, "connector_id", routed.ConnectorID)
	return nil
}

// mayReplay decides whether a parked message may go back on the wire. It returns (false, nil) for a
// settled refusal — the caller commits the offset and moves on — and a non-nil error when the verdict
// could not be established, which leaves the offset uncommitted and stops the tool.
//
// It costs ONE read per record, including each segment of a multi-segment message. Collapsing the
// segments (they share the partition key, so they arrive adjacent) would cut that, but the saving
// depends entirely on how many segments the traffic carries — an optimisation to measure, not to
// assume. As it stands, a drain of 100 000 records is 100 000 aggregate queries in series.
func (r *Replayer) mayReplay(ctx context.Context, routed pipeline.RoutedMT) (bool, error) {
	row, found, err := r.cdr.Current(ctx, routed.CustomerID, routed.AccountID, routed.MessageID)
	switch {
	case err != nil:
		// Fail closed, and stop rather than skip. The consumer commits only the prefix it handled, so
		// returning here leaves this record's offset untouched: the tool exits, reports what it replayed,
		// and a re-run resumes on exactly this message. Skipping and committing would be the one true
		// silent loss in this design, and for a message that is most likely legitimate.
		return false, fmt.Errorf("connectorpool: replay cannot read the status of %s, refusing to replay blind: %w",
			routed.MessageID, err)

	case !found:
		// deadLetterWith writes the failed CDR row BEFORE producing to mt.dead-letter, from this same
		// RoutedMT — so every parked record has a row in the very scope read above. An absent row cannot
		// come from the normal path: retention dropped it (90 days) or a GDPR erasure removed it. Putting
		// such a message back on the wire is worse than leaving it parked, and the separate counter makes
		// the invariant falsifiable: if it ever moves, something upstream broke it.
		r.absent.Add(1)
		r.logger.WarnContext(ctx, "connector: refusing to replay a message with no CDR row",
			"message_id", routed.MessageID, "connector_id", routed.ConnectorID)
		return false, nil

	case neverLeftTheGateway(row.Status):
		// The message was cancelled (or rejected) before it was parked, so replaying it would put a
		// message on the wire that the customer was told would not be sent. Past the 72h cancel-token
		// TTL nothing downstream would stop it: the connector would claim a free token and dispatch.
		//
		// Reading this from the CDR is sound for a reason that is structural, not a matter of timing.
		// ADR-0013 refuses to arbitrate a LIVE cancellation on the CDR because the projection lags — but
		// what lags is enroute/delivered, projected off mt.outcome since step-201c. The cancelled row is
		// written synchronously by its own author (internal/cancel), and it is the only row this guard
		// reads.
		//
		// RESIDUAL, deliberately left open (see the step-245 follow-up): the Canceller claims the token
		// BEFORE writing the row, and a retry after a failed write returns success without writing it. A
		// message in that state reads `failed` here and IS replayed. Closing it means consulting the token
		// in the pool's expiry branch, which touches ground ADR-0013 holds out of scope.
		r.refused.Add(1)
		r.logger.WarnContext(ctx, "connector: refusing to replay a message that never left the gateway",
			"message_id", routed.MessageID, "status", string(row.Status))
		return false, nil
	}
	return true, nil
}

// neverLeftTheGateway reports whether a status means the message was stopped before it reached the SMSC.
//
// Both members matter, and it is one predicate rather than two special cases. `rejected` is not a
// widening of scope: the message-level aggregate resolves status by a fixed precedence that puts
// rejected BEFORE cancelled (internal/storage/clickhouse/cdr.go), so a message carrying both rows reads
// `rejected` and a guard testing only for `cancelled` would wave it through. That this is unreachable
// today — a rejected message never reaches mt.routed — is a property of another service, and this guard
// should not rest on it.
//
// `delivered` is deliberately NOT here. Every parked record carries at least one `failed` row, and
// `failed` is tested before `delivered` in that same precedence, so no dead-letter record can ever read
// `delivered`: the branch would be unreachable code that no honest test could turn red.
func neverLeftTheGateway(s clickhouse.Status) bool {
	return s == clickhouse.StatusCancelled || s == clickhouse.StatusRejected
}
