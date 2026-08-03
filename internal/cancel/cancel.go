// Package cancel is the message-cancellation domain shared by the SMPP front door and the outbound
// connector pool. A cancel_sm cancels a message that has NOT yet been dispatched to the SMSC: the
// Canceller records the intent in a Redis flag the connector pool consults before submit_sm, and
// writes a cancelled CDR row (rank 60, superseding accepted under ReplacingMergeTree). There is no
// REST surface — cancellation is an SMPP-only operation (ADR-0009).
//
// The Canceller returns a platform error Code (never a leaked infrastructure error): the SMPP
// boundary maps it once through errs.SMPPStatusForError. An unknown message is ErrMessageNotFound
// (ESME_RINVMSGID), an already-dispatched one is ErrCancelFailed (ESME_RCANCELFAIL), and a repeat
// cancel of an already-cancelled message is a no-op success (ESME_ROK).
package cancel

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// CDRReader reads a message's current lifecycle snapshot, scoped to (customer_id, account_id) so a
// message outside the caller's account reads as absent. *clickhouse.CDRReader satisfies it.
type CDRReader interface {
	Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (clickhouse.CDRRow, bool, error)
}

// CDRWriter appends the cancelled lifecycle row. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// FlagMarker records the cancel intent the connector pool consults before submit_sm. *RedisFlags
// satisfies it.
type FlagMarker interface {
	Mark(ctx context.Context, messageID uuid.UUID) error
}

// Canceller cancels a not-yet-dispatched message. Construct it with NewCanceller.
type Canceller struct {
	reader CDRReader
	writer CDRWriter
	flags  FlagMarker
	logger *slog.Logger
}

// NewCanceller builds a Canceller. A nil logger defaults to slog.Default.
func NewCanceller(reader CDRReader, writer CDRWriter, flags FlagMarker, logger *slog.Logger) *Canceller {
	if logger == nil {
		logger = slog.Default()
	}
	return &Canceller{reader: reader, writer: writer, flags: flags, logger: logger}
}

// Cancel cancels the message if it is still queued. It reads the current CDR snapshot scoped to the
// caller's account and then, by status:
//
//   - absent from the scope         → ErrMessageNotFound (ESME_RINVMSGID)
//   - already cancelled             → no-op success (idempotent)
//   - accepted (still queued)       → flags the intent, writes a cancelled CDR row
//   - enroute or any terminal state → ErrCancelFailed (ESME_RCANCELFAIL)
//
// The flag is written BEFORE the CDR row: the flag is what actually prevents dispatch, so it must be
// durable before Cancel reports success; the CDR row is the visible state and follows. The race with
// a message the connector is dispatching concurrently is intrinsic and out of scope — if the flag
// lands after the connector's cancel check, the CDR still records cancelled (rank 60) though the
// message left. The body is never touched, so nothing can leak (invariant a).
//
// Limitation: cancellation is decided on the accepted CDR projection, which is written off the ingest
// path within tens of ms and is dropped under saturation (internal/ingest). A cancel_sm that arrives
// before that projection is durable reads the message as absent and returns ErrMessageNotFound — the
// same window as the get-message 404. The message is still queued in Kafka and will dispatch; the ESME
// must retry the cancel once the message is observable. The connector, not this projection, is the
// authority on "already dispatched".
//
// That same limitation has a MIRROR side, and step-201c widened it. The enroute row is no longer written
// synchronously by the connector: it is projected off mt.outcome (step-201c, D1). So a message already on
// the wire keeps reading "accepted" for as long as that projection lags — tens of ms in steady state, but
// bounded only by the lag alert (30 s) under ClickHouse saturation. Throughout that window a cancel_sm is
// ACCEPTED for a message that will be delivered, and rank 60 then buries the enroute and delivered rows
// that follow: get-message reports cancelled for ever on a delivered, billed message.
//
// The window pre-dates step-201c (it was the few ms between the connector's cancel check and its
// synchronous write); what changed is its size, so this is a widened exposure and not a new one. It is
// deliberately NOT fixed here (step-201c, D18): the fix requires deciding what cancelled MEANS once the
// connector dispatched anyway, which is a spec decision. Note that lowering the rank is not the fix it
// looks like — 45 still outranks delivered (40). Money is unaffected: charges follow the reserve/capture
// ledger, which is idempotent by message_id and never reads this status.
func (c *Canceller) Cancel(ctx context.Context, customerID, accountID, messageID uuid.UUID) error {
	row, found, err := c.reader.Current(ctx, customerID, accountID, messageID)
	if err != nil {
		c.logger.ErrorContext(ctx, "cancel: read cdr", "message_id", messageID, "err", err)
		return errs.ErrInternal
	}
	if !found {
		return errs.ErrMessageNotFound
	}

	switch row.Status {
	case clickhouse.StatusCancelled:
		return nil // idempotent: already cancelled
	case clickhouse.StatusAccepted:
		// Cancellable: still queued, not yet dispatched.
	default:
		return errs.ErrCancelFailed // enroute or terminal: the message has left the queue
	}

	if err := c.flags.Mark(ctx, messageID); err != nil {
		c.logger.ErrorContext(ctx, "cancel: mark intent", "message_id", messageID, "err", err)
		return errs.ErrInternal
	}
	if err := c.writer.Insert(ctx, cancelledRow(row)); err != nil {
		c.logger.ErrorContext(ctx, "cancel: write cancelled cdr", "message_id", messageID, "err", err)
		return errs.ErrInternal
	}
	return nil
}

// cancelledRow derives the cancelled lifecycle row from the current snapshot: the same identifiers
// and immutable submitted_at, status cancelled (whose rank supersedes accepted), and no error code.
// The writer derives the ReplacingMergeTree version from the status, so the caller never sets it.
func cancelledRow(row clickhouse.CDRRow) clickhouse.CDRRow {
	row.Status = clickhouse.StatusCancelled
	row.ErrorCode = nil
	return row
}
