// Package cancel is the message-cancellation domain shared by the SMPP front door and the outbound
// connector pool. A cancel_sm cancels a message that has NOT yet been dispatched to the SMSC.
//
// The two sides arbitrate on a single-winner Redis token rather than trusting the CDR projection,
// which lags (ADR-0013): the connector claims it as HolderDispatched before submit_sm, the Canceller
// as HolderCancel, and the first claim wins. Only a Canceller that WINS writes the cancelled CDR row
// (rank 60, superseding accepted under ReplacingMergeTree); one that loses refuses and writes
// nothing, because that row would otherwise bury the enroute and delivered rows that follow.
//
// There is no REST surface — cancellation is an SMPP-only operation (ADR-0009).
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

// Claimer arbitrates the single-winner cancel token the connector pool also claims before submit_sm.
// *RedisFlags satisfies it.
type Claimer interface {
	Claim(ctx context.Context, messageID uuid.UUID, as Holder) (Holder, error)
}

// Canceller cancels a not-yet-dispatched message. Construct it with NewCanceller.
type Canceller struct {
	reader CDRReader
	writer CDRWriter
	flags  Claimer
	logger *slog.Logger
}

// NewCanceller builds a Canceller. A nil logger defaults to slog.Default.
func NewCanceller(reader CDRReader, writer CDRWriter, flags Claimer, logger *slog.Logger) *Canceller {
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
//   - enroute or any terminal state → ErrCancelFailed (ESME_RCANCELFAIL)
//   - accepted (may still be queued) → decided by the cancel token, below
//
// On accepted the status alone does not settle it, so Cancel claims the token and branches on who
// holds it:
//
//   - token was free      → writes a cancelled CDR row, success (ESME_ROK)
//   - held by HolderCancel → our own earlier intent: no-op success, nothing written
//   - held by anyone else  → ErrCancelFailed (ESME_RCANCELFAIL), NOTHING written
//
// The status read above is a NECESSARY but not sufficient guard, because the projection it reads
// lags. The enroute row is no longer written synchronously by the connector: it is projected off
// mt.outcome (step-201c), so a message already on the wire keeps reading "accepted" for as long as
// that projection lags — tens of ms in steady state, bounded only by the lag alert (30 s) under
// ClickHouse saturation. Reading "accepted" therefore does NOT prove the message is still queued.
//
// The cancel token is the guard that does. It is single-winner (ADR-0013): the connector claims it as
// HolderDispatched before submit_sm, this Canceller claims it as HolderCancel, and whoever gets there
// first wins. Losing to HolderDispatched means the message is already gone — ErrCancelFailed
// (ESME_RCANCELFAIL, exactly what §6.22 prescribes for "already submitted"), and CRUCIALLY no CDR row.
//
// No row, because a cancelled row ranks 60 — above delivered (40) and failed (50) — and
// ReplacingMergeTree keeps the max rank whatever the insertion order. One wrongly written row would
// bury every state that follows and make get-message report cancelled for ever on a delivered,
// billed message (step-209). `cancelled` means "never left"; writing it on a dispatched message is
// false, not merely mis-ranked.
//
// The token is claimed BEFORE the CDR row: it is what actually prevents dispatch, so it must be
// durable before Cancel reports success; the row is the visible state and follows. The body is never
// touched, so nothing can leak (invariant a).
//
// Remaining window (unchanged by step-209): the accepted projection is written off the ingest path
// and is dropped under saturation (internal/ingest). A cancel_sm arriving before it is durable reads
// the message as absent and returns ErrMessageNotFound — the same window as the get-message 404. The
// message is still queued in Kafka and will dispatch; the ESME must retry once it is observable.
// Claiming the token unconditionally would close that window but break strict per-account scoping (an
// account could cancel another's message): the scoping wins (ADR-0009).
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

	switch holder, err := c.flags.Claim(ctx, messageID, HolderCancel); {
	case err != nil:
		c.logger.ErrorContext(ctx, "cancel: claim token", "message_id", messageID, "err", err)
		return errs.ErrInternal
	case holder == HolderCancel:
		// Our own earlier intent, whose CDR row has not been projected back to us yet. Idempotent.
		return nil
	case holder != HolderNone:
		// We did not win the token. HolderDispatched means the connector got there first and the
		// message is on the wire; any OTHER value means a holder this build cannot name — including
		// the plain "1" the previous build wrote into this very key, which a rolling deploy can still
		// surface. Both are "not ours", so both refuse and write NOTHING. Only an explicitly free
		// token may proceed: when in doubt, refuse (step-209, DN5).
		return errs.ErrCancelFailed
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
