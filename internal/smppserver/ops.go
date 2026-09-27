package smppserver

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/observability"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// onQuery returns the session's query_sm hook, bound to this connection's account toggles. When
// query_sm is disabled on the account the operation is answered ESME_RINVCMDID, as if unsupported
// (§6.22). When enabled it is rate-limited on a DEDICATED per-account query_sm bucket (step-087) —
// isolated from the submit_sm budget, so an intensive querier cannot abuse the SMSC nor eat the send
// allowance — then resolved against the CDR, scoped to the bind's account; the limit runs first so
// polling cannot push load onto ClickHouse. Over the limit it is refused ESME_RTHROTTLED. A lookup that
// cannot resolve is ESME_RQUERYFAIL, never ESME_ROK with an unknown state. The message body is never
// involved, so nothing can leak (invariant a).
func (l *Listener) onQuery(_ context.Context, st *connState) session.QueryHandler {
	return func(ctx context.Context, req session.QueryRequest) (res session.QueryResult) {
		if !st.querySMEnabled {
			return session.QueryResult{Status: errs.StatusInvalidCmdID}
		}
		if l.opts.QueryLimiter != nil && !l.opts.QueryLimiter.Allow(ctx, st.accountID) {
			if l.opts.QueryThrottled != nil {
				l.opts.QueryThrottled.Inc()
			}
			// Debug, not Info: this is the hot path a query_sm flood hammers, and smpp_query_throttled_total
			// already carries the signal — an Info per refusal would amplify a flood into a log flood.
			l.logger.DebugContext(ctx, "smpp query_sm throttled", "account_id", st.accountID)
			return session.QueryResult{Status: errs.StatusThrottled}
		}
		if l.opts.MessageReader == nil {
			return session.QueryResult{Status: errs.StatusQueryFail}
		}

		ctx, span := l.opts.Tracer.Start(ctx, "smpp.query")
		defer span.End()
		defer func() {
			switch res.Status {
			case smpp.StatusOK:
			case errs.StatusInvalidMsgID:
				observability.RecordSpanError(span, errs.ErrMessageNotFound)
			default:
				observability.RecordSpanError(span, errs.ErrInternal)
			}
		}()

		id, err := uuid.Parse(req.MessageID)
		if err != nil {
			return session.QueryResult{Status: errs.StatusInvalidMsgID}
		}
		ctx, cancel := context.WithTimeout(ctx, cdrLookupTimeout)
		defer cancel()
		row, found, err := l.opts.MessageReader.Current(ctx, st.customerID, st.accountID, id)
		if err != nil {
			l.logger.WarnContext(ctx, "smpp query_sm: read cdr", "message_id", id, "account_id", st.accountID, "err", err)
			return session.QueryResult{Status: errs.StatusQueryFail}
		}
		if !found {
			return session.QueryResult{Status: errs.StatusInvalidMsgID}
		}
		state, mapped := messageState(row.Status)
		if !mapped {
			l.logger.ErrorContext(ctx, "smpp query_sm: unmapped cdr status", "message_id", id, "status", row.Status)
			return session.QueryResult{Status: errs.StatusQueryFail}
		}
		res = session.QueryResult{Status: smpp.StatusOK, MessageID: req.MessageID, MessageState: state}
		if row.Status == clickhouse.StatusDelivered && row.DeliveredAt != nil {
			res.FinalDate = smppAbsoluteTime(*row.DeliveredAt)
		}
		return res
	}
}

// messageState maps a CDR lifecycle status to its SMPP v3.4 message_state (§5.2.28). ACCEPTED is never
// answered: in 3.4 it means "read on the subscriber's behalf by customer service", not "accepted by the
// gateway", so a message still held before dispatch is ENROUTE.
func messageState(status clickhouse.Status) (uint8, bool) {
	switch status {
	case clickhouse.StatusDelivered:
		return smpp.MessageStateDelivered, true
	case clickhouse.StatusExpired:
		return smpp.MessageStateExpired, true
	case clickhouse.StatusCancelled:
		return smpp.MessageStateDeleted, true
	case clickhouse.StatusFailed:
		return smpp.MessageStateUndeliverable, true
	case clickhouse.StatusRejected:
		return smpp.MessageStateRejected, true
	case clickhouse.StatusAccepted, clickhouse.StatusEnroute, clickhouse.StatusRerouted:
		return smpp.MessageStateEnroute, true
	default:
		return 0, false
	}
}

// smppAbsoluteTime formats t as an SMPP absolute time (§7.1.1, YYMMDDhhmmsstnnp) in UTC.
func smppAbsoluteTime(t time.Time) string {
	t = t.UTC()
	return fmt.Sprintf("%s%d00+", t.Format("060102150405"), t.Nanosecond()/100_000_000)
}

// onCancel returns the session's cancel_sm hook, bound to this connection's bind identity. When
// cancel_sm is disabled on the account the operation is answered ESME_RINVCMDID, as if unsupported
// (§6.22). When enabled it cancels a not-yet-dispatched message through the shared Canceller, scoped
// to the bind's account (invariant: a bind cannot cancel another account's message): an unknown
// message is ESME_RINVMSGID, an already-dispatched one is ESME_RCANCELFAIL, and a still-queued (or
// already-cancelled) one is ESME_ROK. A nil Canceller rejects with ESME_RCANCELFAIL. The message body
// is never involved, so nothing can leak (invariant a).
func (l *Listener) onCancel(_ context.Context, st *connState) session.CancelHandler {
	return func(sctx context.Context, req session.CancelRequest) (res session.CancelResult) {
		if !st.cancelSMEnabled {
			return session.CancelResult{Status: errs.StatusInvalidCmdID}
		}
		if l.opts.Canceller == nil {
			return session.CancelResult{Status: errs.StatusCancelFail}
		}

		sctx, span := l.opts.Tracer.Start(sctx, "smpp.cancel")
		defer span.End()
		defer func() {
			if res.Status != 0 {
				observability.RecordSpanError(span, errs.CodeFromSMPPStatus(res.Status))
			}
		}()

		id, err := uuid.Parse(req.MessageID)
		if err != nil {
			// A malformed or empty message_id names no message: treat it as unknown (ESME_RINVMSGID).
			status := errs.SMPPStatusForError(errs.ErrMessageNotFound)
			l.logger.InfoContext(sctx, "smpp cancel: invalid message_id",
				"account_id", st.accountID, "command_status", status)
			return session.CancelResult{Status: status}
		}

		sctx, cancel := context.WithTimeout(sctx, cdrLookupTimeout)
		defer cancel()
		if err := l.opts.Canceller.Cancel(sctx, st.customerID, st.accountID, id); err != nil {
			status := errs.SMPPStatusForError(err)
			l.logger.InfoContext(sctx, "smpp cancel: rejected",
				"message_id", id, "account_id", st.accountID, "command_status", status)
			return session.CancelResult{Status: status}
		}
		return session.CancelResult{Status: smpp.StatusOK}
	}
}
