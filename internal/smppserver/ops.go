package smppserver

import (
	"context"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// onQuery returns the session's query_sm hook, bound to this connection's account toggles. When
// query_sm is disabled on the account the operation is answered ESME_RINVCMDID, as if unsupported
// (§6.22). When enabled it is a skeleton: it echoes the message_id with an unknown message state —
// the real message-state lookup and its dedicated rate limit are M6 (out of scope for step-025). The
// message body is never involved, so nothing can leak (invariant a).
func (l *Listener) onQuery(_ context.Context, st *connState) session.QueryHandler {
	return func(_ context.Context, req session.QueryRequest) session.QueryResult {
		if !st.querySMEnabled {
			return session.QueryResult{Status: errs.StatusInvalidCmdID}
		}
		// Skeleton: acknowledge with the queried id and a valid UNKNOWN state — the real message-state
		// lookup (and its dedicated rate limit) is M6.
		return session.QueryResult{
			Status:       smpp.StatusOK,
			MessageID:    req.MessageID,
			MessageState: smpp.MessageStateUnknown,
		}
	}
}

// onCancel returns the session's cancel_sm hook, bound to this connection's bind identity. When
// cancel_sm is disabled on the account the operation is answered ESME_RINVCMDID, as if unsupported
// (§6.22). When enabled it cancels a not-yet-dispatched message through the shared Canceller, scoped
// to the bind's account (invariant: a bind cannot cancel another account's message): an unknown
// message is ESME_RINVMSGID, an already-dispatched one is ESME_RCANCELFAIL, and a still-queued (or
// already-cancelled) one is ESME_ROK. A nil Canceller rejects with ESME_RCANCELFAIL. The message body
// is never involved, so nothing can leak (invariant a).
func (l *Listener) onCancel(_ context.Context, st *connState) session.CancelHandler {
	return func(sctx context.Context, req session.CancelRequest) session.CancelResult {
		if !st.cancelSMEnabled {
			return session.CancelResult{Status: errs.StatusInvalidCmdID}
		}
		if l.opts.Canceller == nil {
			return session.CancelResult{Status: errs.StatusCancelFail}
		}

		sctx, span := l.opts.Tracer.Start(sctx, "smpp.cancel")
		defer span.End()

		id, err := uuid.Parse(req.MessageID)
		if err != nil {
			// A malformed or empty message_id names no message: treat it as unknown (ESME_RINVMSGID).
			status := errs.SMPPStatusForError(errs.ErrMessageNotFound)
			l.logger.InfoContext(sctx, "smpp cancel: invalid message_id",
				"account_id", st.accountID, "command_status", status)
			return session.CancelResult{Status: status}
		}

		if err := l.opts.Canceller.Cancel(sctx, st.customerID, st.accountID, id); err != nil {
			status := errs.SMPPStatusForError(err)
			l.logger.InfoContext(sctx, "smpp cancel: rejected",
				"message_id", id, "account_id", st.accountID, "command_status", status)
			return session.CancelResult{Status: status}
		}
		return session.CancelResult{Status: smpp.StatusOK}
	}
}
