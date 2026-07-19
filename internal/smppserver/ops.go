package smppserver

import (
	"context"

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

// onCancel returns the session's cancel_sm hook, bound to this connection's account toggles. When
// cancel_sm is disabled the operation is answered ESME_RINVCMDID, as if unsupported (§6.22). When
// enabled it is a skeleton returning an OK cancel_sm_resp — the real cancellation semantics arrive at
// step-030. The message body is never involved (invariant a).
func (l *Listener) onCancel(_ context.Context, st *connState) session.CancelHandler {
	return func(_ context.Context, _ session.CancelRequest) session.CancelResult {
		if !st.cancelSMEnabled {
			return session.CancelResult{Status: errs.StatusInvalidCmdID}
		}
		return session.CancelResult{Status: smpp.StatusOK}
	}
}
