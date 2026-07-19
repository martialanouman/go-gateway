package session

import (
	"context"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// BindHandler decides the outcome of a bind. It runs on the session's read goroutine, so it must
// not block indefinitely. Returning a Status other than smpp.StatusOK rejects the bind (the
// session stays open); the auth and max_sessions logic that populate it are wired in step-024.
type BindHandler func(ctx context.Context, req BindRequest) BindResult

// SubmitHandler decides the outcome of a submit_sm. It runs on the session's read goroutine. The
// pipeline that routes the message is wired in step-025; here the handler only maps a request to a
// response.
type SubmitHandler func(ctx context.Context, req SubmitRequest) SubmitResult

// QueryHandler decides the outcome of a query_sm. It runs on the session's read goroutine. step-025
// wires the account's query_sm_enabled toggle here; the real message-state lookup is later work.
type QueryHandler func(ctx context.Context, req QueryRequest) QueryResult

// CancelHandler decides the outcome of a cancel_sm. It runs on the session's read goroutine. step-025
// wires the cancel_sm_enabled toggle here; the real cancellation semantics arrive at step-030.
type CancelHandler func(ctx context.Context, req CancelRequest) CancelResult

// UnbindHandler is notified when the ESME unbinds, before the session sends unbind_resp and closes.
// It is advisory: the session unbinds regardless.
type UnbindHandler func(ctx context.Context)

// BindRequest is the bind_* content handed to a BindHandler. Password is carried so the handler can
// authenticate; the session never logs it.
type BindRequest struct {
	Mode             BindMode
	SystemID         string
	Password         string
	SystemType       string
	InterfaceVersion uint8
	AddrTON          uint8
	AddrNPI          uint8
	AddressRange     string
}

// BindResult is a BindHandler's decision. Status is an SMPP command_status: smpp.StatusOK accepts,
// any other value (e.g. errs.StatusInvalidPasswd, errs.StatusBindFail) rejects. An empty SystemID
// falls back to Config.SystemID in the response.
type BindResult struct {
	Status   uint32
	SystemID string
}

// SubmitRequest is the submit_sm content handed to a SubmitHandler. Body is the message content
// wrapped in msg.Body so it can never leak into a log or span (invariant a): the plaintext is
// reachable only through Body.Reveal, which step-025 calls as an audited egress. A payload larger
// than 254 octets travels in the message_payload TLV, so TLVs may carry content too and must never
// be logged.
type SubmitRequest struct {
	Source             string
	Destination        string
	ServiceType        string
	ESMClass           uint8
	DataCoding         uint8
	RegisteredDelivery uint8
	Body               msg.Body
	TLVs               smpp.TLVList
}

// SubmitResult is a SubmitHandler's decision. Status is an SMPP command_status: smpp.StatusOK
// accepts, and MessageID (the assigned message id) is echoed in submit_sm_resp only on success.
type SubmitResult struct {
	Status    uint32
	MessageID string
}

// QueryRequest is the query_sm content handed to a QueryHandler: the message_id to look up, scoped to
// its original source address.
type QueryRequest struct {
	MessageID     string
	SourceAddrTON uint8
	SourceAddrNPI uint8
	SourceAddr    string
}

// QueryResult is a QueryHandler's decision. Status is an SMPP command_status: smpp.StatusOK answers a
// query_sm_resp carrying MessageID/FinalDate/MessageState/ErrorCode; any other value rejects with that
// status (e.g. errs.StatusInvalidCmdID when the operation is disabled).
type QueryResult struct {
	Status       uint32
	MessageID    string
	FinalDate    string
	MessageState uint8
	ErrorCode    uint8
}

// CancelRequest is the cancel_sm content handed to a CancelHandler: a single message_id, or a batch
// keyed by source/destination when message_id is empty.
type CancelRequest struct {
	ServiceType     string
	MessageID       string
	SourceAddrTON   uint8
	SourceAddrNPI   uint8
	SourceAddr      string
	DestAddrTON     uint8
	DestAddrNPI     uint8
	DestinationAddr string
}

// CancelResult is a CancelHandler's decision. Status is an SMPP command_status: smpp.StatusOK answers
// an empty cancel_sm_resp; any other value rejects with that status.
type CancelResult struct {
	Status uint32
}

// callOnBind runs the OnBind hook under panic recovery. The hooks are caller-supplied extension
// points (step-024 wires auth and max_sessions there); a panic in one must not tear down the read
// goroutine — and, through it, the process that multiplexes every other session. A panic rejects
// the bind with ESME_RSYSERR and keeps the session open. A nil hook accepts with Config defaults.
func (s *Session) callOnBind(ctx context.Context, req BindRequest) (res BindResult) {
	if s.cfg.OnBind == nil {
		return BindResult{Status: smpp.StatusOK, SystemID: s.cfg.SystemID}
	}
	defer func() {
		if r := recover(); r != nil {
			// The panic value is logged for diagnosis; it never carries the message body (invariant
			// a) — a hook that panicked with the body would itself be violating the invariant.
			s.logger.Error("session: OnBind panicked", "panic", r)
			res = BindResult{Status: errs.StatusSysErr}
		}
	}()
	return s.cfg.OnBind(ctx, req)
}

// callOnSubmit runs the OnSubmit hook under panic recovery. A panic rejects the submit_sm with
// ESME_RSYSERR and keeps the session alive. A nil hook accepts with an empty message id.
func (s *Session) callOnSubmit(ctx context.Context, req SubmitRequest) (res SubmitResult) {
	if s.cfg.OnSubmit == nil {
		return SubmitResult{Status: smpp.StatusOK}
	}
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("session: OnSubmit panicked", "panic", r)
			res = SubmitResult{Status: errs.StatusSysErr}
		}
	}()
	return s.cfg.OnSubmit(ctx, req)
}

// callOnQuery runs the OnQuery hook under panic recovery. A panic rejects the query_sm with
// ESME_RSYSERR and keeps the session alive. A nil hook accepts with a zero-value state (skeleton).
func (s *Session) callOnQuery(ctx context.Context, req QueryRequest) (res QueryResult) {
	if s.cfg.OnQuery == nil {
		return QueryResult{Status: smpp.StatusOK, MessageID: req.MessageID}
	}
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("session: OnQuery panicked", "panic", r)
			res = QueryResult{Status: errs.StatusSysErr}
		}
	}()
	return s.cfg.OnQuery(ctx, req)
}

// callOnCancel runs the OnCancel hook under panic recovery. A panic rejects the cancel_sm with
// ESME_RSYSERR and keeps the session alive. A nil hook accepts.
func (s *Session) callOnCancel(ctx context.Context, req CancelRequest) (res CancelResult) {
	if s.cfg.OnCancel == nil {
		return CancelResult{Status: smpp.StatusOK}
	}
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("session: OnCancel panicked", "panic", r)
			res = CancelResult{Status: errs.StatusSysErr}
		}
	}()
	return s.cfg.OnCancel(ctx, req)
}

// callOnUnbind runs the OnUnbind hook under panic recovery. The hook is advisory, so a panic is
// logged and swallowed: the session unbinds and closes regardless. A nil hook is a no-op.
func (s *Session) callOnUnbind(ctx context.Context) {
	if s.cfg.OnUnbind == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("session: OnUnbind panicked", "panic", r)
		}
	}()
	s.cfg.OnUnbind(ctx)
}
