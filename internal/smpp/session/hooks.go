package session

import (
	"context"

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
