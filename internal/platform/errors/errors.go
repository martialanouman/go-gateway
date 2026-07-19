// Package errors defines the gateway's stable error codes and their single mapping to the
// REST and SMPP surfaces.
//
// A Code is the contract (guide d'ingénierie §11.2): it is identical whatever protocol the
// client speaks, it is published in both OpenAPI specs, and it is immutable once released —
// codes are added, never renamed or recycled, because clients branch on them. The same Code
// travels through all three surfaces of a rejection — the client response, the CDR
// (error_code) and the failing span — which is what makes an error traceable end to end from
// its value alone (§11.5).
//
// Domain code raises a Code (or wraps one); translation to an HTTP status or an SMPP
// command_status happens ONCE, at the boundary, through this table and nowhere else.
package errors

import (
	"errors"
	"sort"
)

// Code is a stable, machine-readable error key in snake_case. It implements error, so the
// catalogue constants below are usable directly as sentinels:
//
//	if errors.Is(err, errs.ErrRecipientOptedOut) { ... }
//
// and wrapped to add context without losing the code:
//
//	fmt.Errorf("reserve credit for %s: %w", id, errs.ErrInsufficientCredit)
type Code string

// Error makes a Code usable as a sentinel error. The message is the code itself: human-facing
// text belongs in the `message` field of the response, built at the boundary.
func (c Code) Error() string { return string(c) }

// String returns the wire form of the code.
func (c Code) String() string { return string(c) }

// The error catalogue (guide d'ingénierie §11.3). These constants are both the wire codes and
// the sentinels. Adding one means adding it here, in the `code` enum of both OpenAPI specs,
// and in §11.3 — the three move together.
const (
	ErrUnauthenticated       Code = "unauthenticated"
	ErrAccountSuspended      Code = "account_suspended"
	ErrChannelDisabled       Code = "channel_disabled"
	ErrMaxSessionsExceeded   Code = "max_sessions_exceeded"
	ErrInvalidDestination    Code = "invalid_destination"
	ErrInvalidSource         Code = "invalid_source"
	ErrSenderIDNotAuthorized Code = "sender_id_not_authorized"
	ErrRecipientOptedOut     Code = "recipient_opted_out"
	ErrContentBlocked        Code = "content_blocked"
	ErrNoRoute               Code = "no_route"
	ErrPayloadTooLarge       Code = "payload_too_large"
	ErrRateLimited           Code = "rate_limited"
	ErrQueueFull             Code = "queue_full"
	ErrInsufficientCredit    Code = "insufficient_credit"
	ErrMessageNotFound       Code = "message_not_found"
	ErrNotFound              Code = "not_found"
	ErrCancelFailed          Code = "cancel_failed"
	ErrOperationNotSupported Code = "operation_not_supported"
	ErrValidation            Code = "validation_error"
	ErrIdempotencyConflict   Code = "idempotency_conflict"
	ErrForbiddenScope        Code = "forbidden_scope"
	ErrConflict              Code = "conflict"
	ErrInternal              Code = "internal_error"
	ErrServiceUnavailable    Code = "service_unavailable"
	// ErrSubmitFailed is the outbound outcome recorded in cdr.error_code when the SMSC rejects a
	// submit_sm with ESME_RSUBMITFAIL. It is an outcome code, not a REST request error, so it has no
	// HTTP surface.
	ErrSubmitFailed Code = "submit_failed"
)

// SMPP v3.4 command_status values used by the mapping. Errors with no standard SMPP code use
// the reserved vendor range 0x00000400+.
const (
	StatusInvalidMsgLen uint32 = 0x00000001 // ESME_RINVMSGLEN
	StatusInvalidCmdID  uint32 = 0x00000003 // ESME_RINVCMDID
	// StatusInvalidBindStatus rejects a PDU sent in the wrong session state (submit_sm before
	// bind, submit_sm on a receiver bind). It is a protocol-state status, not a business Code,
	// so it lives here as a raw constant and has no catalogue entry.
	StatusInvalidBindStatus uint32 = 0x00000004 // ESME_RINVBNDSTS
	// StatusAlreadyBound rejects a second bind on a session that is already bound.
	StatusAlreadyBound   uint32 = 0x00000005 // ESME_RALYBND
	StatusSysErr         uint32 = 0x00000008 // ESME_RSYSERR
	StatusInvalidSrcAddr uint32 = 0x0000000A // ESME_RINVSRCADR
	StatusInvalidDstAddr uint32 = 0x0000000B // ESME_RINVDSTADR
	StatusInvalidMsgID   uint32 = 0x0000000C // ESME_RINVMSGID
	StatusBindFail       uint32 = 0x0000000D // ESME_RBINDFAIL
	StatusInvalidPasswd  uint32 = 0x0000000E // ESME_RINVPASWD
	StatusInvalidSysID   uint32 = 0x0000000F // ESME_RINVSYSID
	StatusCancelFail     uint32 = 0x00000011 // ESME_RCANCELFAIL
	StatusMsgQueueFull   uint32 = 0x00000014 // ESME_RMSGQFUL
	StatusSubmitFail     uint32 = 0x00000045 // ESME_RSUBMITFAIL
	StatusThrottled      uint32 = 0x00000058 // ESME_RTHROTTLED

	// StatusInsufficientCredit is a vendor-specific status: billing has no standard SMPP code.
	StatusInsufficientCredit uint32 = 0x00000400
)

// Mapping is the boundary translation of a Code: at most one HTTP status and at most one SMPP
// command_status. Some codes reach only one surface — max_sessions_exceeded happens at bind
// time and has no REST equivalent; the Admin-API codes have no SMPP equivalent.
type Mapping struct {
	// HTTPStatus is the REST status, or 0 when the code has no REST surface.
	HTTPStatus int
	// SMPPStatus is the SMPP command_status, or 0 when the code has no SMPP surface. 0 is
	// unambiguous here: ESME_ROK is success and never maps from an error code.
	SMPPStatus uint32
	// Retryable reports whether a client SHOULD replay the request as-is (§11.4). Business 4xx
	// are not: the input must change first. rate_limited and queue_full are, with backoff and
	// honouring Retry-After. 5xx are, because submission is idempotent by Idempotency-Key and
	// billing is idempotent by message_id (§6.9).
	Retryable bool
}

// catalogue is the single source of truth for the boundary mapping (§11.3). It is unexported:
// callers read it through Map/HTTPStatus/SMPPStatus so no caller can mutate the contract.
var catalogue = map[Code]Mapping{
	// §11.3 gives unauthenticated two SMPP statuses: ESME_RINVPASWD for a bad password and
	// ESME_RINVSYSID for an unknown system_id. The catalogue carries the former, and the bind
	// path should keep it in both cases unless an operator decides otherwise: answering
	// ESME_RINVSYSID tells an attacker which system_ids exist. The client-facing code is the
	// same either way, which is the point of the contract.
	ErrUnauthenticated:       {HTTPStatus: 401, SMPPStatus: StatusInvalidPasswd},
	ErrAccountSuspended:      {HTTPStatus: 403, SMPPStatus: StatusBindFail},
	ErrChannelDisabled:       {HTTPStatus: 403, SMPPStatus: StatusBindFail},
	ErrMaxSessionsExceeded:   {SMPPStatus: StatusBindFail, Retryable: true},
	ErrInvalidDestination:    {HTTPStatus: 422, SMPPStatus: StatusInvalidDstAddr},
	ErrInvalidSource:         {HTTPStatus: 422, SMPPStatus: StatusInvalidSrcAddr},
	ErrSenderIDNotAuthorized: {HTTPStatus: 403, SMPPStatus: StatusInvalidSrcAddr},
	ErrRecipientOptedOut:     {HTTPStatus: 403, SMPPStatus: StatusSubmitFail},
	ErrContentBlocked:        {HTTPStatus: 403, SMPPStatus: StatusSubmitFail},
	ErrNoRoute:               {HTTPStatus: 422, SMPPStatus: StatusInvalidDstAddr},
	ErrPayloadTooLarge:       {HTTPStatus: 413, SMPPStatus: StatusInvalidMsgLen},
	ErrRateLimited:           {HTTPStatus: 429, SMPPStatus: StatusThrottled, Retryable: true},
	ErrQueueFull:             {HTTPStatus: 503, SMPPStatus: StatusMsgQueueFull, Retryable: true},
	ErrInsufficientCredit:    {HTTPStatus: 402, SMPPStatus: StatusInsufficientCredit},
	ErrMessageNotFound:       {HTTPStatus: 404, SMPPStatus: StatusInvalidMsgID},
	// not_found is the generic Admin-API 404: an addressed control-plane resource (customer,
	// account, connector, route, sender ID, credential) does not exist. It is distinct from
	// message_not_found, which is a query_sm/GET-message concern and carries ESME_RINVMSGID; a
	// missing customer has no SMPP surface, so this code has none.
	ErrNotFound:              {HTTPStatus: 404},
	ErrCancelFailed:          {HTTPStatus: 409, SMPPStatus: StatusCancelFail},
	ErrOperationNotSupported: {HTTPStatus: 405, SMPPStatus: StatusInvalidCmdID},
	ErrValidation:            {HTTPStatus: 422, SMPPStatus: StatusInvalidMsgLen},
	ErrIdempotencyConflict:   {HTTPStatus: 409},
	ErrForbiddenScope:        {HTTPStatus: 403},
	ErrConflict:              {HTTPStatus: 409},
	ErrInternal:              {HTTPStatus: 500, SMPPStatus: StatusSysErr, Retryable: true},
	ErrServiceUnavailable:    {HTTPStatus: 503, SMPPStatus: StatusSysErr, Retryable: true},
	// submit_failed is an outbound SMSC outcome (ESME_RSUBMITFAIL) recorded in the CDR, not a REST
	// request error, so it carries the SMPP surface only.
	ErrSubmitFailed: {SMPPStatus: StatusSubmitFail},
}

// Valid reports whether c is a published code. Use it on any code arriving from outside.
func (c Code) Valid() bool {
	_, ok := catalogue[c]
	return ok
}

// Map returns the boundary mapping for c, and whether c is a published code.
func Map(c Code) (Mapping, bool) {
	m, ok := catalogue[c]
	return m, ok
}

// HTTPStatus returns the REST status for c. Unknown codes and codes with no REST surface
// return 0 and false — a caller on the REST boundary should fall back to 500.
func HTTPStatus(c Code) (int, bool) {
	m, ok := catalogue[c]
	if !ok || m.HTTPStatus == 0 {
		return 0, false
	}
	return m.HTTPStatus, true
}

// SMPPStatus returns the SMPP command_status for c. Unknown codes and codes with no SMPP
// surface return 0 and false — a caller on the SMPP boundary should fall back to ESME_RSYSERR.
func SMPPStatus(c Code) (uint32, bool) {
	m, ok := catalogue[c]
	if !ok || m.SMPPStatus == 0 {
		return 0, false
	}
	return m.SMPPStatus, true
}

// Retryable reports whether a client should replay a request rejected with c (§11.4).
func Retryable(c Code) bool {
	return catalogue[c].Retryable
}

// CodeFromSMPPStatus maps an SMSC submit_sm_resp command_status to the platform Code recorded in
// cdr.error_code. It is the outcome-side reverse of the catalogue's SMPP surface: the connector
// records what the SMSC returned, so it needs status->code (the catalogue itself is code->status,
// and StatusSubmitFail is a many-to-one target, so it cannot be inverted mechanically). An unmapped
// status falls back to internal_error rather than leaking a raw hex status into the contract; the
// precise command_status still belongs in a log or span at the call site. ESME_ROK is success and
// must not be passed here.
func CodeFromSMPPStatus(status uint32) Code {
	switch status {
	case StatusThrottled:
		return ErrRateLimited
	case StatusSubmitFail:
		return ErrSubmitFailed
	case StatusInvalidDstAddr:
		return ErrInvalidDestination
	case StatusInvalidSrcAddr:
		return ErrInvalidSource
	case StatusMsgQueueFull:
		return ErrQueueFull
	case StatusInvalidMsgLen:
		return ErrValidation
	case StatusInsufficientCredit:
		return ErrInsufficientCredit
	default:
		return ErrInternal
	}
}

// CodeOf extracts the Code carried by err, unwrapping the chain. It reports false for a nil
// error or one carrying no code — callers at a boundary map that to ErrInternal.
func CodeOf(err error) (Code, bool) {
	if err == nil {
		return "", false
	}
	var c Code
	if errors.As(err, &c) {
		return c, true
	}
	return "", false
}

// Codes returns every published code, sorted. Intended for contract tests and documentation
// generation, not for hot paths.
func Codes() []Code {
	out := make([]Code, 0, len(catalogue))
	for c := range catalogue {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
