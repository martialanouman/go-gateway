// Package humaerr renders the gateway's flat error model through huma.
//
// Huma emits RFC 9457 problem+json by default. The gateway's contract (engineering guide §11.1) is
// a flat {code, message, errors[]} object served as application/json, with the HTTP status on the
// status line and never duplicated in the body. Install replaces huma's error constructor with one
// that produces this model, and the generated OpenAPI then reflects it automatically.
//
// This is a sub-package of internal/platform/errors on purpose: the error catalogue stays a leaf
// with no dependency on huma or chi, so the SMPP and pipeline binaries that import it do not drag
// an HTTP framework — and its CVEs — into their build.
package humaerr

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/danielgtaylor/huma/v2"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// Model is the gateway's wire error. It implements huma.StatusError (so huma serves it with the
// right status) and huma.ContentTypeFilter (so it is application/json, not application/problem+json).
type Model struct {
	Code    string       `json:"code" doc:"Stable machine-readable error code (engineering guide §11)."`
	Message string       `json:"message" doc:"Human-readable explanation."`
	Errors  []FieldError `json:"errors,omitempty" doc:"Per-field detail on a validation failure."`

	// status is the HTTP status. It is not serialized: the status line carries it.
	status int
}

// FieldError is one per-field validation detail, matching the contract's errors[] item.
type FieldError struct {
	Field   string `json:"field" doc:"The offending field, e.g. \"name\" or \"messages.0.text\"."`
	Message string `json:"message" doc:"What is wrong with the field."`
}

// Error implements error.
func (m *Model) Error() string { return m.Message }

// GetStatus implements huma.StatusError: huma reads it to set the response status.
func (m *Model) GetStatus() int { return m.status }

// ContentType implements huma.ContentTypeFilter, forcing application/json in place of huma's
// application/problem+json default.
func (m *Model) ContentType(string) string { return "application/json" }

var installOnce sync.Once

// Install replaces huma.NewError with the gateway's flat model. huma.NewError is package-global
// mutable state, so this is guarded by sync.Once and must be called before any API is created (it
// is, from adminapi.New). Calling it more than once is safe and does nothing after the first.
func Install() {
	installOnce.Do(func() {
		huma.NewError = newError
	})
}

// newError is huma's error constructor. Huma hands it a status and a message; the challenge is that
// status→code is not injective (403 maps from forbidden_scope, account_suspended, … ), so the code
// is resolved in priority order:
//
//  1. If any wrapped error carries an errs.Code, that code wins and its own HTTP status overrides
//     huma's — this is the path every handler-raised error takes, and it is exact.
//  2. Otherwise the status is mapped through statusToCode, for the errors huma itself raises.
//  3. A 400 (huma's malformed-body status) is rewritten to 422/validation_error: the M1 contract
//     declares no 400 on any operation, so an unrewritten 400 would be a response the contract does
//     not describe.
//  4. Anything left unmapped is 500/internal_error.
func newError(status int, message string, errList ...error) huma.StatusError {
	code, fields := classify(errList)

	if code != "" {
		if httpStatus, ok := errs.HTTPStatus(code); ok {
			status = httpStatus
		}
	} else {
		if status == http.StatusBadRequest {
			status = http.StatusUnprocessableEntity
		}
		code = statusToCode(status)
	}

	return &Model{Code: string(code), Message: message, Errors: fields, status: status}
}

// classify walks the errors huma passes. It returns the first errs.Code found (if any) and the
// per-field details converted from huma's own validation errors.
func classify(errList []error) (errs.Code, []FieldError) {
	var code errs.Code
	var fields []FieldError

	for _, e := range errList {
		if e == nil {
			continue
		}
		if code == "" {
			if c, ok := errs.CodeOf(e); ok {
				code = c
			}
		}
		// Only huma's own validation failures — which implement ErrorDetailer — become per-field
		// entries: Location -> field, Message -> message, straight from the struct tags. A plain
		// wrapped domain error carries a code, not a field, and must not pollute errors[].
		if d, ok := e.(huma.ErrorDetailer); ok {
			detail := d.ErrorDetail()
			fields = append(fields, FieldError{Field: detail.Location, Message: detail.Message})
		}
	}
	return code, fields
}

// statusToCode maps the statuses huma raises on its own to a catalogue code. It is intentionally
// small: handler-raised errors carry their code directly and never reach here.
func statusToCode(status int) errs.Code {
	switch status {
	case http.StatusUnauthorized:
		return errs.ErrUnauthenticated
	case http.StatusForbidden:
		return errs.ErrForbiddenScope
	case http.StatusNotFound:
		return errs.ErrNotFound
	case http.StatusConflict:
		return errs.ErrConflict
	case http.StatusRequestEntityTooLarge:
		return errs.ErrPayloadTooLarge
	case http.StatusUnprocessableEntity:
		return errs.ErrValidation
	case http.StatusTooManyRequests:
		return errs.ErrRateLimited
	case http.StatusServiceUnavailable:
		return errs.ErrServiceUnavailable
	default:
		return errs.ErrInternal
	}
}

// Fail builds an error carrying code, for a handler to return directly. The status comes from the
// catalogue, so the code and the status can never disagree.
func Fail(code errs.Code, format string, args ...any) huma.StatusError {
	status, ok := errs.HTTPStatus(code)
	if !ok {
		status = http.StatusInternalServerError
	}
	return &Model{Code: string(code), Message: fmt.Sprintf(format, args...), status: status}
}

// FailValidation builds a 422 validation_error with per-field detail.
func FailValidation(message string, fields ...FieldError) huma.StatusError {
	status, _ := errs.HTTPStatus(errs.ErrValidation)
	return &Model{
		Code:    string(errs.ErrValidation),
		Message: message,
		Errors:  fields,
		status:  status,
	}
}

// FromError maps a domain error to the wire model.
//
// A server-side fault (any 5xx code, or an error carrying no code at all) is rendered with a STATIC
// message and never the wrapped error string: that string can carry infrastructure detail — SQL
// constraint and table names, a database host/user, SQLSTATE text — which must not reach a client.
// The underlying error is logged instead, so operators keep the diagnosis. A client 4xx keeps its
// wrapped message (only a benign operation label plus the code).
func FromError(err error) huma.StatusError {
	code, ok := errs.CodeOf(err)
	if !ok {
		logInternal(err)
		return Fail(errs.ErrInternal, "internal error")
	}
	if status, mapped := errs.HTTPStatus(code); !mapped || status >= 500 {
		logInternal(err)
		return Fail(code, "%s", staticMessage(code))
	}
	return Fail(code, "%s", messageFor(code, err))
}

// logInternal records a server-side fault with its full detail on the configured logger, so the
// information withheld from the client is still available to operators.
func logInternal(err error) {
	if err == nil {
		return
	}
	// No request context is available at this mapping boundary; the plain (non-context) logger is
	// used deliberately, so trace correlation happens upstream where the context lives.
	slog.Default().Error("admin api internal error", "err", err.Error())
}

// staticMessage is the safe, detail-free client message for a server-side code.
func staticMessage(code errs.Code) string {
	switch code {
	case errs.ErrServiceUnavailable:
		return "service unavailable"
	default:
		return "internal error"
	}
}

// messageFor picks the human message for a coded 4xx error. For a plain sentinel (whose Error() is
// just the code) it returns a stable phrase; for a wrapped error it keeps the operator-facing
// context, which for a 4xx is only the operation label — never infrastructure detail.
func messageFor(code errs.Code, err error) string {
	if msg := err.Error(); msg != string(code) {
		return msg
	}
	return string(code)
}
