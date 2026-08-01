// Package grpcerr maps the platform's flat error vocabulary onto gRPC statuses. It is the gRPC sibling of
// humaerr (which does the same for HTTP): one place decides how a domain error code crosses a wire, so two
// services cannot drift on what "not found" or "conflict" means to a caller.
package grpcerr

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// Status maps a domain error onto a gRPC status whose MESSAGE is the flat wire code (§11.3) — the shared
// contract clients branch on. An unrecognised error becomes Internal carrying the generic code, so an
// unexpected fault never leaks its text across a service boundary.
func Status(err error) error {
	if err == nil {
		return nil
	}
	code, ok := errs.CodeOf(err)
	if !ok {
		return status.Error(codes.Internal, string(errs.ErrInternal))
	}
	return status.Error(CodeFor(code), string(code))
}

// CodeFor is the platform-code → gRPC-code table.
func CodeFor(c errs.Code) codes.Code {
	switch c {
	case errs.ErrValidation:
		return codes.InvalidArgument
	case errs.ErrConflict:
		return codes.Aborted
	case errs.ErrNotFound:
		return codes.NotFound
	case errs.ErrInsufficientCredit:
		return codes.FailedPrecondition
	case errs.ErrExternalBillingUnavailable:
		// A provider outage under fail_closed is transient: gRPC Unavailable tells the caller to retry
		// (the router treats it as a retryable fault), so a billed message is held, never sent unconfirmed
		// nor permanently rejected.
		return codes.Unavailable
	default:
		return codes.Internal
	}
}
