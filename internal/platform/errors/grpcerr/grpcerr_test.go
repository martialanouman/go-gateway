package grpcerr_test

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/grpcerr"
)

// TestStatusMapsTheSharedVocabulary pins the table now that TWO services (billing-svc and content-key-svc)
// answer with it: a drift here would silently change what "not found" or "conflict" means to a client.
func TestStatusMapsTheSharedVocabulary(t *testing.T) {
	tests := []struct {
		err      error
		wantCode codes.Code
		wantMsg  errs.Code
	}{
		{errs.ErrValidation, codes.InvalidArgument, errs.ErrValidation},
		{errs.ErrConflict, codes.Aborted, errs.ErrConflict},
		{errs.ErrNotFound, codes.NotFound, errs.ErrNotFound},
		{errs.ErrInsufficientCredit, codes.FailedPrecondition, errs.ErrInsufficientCredit},
		{errs.ErrExternalBillingUnavailable, codes.Unavailable, errs.ErrExternalBillingUnavailable},
		{errs.ErrInternal, codes.Internal, errs.ErrInternal},
		// A wrapped sentinel still maps: callers wrap with context on the way out.
		{fmt.Errorf("rotate content key: %w", errs.ErrNotFound), codes.NotFound, errs.ErrNotFound},
	}
	for _, tc := range tests {
		t.Run(string(tc.wantMsg), func(t *testing.T) {
			st := status.Convert(grpcerr.Status(tc.err))
			if st.Code() != tc.wantCode {
				t.Errorf("code = %v, want %v", st.Code(), tc.wantCode)
			}
			// The MESSAGE is the flat wire code — the contract clients branch on (§11.3).
			if st.Message() != string(tc.wantMsg) {
				t.Errorf("message = %q, want the wire code %q", st.Message(), tc.wantMsg)
			}
		})
	}
}

// TestStatusHidesUnknownErrors: an error carrying no platform code becomes a bare Internal — its text must
// never cross a service boundary, where it could leak internals.
func TestStatusHidesUnknownErrors(t *testing.T) {
	st := status.Convert(grpcerr.Status(errors.New("connection refused to secret-host:5432")))
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
	if st.Message() != string(errs.ErrInternal) {
		t.Errorf("message = %q, want the generic %q (no leaked text)", st.Message(), errs.ErrInternal)
	}
}

// TestStatusPassesNilThrough: no error, no status.
func TestStatusPassesNilThrough(t *testing.T) {
	if err := grpcerr.Status(nil); err != nil {
		t.Errorf("Status(nil) = %v, want nil", err)
	}
}
