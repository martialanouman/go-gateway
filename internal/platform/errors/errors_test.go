package errors_test

import (
	goerrors "errors"
	"fmt"
	"testing"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

func TestSMPPStatusForError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want uint32
	}{
		{"code with smpp surface", errs.ErrInvalidDestination, errs.StatusInvalidDstAddr},
		{"wrapped code keeps its status", fmt.Errorf("produce: %w", errs.ErrServiceUnavailable), errs.StatusSysErr},
		{"code with no smpp surface falls back", errs.ErrForbiddenScope, errs.StatusSysErr},
		{"non-coded error falls back", goerrors.New("plain"), errs.StatusSysErr},
		{"nil falls back", nil, errs.StatusSysErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := errs.SMPPStatusForError(tc.err); got != tc.want {
				t.Errorf("SMPPStatusForError(%v) = %#x, want %#x", tc.err, got, tc.want)
			}
		})
	}
}

// TestCatalogueMatchesSpec transcribes the §11.3 table of the engineering guide independently of
// the implementation. It is the contract test: if the code and the spec disagree, one of them is
// wrong and this fails. Do not "fix" it by copying from errors.go — check the spec.
func TestCatalogueMatchesSpec(t *testing.T) {
	tests := []struct {
		code      errs.Code
		wire      string
		http      int    // 0 = no REST surface
		smpp      uint32 // 0 = no SMPP surface
		retryable bool
	}{
		{errs.ErrUnauthenticated, "unauthenticated", 401, 0x0E, false},
		{errs.ErrAccountSuspended, "account_suspended", 403, 0x0D, false},
		{errs.ErrChannelDisabled, "channel_disabled", 403, 0x0D, false},
		{errs.ErrMaxSessionsExceeded, "max_sessions_exceeded", 0, 0x0D, true},
		{errs.ErrInvalidDestination, "invalid_destination", 422, 0x0B, false},
		{errs.ErrInvalidSource, "invalid_source", 422, 0x0A, false},
		{errs.ErrSenderIDNotAuthorized, "sender_id_not_authorized", 403, 0x0A, false},
		{errs.ErrRecipientOptedOut, "recipient_opted_out", 403, 0x45, false},
		{errs.ErrContentBlocked, "content_blocked", 403, 0x45, false},
		{errs.ErrNoRoute, "no_route", 422, 0x0B, false},
		{errs.ErrPayloadTooLarge, "payload_too_large", 413, 0x01, false},
		{errs.ErrRateLimited, "rate_limited", 429, 0x58, true},
		{errs.ErrQueueFull, "queue_full", 503, 0x14, true},
		{errs.ErrInsufficientCredit, "insufficient_credit", 402, 0x00000400, false},
		{errs.ErrExternalBillingUnavailable, "external_billing_unavailable", 503, 0x08, true},
		{errs.ErrMessageNotFound, "message_not_found", 404, 0x0C, false},
		{errs.ErrNotFound, "not_found", 404, 0, false},
		{errs.ErrCancelFailed, "cancel_failed", 409, 0x11, false},
		{errs.ErrOperationNotSupported, "operation_not_supported", 405, 0x03, false},
		{errs.ErrValidation, "validation_error", 422, 0x01, false},
		{errs.ErrIdempotencyConflict, "idempotency_conflict", 409, 0, false},
		{errs.ErrForbiddenScope, "forbidden_scope", 403, 0, false},
		{errs.ErrConflict, "conflict", 409, 0, false},
		{errs.ErrInternal, "internal_error", 500, 0x08, true},
		{errs.ErrServiceUnavailable, "service_unavailable", 503, 0x08, true},
		{errs.ErrSubmitFailed, "submit_failed", 0, 0x45, false},
	}

	for _, tc := range tests {
		t.Run(tc.wire, func(t *testing.T) {
			if got := tc.code.String(); got != tc.wire {
				t.Errorf("wire form = %q, want %q", got, tc.wire)
			}

			gotHTTP, okHTTP := errs.HTTPStatus(tc.code)
			if tc.http == 0 {
				if okHTTP {
					t.Errorf("HTTPStatus() = %d, want no REST surface", gotHTTP)
				}
			} else if !okHTTP || gotHTTP != tc.http {
				t.Errorf("HTTPStatus() = %d (ok=%v), want %d", gotHTTP, okHTTP, tc.http)
			}

			gotSMPP, okSMPP := errs.SMPPStatus(tc.code)
			if tc.smpp == 0 {
				if okSMPP {
					t.Errorf("SMPPStatus() = %#x, want no SMPP surface", gotSMPP)
				}
			} else if !okSMPP || gotSMPP != tc.smpp {
				t.Errorf("SMPPStatus() = %#x (ok=%v), want %#x", gotSMPP, okSMPP, tc.smpp)
			}

			if got := errs.Retryable(tc.code); got != tc.retryable {
				t.Errorf("Retryable() = %v, want %v", got, tc.retryable)
			}
		})
	}

	if got, want := len(errs.Codes()), len(tests); got != want {
		t.Errorf("catalogue holds %d codes, spec §11.3 lists %d — a code was added without updating "+
			"the spec transcription (or vice versa)", got, want)
	}
}

// TestOutcomeOnlyCodes pins the wire form of the CDR outcome codes that reach neither REST nor SMPP
// (a delivery receipt is not a command response). They are deliberately bare constants, NOT catalogue
// entries — TestEveryCodeReachesASurface would reject a code that maps nowhere — so this guards their
// snake_case string against a typo or a drift from the guide §11.3 rows.
func TestOutcomeOnlyCodes(t *testing.T) {
	cases := []struct {
		code errs.Code
		want string
	}{
		{errs.ErrDeliveryFailed, "delivery_failed"},
		{errs.ErrDeliveryExpired, "delivery_expired"},
		{errs.ErrFallbackExhausted, "fallback_exhausted"},
		{errs.ErrRetriesExhausted, "retries_exhausted"},
	}
	for _, c := range cases {
		if string(c.code) != c.want {
			t.Errorf("code = %q, want %q", c.code, c.want)
		}
		// These outcome codes are intentionally outside the catalogue (no HTTP/SMPP surface).
		if _, ok := errs.Map(c.code); ok {
			t.Errorf("%q must not be a catalogue entry (it reaches no surface)", c.code)
		}
	}
}

// TestCodeFromSMPPStatus pins the outcome-side reverse mapping the connector records in
// cdr.error_code: every result is a published contract code, never an ad-hoc string.
func TestCodeFromSMPPStatus(t *testing.T) {
	tests := []struct {
		status uint32
		want   errs.Code
	}{
		{errs.StatusThrottled, errs.ErrRateLimited},
		{errs.StatusSubmitFail, errs.ErrSubmitFailed},
		{errs.StatusInvalidDstAddr, errs.ErrInvalidDestination},
		{errs.StatusInvalidSrcAddr, errs.ErrInvalidSource},
		{errs.StatusMsgQueueFull, errs.ErrQueueFull},
		{errs.StatusInvalidMsgLen, errs.ErrValidation},
		{errs.StatusInsufficientCredit, errs.ErrInsufficientCredit},
		{errs.StatusSysErr, errs.ErrInternal},
		{errs.StatusInvalidCmdID, errs.ErrInternal}, // unmapped -> internal_error, not a raw hex string
	}
	for _, tc := range tests {
		got := errs.CodeFromSMPPStatus(tc.status)
		if got != tc.want {
			t.Errorf("CodeFromSMPPStatus(%#x) = %q, want %q", tc.status, got, tc.want)
		}
		if !got.Valid() {
			t.Errorf("CodeFromSMPPStatus(%#x) = %q, which is not a published code", tc.status, got)
		}
	}
}

// TestEveryCodeReachesASurface guards against a code that maps nowhere: it would be invisible to
// both REST and SMPP clients.
func TestEveryCodeReachesASurface(t *testing.T) {
	for _, c := range errs.Codes() {
		_, http := errs.HTTPStatus(c)
		_, smpp := errs.SMPPStatus(c)
		if !http && !smpp {
			t.Errorf("code %q maps to neither an HTTP nor an SMPP status", c)
		}
	}
}

// TestCodesAreSnakeCase pins the naming rule of §11.2.
func TestCodesAreSnakeCase(t *testing.T) {
	for _, c := range errs.Codes() {
		s := c.String()
		if s == "" {
			t.Error("empty code in catalogue")
			continue
		}
		for _, r := range s {
			isLower := r >= 'a' && r <= 'z'
			if !isLower && r != '_' {
				t.Errorf("code %q is not snake_case (offending rune %q)", s, r)
				break
			}
		}
	}
}

// TestCodeIsASentinel is the ergonomic contract: a Code must work with errors.Is directly and
// survive wrapping, so domain code can add context without losing the wire code.
func TestCodeIsASentinel(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		var err error = errs.ErrRecipientOptedOut
		if !goerrors.Is(err, errs.ErrRecipientOptedOut) {
			t.Error("errors.Is() = false on the sentinel itself")
		}
	})

	t.Run("wrapped once", func(t *testing.T) {
		err := fmt.Errorf("optout check for +2250700000000: %w", errs.ErrRecipientOptedOut)
		if !goerrors.Is(err, errs.ErrRecipientOptedOut) {
			t.Error("errors.Is() = false through one wrap")
		}
		if goerrors.Is(err, errs.ErrContentBlocked) {
			t.Error("errors.Is() = true for an unrelated code")
		}
	})

	t.Run("wrapped twice", func(t *testing.T) {
		inner := fmt.Errorf("reserve: %w", errs.ErrInsufficientCredit)
		err := fmt.Errorf("pipeline stage billing: %w", inner)
		if !goerrors.Is(err, errs.ErrInsufficientCredit) {
			t.Error("errors.Is() = false through two wraps")
		}
	})
}

func TestCodeOf(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		want     errs.Code
		wantFind bool
	}{
		{"nil", nil, "", false},
		{"bare sentinel", errs.ErrNoRoute, errs.ErrNoRoute, true},
		{"wrapped", fmt.Errorf("resolve: %w", errs.ErrNoRoute), errs.ErrNoRoute, true},
		{"double wrapped", fmt.Errorf("a: %w", fmt.Errorf("b: %w", errs.ErrRateLimited)), errs.ErrRateLimited, true},
		{"no code", goerrors.New("some infrastructure failure"), "", false},
		{"joined", goerrors.Join(goerrors.New("x"), errs.ErrQueueFull), errs.ErrQueueFull, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, found := errs.CodeOf(tc.err)
			if found != tc.wantFind {
				t.Fatalf("CodeOf() found = %v, want %v", found, tc.wantFind)
			}
			if got != tc.want {
				t.Errorf("CodeOf() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCodeOfPreservesContext checks that wrapping keeps the operator-facing context while the
// code stays machine-readable — the two must not compete.
func TestCodeOfPreservesContext(t *testing.T) {
	err := fmt.Errorf("reserve credit for message 0199a1b2: %w", errs.ErrInsufficientCredit)

	code, ok := errs.CodeOf(err)
	if !ok || code != errs.ErrInsufficientCredit {
		t.Fatalf("CodeOf() = %q, %v; want insufficient_credit, true", code, ok)
	}
	if msg := err.Error(); msg != "reserve credit for message 0199a1b2: insufficient_credit" {
		t.Errorf("error message = %q, context was lost", msg)
	}
}

func TestValid(t *testing.T) {
	tests := []struct {
		code errs.Code
		want bool
	}{
		{errs.ErrNoRoute, true},
		{errs.ErrInternal, true},
		{"", false},
		{"not_a_published_code", false},
		{"NO_ROUTE", false},
	}

	for _, tc := range tests {
		t.Run(string(tc.code), func(t *testing.T) {
			if got := tc.code.Valid(); got != tc.want {
				t.Errorf("Code(%q).Valid() = %v, want %v", tc.code, got, tc.want)
			}
		})
	}
}

func TestMapUnknownCode(t *testing.T) {
	if _, ok := errs.Map("not_a_published_code"); ok {
		t.Error("Map() reported an unknown code as published")
	}
	if _, ok := errs.HTTPStatus("not_a_published_code"); ok {
		t.Error("HTTPStatus() reported a status for an unknown code")
	}
	if _, ok := errs.SMPPStatus("not_a_published_code"); ok {
		t.Error("SMPPStatus() reported a status for an unknown code")
	}
	if errs.Retryable("not_a_published_code") {
		t.Error("Retryable() = true for an unknown code")
	}
}

// TestUnauthenticatedDefaultsToInvalidPasswd covers the one code §11.3 gives two SMPP statuses.
// The default must stay ESME_RINVPASWD: answering ESME_RINVSYSID would tell an attacker which
// system_ids exist, and both spellings carry the same client-facing code anyway.
func TestUnauthenticatedDefaultsToInvalidPasswd(t *testing.T) {
	got, ok := errs.SMPPStatus(errs.ErrUnauthenticated)
	if !ok || got != errs.StatusInvalidPasswd {
		t.Errorf("unauthenticated status = %#x, want %#x (ESME_RINVPASWD)", got, errs.StatusInvalidPasswd)
	}
	if errs.StatusInvalidSysID != 0x0F {
		t.Errorf("StatusInvalidSysID = %#x, want 0x0F (ESME_RINVSYSID)", errs.StatusInvalidSysID)
	}
}

// TestCodesIsASnapshot verifies Codes() hands back a copy: a caller must not be able to reach
// into the catalogue and mutate the contract.
func TestCodesIsASnapshot(t *testing.T) {
	first := errs.Codes()
	if len(first) == 0 {
		t.Fatal("Codes() returned nothing")
	}
	first[0] = "mutated"

	if second := errs.Codes(); second[0] == "mutated" {
		t.Error("Codes() exposes the catalogue: mutating the result changed it")
	}
}
