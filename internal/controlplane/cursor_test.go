package controlplane_test

import (
	"encoding/base64"
	goerrors "errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// mustB64 renders a valid base64url payload, so a test can build a cursor that decodes cleanly but
// carries a bad payload — exercising the version/uuid checks rather than the base64 check.
func mustB64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

// TestCursorRoundTrips: a position encoded and decoded returns the same id, so paging forward lands
// exactly where the previous page ended.
func TestCursorRoundTrips(t *testing.T) {
	id := uuid.New()
	got, err := cp.DecodeCursor(cp.EncodeCursor(id))
	if err != nil {
		t.Fatalf("DecodeCursor() error = %v", err)
	}
	if got != id {
		t.Errorf("round-trip id = %s, want %s", got, id)
	}
}

// TestEmptyCursorIsTheFirstPage: the zero cursor decodes to the nil UUID (no lower bound), and the
// nil id encodes back to the empty cursor (no next page).
func TestEmptyCursorIsTheFirstPage(t *testing.T) {
	got, err := cp.DecodeCursor("")
	if err != nil {
		t.Fatalf("DecodeCursor(\"\") error = %v", err)
	}
	if got != uuid.Nil {
		t.Errorf("DecodeCursor(\"\") = %s, want the nil UUID", got)
	}
	if c := cp.EncodeCursor(uuid.Nil); c != "" {
		t.Errorf("EncodeCursor(nil) = %q, want empty", c)
	}
}

// TestDecodeCursorRejectsClientGarbageAsValidationNotInternal: a cursor is client input. A bad one
// is a 422, never a 500 — so it must carry ErrValidation, not an unmapped error.
func TestDecodeCursorRejectsClientGarbageAsValidationNotInternal(t *testing.T) {
	for _, bad := range []cp.Cursor{
		"not-base64-@@@",
		cp.Cursor(mustB64("garbage-no-colon")),
		cp.Cursor(mustB64("cp9:" + uuid.New().String())), // unknown version
		cp.Cursor(mustB64("cp1:not-a-uuid")),
	} {
		_, err := cp.DecodeCursor(bad)
		if err == nil {
			t.Errorf("DecodeCursor(%q) succeeded, want a validation error", bad)
			continue
		}
		if code, _ := errs.CodeOf(err); code != errs.ErrValidation {
			t.Errorf("DecodeCursor(%q) code = %q, want validation_error", bad, code)
		}
		if !goerrors.Is(err, errs.ErrValidation) {
			t.Errorf("DecodeCursor(%q) is not ErrValidation", bad)
		}
	}
}
