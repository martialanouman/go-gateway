package auth_test

import (
	"context"
	goerrors "errors"
	"testing"

	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestStaticVerifierParsesTokensAndScopes: a well-formed entry yields a principal whose subject is
// the token and whose scopes are exactly those declared.
func TestStaticVerifierParsesTokensAndScopes(t *testing.T) {
	v, err := auth.NewStaticVerifier([]string{"tok-abc:admin:read|admin:write"})
	if err != nil {
		t.Fatalf("NewStaticVerifier() error = %v", err)
	}

	p, err := v.Verify(context.Background(), "tok-abc")
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if p.Subject != "tok-abc" {
		t.Errorf("Subject = %q, want tok-abc", p.Subject)
	}
	if !p.Has(auth.ScopeAdminRead) || !p.Has(auth.ScopeAdminWrite) {
		t.Errorf("scopes = %v, want admin:read and admin:write", p.Scopes)
	}
	if p.Has(auth.ScopeContentRead) {
		t.Error("principal should not hold content:read")
	}
}

// TestStaticVerifierRejectsUnknownToken: an unrecognised token is ErrUnauthenticated, so the
// middleware can answer 401.
func TestStaticVerifierRejectsUnknownToken(t *testing.T) {
	v, _ := auth.NewStaticVerifier([]string{"good:admin:read"})

	_, err := v.Verify(context.Background(), "bad")
	if !goerrors.Is(err, errs.ErrUnauthenticated) {
		t.Errorf("Verify(bad) error = %v, want unauthenticated", err)
	}
}

// TestStaticVerifierRejectsMalformedConfig: a bad token entry or an unknown scope fails at
// construction, so a broken operator-token list stops the boot rather than silently accepting no
// one.
func TestStaticVerifierRejectsMalformedConfig(t *testing.T) {
	for _, bad := range [][]string{
		{"no-colon-no-scopes"},
		{":admin:read"},
		{"tok:not-a-real-scope"},
	} {
		if _, err := auth.NewStaticVerifier(bad); err == nil {
			t.Errorf("NewStaticVerifier(%v) succeeded, want an error", bad)
		}
	}
}

// TestStaticVerifierSkipsBlankEntries: a trailing empty entry from a comma-split env value is
// ignored, not treated as a malformed token.
func TestStaticVerifierSkipsBlankEntries(t *testing.T) {
	v, err := auth.NewStaticVerifier([]string{"tok:admin:read", "", "  "})
	if err != nil {
		t.Fatalf("NewStaticVerifier() error = %v", err)
	}
	if _, err := v.Verify(context.Background(), "tok"); err != nil {
		t.Errorf("Verify() error = %v", err)
	}
}
