package auth_test

import (
	"context"
	goerrors "errors"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestStaticVerifierParsesTokensAndScopes: a well-formed entry yields a principal whose subject is
// the token's fingerprint and whose scopes are exactly those declared.
func TestStaticVerifierParsesTokensAndScopes(t *testing.T) {
	v, err := auth.NewStaticVerifier([]string{"tok-abc:admin:read|admin:write"})
	if err != nil {
		t.Fatalf("NewStaticVerifier() error = %v", err)
	}

	p, err := v.Verify(context.Background(), "tok-abc")
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	// The vector is computed outside Go (printf 'tok-abc' | shasum -a 256), so this is not a round trip.
	if p.Subject != "tok_0b9f31c5403adf5a" {
		t.Errorf("Subject = %q, want the token's fingerprint tok_0b9f31c5403adf5a", p.Subject)
	}
	if strings.Contains(p.Subject, "tok-abc") {
		t.Errorf("Subject %q carries the token: it is written to audit tables and logs", p.Subject)
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

// TestStaticVerifierErrorsNeverEchoAnEntry: the error lands in the boot log, and an entry written in the
// wrong order ("admin:read:<token>") puts the token where a scope is expected — so neither a scope nor a
// token is ever quoted, only the entry number and the position.
func TestStaticVerifierErrorsNeverEchoAnEntry(t *testing.T) {
	const secret = "s3cret-operator-token-0123456789"
	_, err := auth.NewStaticVerifier([]string{"admin:read:" + secret})
	if err == nil {
		t.Fatal("NewStaticVerifier() succeeded on a misordered entry, want an error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error %q echoes the token", err)
	}
	if !strings.Contains(err.Error(), "entry 0") {
		t.Errorf("error %q should name the entry", err)
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

// TestFingerprintMatchesAnOutOfProcessDigest pins the identity format every audit row carries. The
// vector comes from shasum, not from Fingerprint itself, and migration 0014 recomputes the same value
// in SQL: changing either side orphans every operator already recorded.
func TestFingerprintMatchesAnOutOfProcessDigest(t *testing.T) {
	t.Parallel()

	if got := auth.Fingerprint("operator-token-for-tests"); got != "tok_534125de141542e2" {
		t.Errorf("Fingerprint = %q, want tok_534125de141542e2", got)
	}
}
