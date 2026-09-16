package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// StaticVerifier accepts a fixed set of operator tokens. It exists so the authorization path is
// real while the identity provider is not yet built; it is NOT an authentication system, and it is
// replaced wholesale at M12. Tokens are compared in constant time.
type StaticVerifier struct {
	entries []staticEntry
}

// fingerprintBytes is how much of the SHA-256 digest the fingerprint keeps: 8 octets, 16 hex characters,
// 64 bits. Production refuses a token under 32 characters (cmd/admin-api-svc), so the prefix identifies
// an operator without offering a digest worth brute-forcing.
const fingerprintBytes = 8

// Fingerprint is the identity recorded for an operator token wherever a principal is written down —
// audit rows, job rows, logs. It is "tok_" and a truncated SHA-256 of the token, never the token: those
// records outlive the token and are read by people who must not be able to replay it. Migration
// 0014_operator_fingerprint computes the same value in SQL for the rows written before it existed.
func Fingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "tok_" + hex.EncodeToString(sum[:fingerprintBytes])
}

type staticEntry struct {
	token     string
	principal Principal
}

// NewStaticVerifier parses "token:scope|scope" entries (config.HTTP.AdminTokens). Each entry's
// subject is the token's Fingerprint; the pipe-separated scopes must be known. An empty list is allowed (a
// verifier that rejects everything), which is valid on a laptop; cmd/admin-api-svc enforces the
// "at least one token in production" policy before wiring this verifier.
func NewStaticVerifier(entries []string) (*StaticVerifier, error) {
	parsed := make([]staticEntry, 0, len(entries))
	for i, raw := range entries {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		token, scopeSpec, ok := strings.Cut(raw, ":")
		if !ok || strings.TrimSpace(token) == "" {
			return nil, fmt.Errorf("admin token entry %d: want \"token:scope|scope\"", i)
		}

		var scopes []Scope
		for _, s := range strings.Split(scopeSpec, "|") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			scope := Scope(s)
			if !knownScope(scope) {
				return nil, fmt.Errorf("admin token entry %d: unknown scope %q", i, s)
			}
			scopes = append(scopes, scope)
		}
		parsed = append(parsed, staticEntry{
			token:     token,
			principal: Principal{Subject: Fingerprint(token), Scopes: scopes},
		})
	}
	return &StaticVerifier{entries: parsed}, nil
}

// Verify returns the Principal for token, or ErrUnauthenticated. It compares against every
// configured token in constant time and does not stop at the first match: returning early would
// leak, through response timing, which prefix of a token is correct.
func (v *StaticVerifier) Verify(_ context.Context, token string) (Principal, error) {
	var match *staticEntry
	for i := range v.entries {
		if subtle.ConstantTimeCompare([]byte(v.entries[i].token), []byte(token)) == 1 {
			match = &v.entries[i]
		}
	}
	if match == nil {
		return Principal{}, errs.ErrUnauthenticated
	}
	return match.principal, nil
}

func knownScope(s Scope) bool {
	switch s {
	case ScopeAdminRead, ScopeAdminWrite, ScopeContentRead, ScopeContentErase, ScopeGDPRErase, ScopeMSISDNReveal,
		ScopeCDRExportBulk:
		return true
	default:
		return false
	}
}
