package auth

import (
	"context"
	"crypto/subtle"
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
		for pos, s := range strings.Split(scopeSpec, "|") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			scope := Scope(s)
			if !knownScope(scope) {
				// The value is not echoed: an entry written in the wrong order puts the token here, and this
				// error lands in the boot log.
				return nil, fmt.Errorf("admin token entry %d: unknown scope at position %d", i, pos+1)
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
		ScopeCDRExportBulk, ScopeAuditRead:
		return true
	default:
		return false
	}
}
