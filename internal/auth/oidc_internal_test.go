package auth

import (
	"net/http"
	"testing"
)

// TestKeySetRedirectsNeverDowngradeToPlaintext: config refuses a plaintext JWKS URL in production, and a
// redirect must not reopen what it closed — keys fetched in clear can be swapped for an attacker's.
func TestKeySetRedirectsNeverDowngradeToPlaintext(t *testing.T) {
	request := func(url string) *http.Request {
		r, err := http.NewRequest(http.MethodGet, url, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	for _, tt := range []struct {
		from, to string
		allowed  bool
	}{
		{"https://idp/certs", "http://idp/certs", false},
		{"https://idp/certs", "https://idp/certs/", true},
		{"http://localhost/certs", "http://localhost/certs/", true},
	} {
		err := keySetClient().CheckRedirect(request(tt.to), []*http.Request{request(tt.from)})
		if (err == nil) != tt.allowed {
			t.Errorf("%s → %s: CheckRedirect() = %v, want allowed=%v", tt.from, tt.to, err, tt.allowed)
		}
	}
}
