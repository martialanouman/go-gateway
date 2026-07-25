package disconnect_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/session/disconnect"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []disconnect.Event{
		{Scope: disconnect.ScopeAccount, ID: "acct-1", Reason: "credential_revoked"},
		{Scope: disconnect.ScopeCustomer, ID: "cust-9", Reason: "customer_suspended"},
	}
	for _, want := range tests {
		t.Run(want.Reason, func(t *testing.T) {
			t.Parallel()
			got, err := disconnect.Decode(disconnect.Encode(want))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got != want {
				t.Errorf("round-trip = %+v, want %+v", got, want)
			}
		})
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	t.Parallel()
	if _, err := disconnect.Decode([]byte("not json")); err == nil {
		t.Error("Decode(garbage) = nil error, want error")
	}
}

func TestDecodeRejectsUnknownScope(t *testing.T) {
	t.Parallel()
	// A payload with a scope outside the known set must not silently decode to a zero scope that
	// could match every session: an unknown scope is a hard error.
	if _, err := disconnect.Decode([]byte(`{"scope":"planet","id":"x","reason":"y"}`)); err == nil {
		t.Error("Decode(unknown scope) = nil error, want error")
	}
}
