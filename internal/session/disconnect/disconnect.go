// Package disconnect defines the wire contract for the session force-disconnect fan-out (step-032):
// the Redis pub/sub channel and the small event carried on it. session-manager-svc publishes an
// Event when an account or customer loses its authorization (revocation, suspension); every
// smpp-server-svc pod subscribes and force-closes the matching sessions it owns. The event carries
// only identifiers and a reason — never a secret or a message body (§1.9).
package disconnect

import (
	"encoding/json"
	"fmt"
)

// Channel is the Redis pub/sub channel the disconnect fan-out travels on. It is shared verbatim by
// the publisher (session-manager) and every subscriber (smpp-server pods).
const Channel = "session:disconnect"

// Scope names what an Event targets: a single SMPP account, or every account of a customer (used for
// a customer-wide suspension). Each pod resolves the scope against the sessions it owns.
type Scope string

const (
	// ScopeAccount targets the sessions of one smpp_account (Event.ID is the account id).
	ScopeAccount Scope = "account"
	// ScopeCustomer targets the sessions of every account of one customer (Event.ID is the customer id).
	ScopeCustomer Scope = "customer"
)

// valid reports whether s is a known scope. An unknown scope must never match sessions.
func (s Scope) valid() bool { return s == ScopeAccount || s == ScopeCustomer }

// Event is one force-disconnect order. ID is the account or customer UUID (as a string) selected by
// Scope; Reason is a short machine label (e.g. "credential_revoked") logged on the close, never a
// secret.
type Event struct {
	Scope  Scope  `json:"scope"`
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// Encode serialises e for publication. It never fails for a well-formed Event, so it returns only
// the bytes; a malformed Event surfaces at Decode on the subscriber side.
func Encode(e Event) []byte {
	b, _ := json.Marshal(e) //nolint:errchkjson // Event has only string fields; Marshal cannot fail.
	return b
}

// Decode parses a published payload, rejecting a malformed body or an unknown scope so a corrupt or
// forward-incompatible message can never fan out to an over-broad set of sessions.
func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, fmt.Errorf("disconnect: decode: %w", err)
	}
	if !e.Scope.valid() {
		return Event{}, fmt.Errorf("disconnect: decode: unknown scope %q", e.Scope)
	}
	return e, nil
}
