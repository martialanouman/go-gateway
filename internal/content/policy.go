package content

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// EffectiveStorage resolves a customer's raw content_storage into the policy actually applied at CDR write.
// `inherit` (and any unknown value) falls to the conservative platform default OFF: a customer that has not
// explicitly opted in stores no body. A configurable platform default replaces this constant in a later step.
func EffectiveStorage(cs cp.ContentStorage) cp.ContentStorage {
	switch cs {
	case cp.ContentOff, cp.ContentStoredPlaintext, cp.ContentStoredEncrypted:
		return cs
	default: // ContentInherit or anything unrecognized
		return cp.ContentOff
	}
}

// PolicyLister loads every customer's content_storage. *postgres.CustomerRepo satisfies it;
// declared consumer-side.
type PolicyLister interface {
	ListContentStorage(ctx context.Context) ([]cp.CustomerContentPolicy, error)
}

// PolicySnapshot is an immutable, lock-free map of customer_id → effective content_storage, loaded once at
// boot (the sender-ID snapshot pattern). The data plane resolves a message's storage policy per message with
// a plain map read. A customer absent from the snapshot resolves to OFF.
//
// STALENESS — asymmetric risk. Because the snapshot only refreshes on restart, a policy change takes effect
// only at the next data-plane restart. In the ENABLING direction (off→stored_*) this is safe: the gateway
// merely stores too little until then. In the DISABLING direction (stored_*→off, e.g. a consent withdrawal or
// a GDPR request) it is UNSAFE: the data plane keeps storing bodies until restart, so the operator must
// restart (or roll) the ingest pods to make an opt-out effective. Hot reload — with opt-out as its first case
// — is the real fix and is a pre-prod prerequisite (tracked). content_storage changes are otherwise rare, so
// a boot snapshot is the right first cut.
type PolicySnapshot struct {
	byCustomer map[uuid.UUID]cp.ContentStorage
}

// LoadPolicySnapshot reads every customer's content_storage once and indexes the EFFECTIVE policy per customer
// (inherit already resolved to off). An empty snapshot is valid: every customer then resolves to off.
func LoadPolicySnapshot(ctx context.Context, lister PolicyLister) (*PolicySnapshot, error) {
	rows, err := lister.ListContentStorage(ctx)
	if err != nil {
		return nil, fmt.Errorf("content: load content-storage snapshot: %w", err)
	}
	byCustomer := make(map[uuid.UUID]cp.ContentStorage, len(rows))
	for _, r := range rows {
		byCustomer[r.CustomerID] = EffectiveStorage(r.ContentStorage)
	}
	return &PolicySnapshot{byCustomer: byCustomer}, nil
}

// For returns the customer's effective content-storage policy (off when the customer is unknown).
func (s *PolicySnapshot) For(customerID uuid.UUID) cp.ContentStorage {
	if cs, ok := s.byCustomer[customerID]; ok {
		return cs
	}
	return cp.ContentOff
}
