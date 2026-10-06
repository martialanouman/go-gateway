// Package senderid authorizes a message's source address (the sender ID) against its customer's active
// registered sender IDs (spec §6.19). Every sender must be registered, numeric ones included (ADR-0020). It is the real implementation behind
// the frozen pipeline.sender_id stage (step-060): the authorization runs off an immutable snapshot the
// router swaps whole on each config-sync invalidation (step-390), so every message is checked lock-free.
package senderid

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// ActiveSenderIDLister loads every active sender ID across customers. *postgres.SenderIDRepo
// satisfies it structurally.
type ActiveSenderIDLister interface {
	ListActive(ctx context.Context) ([]cp.SenderID, error)
}

// Authorizer answers whether a source address is permitted for a customer, against an immutable
// snapshot. It is safe for concurrent reads: nothing mutates after LoadSnapshot.
type Authorizer struct {
	activeByCustomer map[uuid.UUID]map[string]cp.TrafficCategory
}

// LoadSnapshot reads the active sender IDs once and indexes them for per-message lookup. An empty
// snapshot is valid: every source is then rejected.
func LoadSnapshot(ctx context.Context, ids ActiveSenderIDLister) (*Authorizer, error) {
	active, err := ids.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("senderid: load active sender ids: %w", err)
	}

	a := &Authorizer{activeByCustomer: make(map[uuid.UUID]map[string]cp.TrafficCategory)}
	for _, s := range active {
		// Defense in depth over the query's WHERE clause: only 'active' registrations authorize.
		if s.Status != cp.SenderIDActive {
			continue
		}
		set := a.activeByCustomer[s.CustomerID]
		if set == nil {
			set = make(map[string]cp.TrafficCategory)
			a.activeByCustomer[s.CustomerID] = set
		}
		set[s.Address] = s.TrafficCategory
	}
	return a, nil
}

// Authorize returns the traffic category declared for from when it is an active registered sender ID of
// the customer, else ErrSenderIDNotAuthorized.
func (a *Authorizer) Authorize(_ context.Context, customerID uuid.UUID, from string) (cp.TrafficCategory, error) {
	// The match is exact (byte-for-byte, case- and whitespace-sensitive): the source_addr placed on the
	// wire must be exactly a carrier-approved sender ID. A case variant ("Bank" vs the approved "BANK")
	// or a padded value is deliberately rejected — the schema registers addresses case-sensitively
	// (sender_ids_uq), so a customer may hold "ACME" and "acme" as distinct IDs, and authorizing a
	// casing that was not approved would let an unapproved sender ID reach the operator. From is never
	// rewritten here (that is §6.16, pre-dispatch), so we never authorize one value and send another.
	if category, registered := a.activeByCustomer[customerID][from]; registered {
		return category, nil
	}
	return "", errs.ErrSenderIDNotAuthorized
}

// Holder keeps the current Authorizer behind an atomic pointer: loaded at boot, swapped on each
// invalidation, so a sender ID registered or disabled after boot reaches the next message.
type Holder struct {
	snap atomic.Pointer[Authorizer]
}

// Store swaps in a freshly loaded snapshot.
func (h *Holder) Store(a *Authorizer) { h.snap.Store(a) }

// Authorize checks against the current snapshot.
func (h *Holder) Authorize(ctx context.Context, customerID uuid.UUID, from string) (cp.TrafficCategory, error) {
	return h.snap.Load().Authorize(ctx, customerID, from)
}
