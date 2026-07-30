// Package credit is the MT credit-reserve stage of the router pipeline (step-145). It gates on an
// immutable per-customer snapshot so a billing-disabled customer costs ZERO billing round-trips, resolves
// the balance owner from the customer's scope, and reserves credit through the billing gRPC service. The
// message body never enters this package (invariant a): it sees only owner identifiers, a message_id and an
// integer segment count.
package credit

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// Snapshot is an immutable, precompiled map of every billing-enabled customer to its balance scope.
// Presence IS the billing-enabled flag: a customer absent from it is not billed, so the credit stage makes
// no billing call for it. It is built whole and swapped atomically (like routing.Snapshot), so a hot-path
// Lookup never locks and always sees a consistent whole.
type Snapshot struct {
	byCustomer map[uuid.UUID]cp.BalanceScope
}

// BuildSnapshot precompiles the scope of each billing-enabled customer.
func BuildSnapshot(scopes []cp.CustomerBillingScope) *Snapshot {
	byCustomer := make(map[uuid.UUID]cp.BalanceScope, len(scopes))
	for _, s := range scopes {
		byCustomer[s.CustomerID] = s.Scope
	}
	return &Snapshot{byCustomer: byCustomer}
}

// Lookup returns a customer's balance scope and whether billing is enabled for it (present in the
// snapshot). enabled=false means no reservation and no billing call. A nil snapshot (never loaded) reports
// disabled for everyone — the pre-billing pass-through — so a router that has not built the snapshot yet
// bills nobody rather than failing.
func (s *Snapshot) Lookup(customerID uuid.UUID) (scope cp.BalanceScope, enabled bool) {
	if s == nil {
		return "", false
	}
	scope, ok := s.byCustomer[customerID]
	return scope, ok
}

// ScopeLister loads every billing-enabled customer's scope. *postgres.BillingRepo satisfies it; declared
// consumer-side (convention §2).
type ScopeLister interface {
	ListBillingScopes(ctx context.Context) ([]cp.CustomerBillingScope, error)
}

// Holder keeps the current Snapshot behind an atomic pointer. The router loads one at boot and rebuilds it
// on each config-sync invalidation, swapping it whole; the hot-path Lookup reads lock-free. The zero value
// is usable and reports billing disabled for everyone until the first Store.
type Holder struct {
	snap atomic.Pointer[Snapshot]
}

// Store swaps in a freshly built snapshot (boot, then each customers-config invalidation).
func (h *Holder) Store(s *Snapshot) { h.snap.Store(s) }

// Lookup reads the current snapshot, reporting billing disabled when none has been stored yet.
func (h *Holder) Lookup(customerID uuid.UUID) (cp.BalanceScope, bool) {
	return h.snap.Load().Lookup(customerID)
}

// LoadSnapshot builds a fresh snapshot from the durable billing configuration — the router calls it at boot
// and on each customers-config invalidation, then Stores the result in the holder.
func LoadSnapshot(ctx context.Context, lister ScopeLister) (*Snapshot, error) {
	scopes, err := lister.ListBillingScopes(ctx)
	if err != nil {
		return nil, fmt.Errorf("credit: load scope snapshot: %w", err)
	}
	return BuildSnapshot(scopes), nil
}
