// Package senderid authorizes a message's source address (the sender ID) against each account's
// policy and its customer's registered sender IDs (spec §6.19). It is the real implementation behind
// the frozen pipeline.sender_id stage (step-060): the authorization runs off an immutable snapshot
// loaded once at startup, so the router checks every message lock-free. Hot reload arrives with M7.
package senderid

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// PolicyLister loads every account's sender-ID policy with its owning customer.
// *postgres.AccountRepo satisfies it structurally.
type PolicyLister interface {
	ListSenderIDPolicies(ctx context.Context) ([]cp.AccountSenderIDPolicy, error)
}

// ActiveSenderIDLister loads every active sender ID across customers. *postgres.SenderIDRepo
// satisfies it structurally.
type ActiveSenderIDLister interface {
	ListActive(ctx context.Context) ([]cp.SenderID, error)
}

// Authorizer answers whether a source address is permitted for an account, against an immutable
// snapshot. It is safe for concurrent reads: nothing mutates after LoadSnapshot.
type Authorizer struct {
	policyByAccount  map[uuid.UUID]cp.SenderIDPolicy
	activeByCustomer map[uuid.UUID]map[string]struct{}
}

// LoadSnapshot reads the account policies and active sender IDs once and indexes them for per-message
// lookup. An empty snapshot is valid: every account then falls to the strict default (see Authorize).
func LoadSnapshot(ctx context.Context, policies PolicyLister, ids ActiveSenderIDLister) (*Authorizer, error) {
	pols, err := policies.ListSenderIDPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("senderid: load account policies: %w", err)
	}
	active, err := ids.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("senderid: load active sender ids: %w", err)
	}

	a := &Authorizer{
		policyByAccount:  make(map[uuid.UUID]cp.SenderIDPolicy, len(pols)),
		activeByCustomer: make(map[uuid.UUID]map[string]struct{}),
	}
	for _, p := range pols {
		a.policyByAccount[p.AccountID] = p.Policy
	}
	for _, s := range active {
		// Defense in depth over the query's WHERE clause: only 'active' registrations authorize.
		if s.Status != cp.SenderIDActive {
			continue
		}
		set := a.activeByCustomer[s.CustomerID]
		if set == nil {
			set = make(map[string]struct{})
			a.activeByCustomer[s.CustomerID] = set
		}
		set[s.Address] = struct{}{}
	}
	return a, nil
}

// Authorize returns nil if from is a permitted source address for the account, else
// ErrSenderIDNotAuthorized. An account missing from the (cold) snapshot fails safe: it is treated as
// strict, so an unregistered source is rejected rather than silently allowed.
func (a *Authorizer) Authorize(_ context.Context, accountID, customerID uuid.UUID, from string) error {
	policy, ok := a.policyByAccount[accountID]
	if !ok {
		policy = cp.SenderIDStrict
	}
	if policy == cp.SenderIDPolicyDisabled {
		return nil
	}

	// The match is exact (byte-for-byte, case- and whitespace-sensitive): the source_addr placed on the
	// wire must be exactly a carrier-approved sender ID. A case variant ("Bank" vs the approved "BANK")
	// or a padded value is deliberately rejected — the schema registers addresses case-sensitively
	// (sender_ids_uq), so a customer may hold "ACME" and "acme" as distinct IDs, and authorizing a
	// casing that was not approved would let an unapproved sender ID reach the operator. From is never
	// rewritten here (that is §6.16, pre-dispatch), so we never authorize one value and send another.
	if _, registered := a.activeByCustomer[customerID][from]; registered {
		return nil
	}
	// allow_unregistered_numeric tolerates a purely numeric source (a short code or MSISDN) that is
	// not registered; strict requires a registered match.
	if policy == cp.SenderIDAllowUnregisteredNum && isNumeric(from) {
		return nil
	}
	return errs.ErrSenderIDNotAuthorized
}

// isNumeric reports whether s is a non-empty numeric sender ID: an optional single leading '+' (E.164)
// followed by digits only. An alphanumeric sender ID (e.g. "PROMO") is not numeric.
func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '+' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
