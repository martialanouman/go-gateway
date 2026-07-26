package optout

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
)

// InboundNumberLister lists inbound numbers for the sender→inbound-number index.
// *postgres.InboundNumberRepo satisfies it structurally.
type InboundNumberLister interface {
	List(ctx context.Context) ([]cp.InboundNumber, error)
}

// InboundNumberIndex maps a sender address to the inbound number it is, so an MT's From can be
// resolved to the inbound_number opt-out scope (a STOP to a shortcode suppresses MTs sent from it).
// Immutable after build; keyed by the same normalized-address convention as the MO router
// (e164.NormalizeAddr), so a sender resolves identically on both paths.
//
// The key is the address alone, not (address, country_code) as the schema's uniqueness is: an MT's
// From carries no country, so it cannot be resolved otherwise. For a single-country aggregator this
// is exact (one row per address); a genuine MSISDN already embeds its country code, so only a bare
// shortcode reused across countries could collide — out of scope until multi-country (revisit with
// live STOP writes, step-063).
type InboundNumberIndex struct {
	byAddr map[string]uuid.UUID
}

// LoadInboundNumberIndex reads the inbound numbers once and indexes their ids by normalized address.
func LoadInboundNumberIndex(ctx context.Context, lister InboundNumberLister) (*InboundNumberIndex, error) {
	nums, err := lister.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("optout: load inbound numbers: %w", err)
	}
	byAddr := make(map[string]uuid.UUID, len(nums))
	for _, n := range nums {
		byAddr[e164.NormalizeAddr(n.Address)] = n.ID
	}
	return &InboundNumberIndex{byAddr: byAddr}, nil
}

// Resolve returns the inbound number id a sender address belongs to, if the platform owns it. A
// sender that is not one of our inbound numbers (a client's own alphanumeric or MSISDN) resolves to
// no inbound_number scope.
func (i *InboundNumberIndex) Resolve(from string) (uuid.UUID, bool) {
	id, ok := i.byAddr[e164.NormalizeAddr(from)]
	return id, ok
}

// Enforcer answers the MT opt-out question: is a destination suppressed in ANY scope applicable to
// this message (spec §6.20)? The scopes are platform, the sending customer, the sending account, and
// — when the sender is one of our inbound numbers — that inbound number. It short-circuits on the
// first suppressed scope and consults the database only behind a Bloom hit (via Guard).
type Enforcer struct {
	guard   *Guard
	inbound *InboundNumberIndex
}

// NewEnforcer composes the opt-out Guard with the inbound-number index. A nil index is treated as
// empty (no inbound_number scope ever applies), so the enforcer never panics on a missing index.
func NewEnforcer(guard *Guard, inbound *InboundNumberIndex) *Enforcer {
	if inbound == nil {
		inbound = &InboundNumberIndex{byAddr: map[string]uuid.UUID{}}
	}
	return &Enforcer{guard: guard, inbound: inbound}
}

// IsOptedOut reports whether dest is suppressed in any scope applicable to an MT from (accountID,
// customerID) sent as from. The (accountID, customerID) order matches SenderIDAuthorizer.Authorize so
// the two compliance stages read alike. dest must be E.164-normalized (the pipeline destination is).
// An error from the exact confirmation is returned for the caller to treat as transient — the message
// is neither passed nor rejected on a database fault.
func (e *Enforcer) IsOptedOut(ctx context.Context, accountID, customerID uuid.UUID, from, dest string) (bool, error) {
	// Widest to narrowest; any match blocks. Platform has no scope_id.
	if ok, err := e.guard.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, dest); err != nil || ok {
		return ok, err
	}
	if ok, err := e.guard.IsSuppressed(ctx, cp.SuppressionScopeCustomer, &customerID, dest); err != nil || ok {
		return ok, err
	}
	if ok, err := e.guard.IsSuppressed(ctx, cp.SuppressionScopeAccount, &accountID, dest); err != nil || ok {
		return ok, err
	}
	// The inbound_number scope applies only when this MT is sent from one of our inbound numbers — the
	// channel a recipient's STOP targets (the default scope of an MO STOP, §6.20).
	if id, ok := e.inbound.Resolve(from); ok {
		if ok, err := e.guard.IsSuppressed(ctx, cp.SuppressionScopeInboundNumber, &id, dest); err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}
