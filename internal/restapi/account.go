package restapi

import (
	"context"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// AccountInfo is the read-only projection of the caller's own account (api/openapi-public.yaml
// AccountInfo). It carries no secret: neither the API-key nor the bind-password hash is a field here.
type AccountInfo struct {
	AccountID      string             `json:"account_id" format:"uuid"`
	CustomerID     string             `json:"customer_id" format:"uuid"`
	Name           string             `json:"name"`
	Status         string             `json:"status" enum:"active,suspended,closed"`
	Channels       AccountChannels    `json:"channels"`
	SenderIDPolicy string             `json:"sender_id_policy" enum:"strict,allow_unregistered_numeric,disabled"`
	MaxSessions    int                `json:"max_sessions" minimum:"0" doc:"Max concurrent SMPP binds allowed for this account."`
	SenderIDs      []AccountSenderID  `json:"sender_ids"`
	RateLimits     *AccountRateLimits `json:"rate_limits"`
}

// AccountChannels reports which delivery channels the account may use.
type AccountChannels struct {
	SMPPEnabled bool `json:"smpp_enabled"`
	RESTEnabled bool `json:"rest_enabled"`
}

// AccountSenderID is one customer-level sender address and its carrier-approval state.
type AccountSenderID struct {
	Address string `json:"address"`
	Status  string `json:"status" enum:"pending_carrier_approval,active,disabled"`
}

// AccountRateLimits is the account's throughput ceiling. The whole object is null when no limit is
// configured; each field is independently null when that dimension is unbounded.
type AccountRateLimits struct {
	MaxPerSec     *int `json:"max_per_sec"`
	MaxPerDay     *int `json:"max_per_day"`
	BurstCapacity *int `json:"burst_capacity"`
}

type getAccountOutput struct {
	Body AccountInfo
}

// getAccount returns the authenticated caller's own account, scoped by the principal. It reads three
// control-plane sources — the account row, the customer's sender IDs, and the account's optional rate
// limit — and projects them onto AccountInfo. It changes nothing. Auth (401) and the suspended /
// REST-disabled cases (403) are handled by the shared middleware before this runs; any read failure
// here (including the caller's own account somehow missing, an internal inconsistency) is a 500.
func (s *server) getAccount(ctx context.Context, _ *struct{}) (*getAccountOutput, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return nil, humaerr.FromError(errs.ErrUnauthenticated)
	}

	acc, err := s.deps.Accounts.Get(ctx, principal.AccountID)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "read account", "account_id", principal.AccountID, "err", err)
		return nil, humaerr.FromError(errs.ErrInternal)
	}

	senderIDs, err := s.deps.SenderIDs.ListByCustomer(ctx, principal.CustomerID)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "list sender ids", "customer_id", principal.CustomerID, "err", err)
		return nil, humaerr.FromError(errs.ErrInternal)
	}

	limit, hasLimit, err := s.deps.RateLimits.RateLimit(ctx, principal.AccountID)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "read rate limit", "account_id", principal.AccountID, "err", err)
		return nil, humaerr.FromError(errs.ErrInternal)
	}

	return &getAccountOutput{Body: accountInfo(acc, senderIDs, limit, hasLimit)}, nil
}

// accountInfo projects the domain reads onto the wire shape. sender_ids is always a (possibly empty)
// array, never null, to honour the contract's required non-nullable list.
func accountInfo(acc cp.Account, senderIDs []cp.SenderID, limit cp.RateLimit, hasLimit bool) AccountInfo {
	ids := make([]AccountSenderID, 0, len(senderIDs))
	for _, sid := range senderIDs {
		ids = append(ids, AccountSenderID{Address: sid.Address, Status: string(sid.Status)})
	}

	info := AccountInfo{
		AccountID:      acc.ID.String(),
		CustomerID:     acc.CustomerID.String(),
		Name:           acc.Name,
		Status:         string(acc.Status),
		Channels:       AccountChannels{SMPPEnabled: acc.SMPPEnabled, RESTEnabled: acc.RESTEnabled},
		SenderIDPolicy: string(acc.SenderIDPolicy),
		MaxSessions:    acc.MaxSessions,
		SenderIDs:      ids,
	}
	if hasLimit {
		info.RateLimits = &AccountRateLimits{
			MaxPerSec:     limit.MaxPerSec,
			MaxPerDay:     limit.MaxPerDay,
			BurstCapacity: limit.BurstCapacity,
		}
	}
	return info
}
