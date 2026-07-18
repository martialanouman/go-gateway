package controlplane

import "github.com/google/uuid"

// APIKeyPrincipal identifies the account and customer behind a presented REST API key, with the
// flags the authentication layer needs to authorise a request. It is a projection for the auth
// path only — not a control-plane resource in its own right.
//
// The statuses and RESTEnabled are carried rather than pre-filtered so the verifier can answer with
// the right code: an unknown key is unauthenticated (401), but a known key on a channel that is
// disabled or an account that is suspended is forbidden (403), not "not found".
type APIKeyPrincipal struct {
	AccountID      uuid.UUID
	CustomerID     uuid.UUID
	AccountStatus  AccountStatus
	CustomerStatus CustomerStatus
	RESTEnabled    bool
}

// EffectiveStatus is the status the caller actually experiences: the more restrictive of the
// account's own status and its customer's (ADR-0006). Suspending the customer suspends the account.
func (p APIKeyPrincipal) EffectiveStatus() AccountStatus {
	return EffectiveAccountStatus(p.CustomerStatus, p.AccountStatus)
}
