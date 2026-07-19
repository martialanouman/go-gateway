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

// BindCredential is the projection an SMPP bind authentication needs: the stored password hash to
// verify against, plus the credential, channel and account/customer state that gate the bind
// (invariant d). Like APIKeyPrincipal it is an auth-path projection, not a control-plane resource.
//
// The statuses and SMPPEnabled are carried rather than pre-filtered so the caller answers with the
// right SMPP command_status: an unknown system_id or a wrong password is ESME_RINVPASWD, while a
// known bind on a disabled channel or a suspended account is ESME_RBINDFAIL — not "not found".
//
// PasswordHash is secret-bearing (an argon2id PHC string): it exists only on the authentication path,
// is consumed immediately by internal/credential, and must never be logged.
type BindCredential struct {
	AccountID        uuid.UUID
	CustomerID       uuid.UUID
	PasswordHash     string
	CredentialStatus CredentialStatus
	SMPPEnabled      bool
	AllowedBindType  BindType
	MaxSessions      int32
	AccountStatus    AccountStatus
	CustomerStatus   CustomerStatus
}

// EffectiveStatus is the status the bind actually experiences: the more restrictive of the account's
// own status and its customer's (ADR-0006), as for APIKeyPrincipal.
func (c BindCredential) EffectiveStatus() AccountStatus {
	return EffectiveAccountStatus(c.CustomerStatus, c.AccountStatus)
}
