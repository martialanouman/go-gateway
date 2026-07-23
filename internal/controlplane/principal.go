package controlplane

import (
	"time"

	"github.com/google/uuid"
)

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
// PasswordHash and PreviousSecretHash are secret-bearing (argon2id PHC strings): they exist only on
// the authentication path, are consumed immediately by internal/credential, and must never be logged.
// Neither is ever carried into a control-plane DTO — the Admin read path has no field for them.
type BindCredential struct {
	AccountID    uuid.UUID
	CustomerID   uuid.UUID
	PasswordHash string
	// PreviousSecretHash and GraceExpiresAt describe a rotation grace window (§6.3, step-027): after a
	// rotation with a grace period, the superseded secret keeps binding until GraceExpiresAt, then is
	// cut off for good. Both are nil outside a window, and the pair is only meaningful together — a
	// hash without a deadline must never authenticate.
	PreviousSecretHash *string
	GraceExpiresAt     *time.Time
	CredentialStatus   CredentialStatus
	SMPPEnabled        bool
	AllowedBindType    BindType
	MaxSessions        int32
	// QuerySMEnabled and CancelSMEnabled gate the optional SMPP operations (§6.22): a disabled op is
	// answered ESME_RINVCMDID, as if unsupported. They are account switches, resolved once at bind.
	QuerySMEnabled  bool
	CancelSMEnabled bool
	AccountStatus   AccountStatus
	CustomerStatus  CustomerStatus
}

// EffectiveStatus is the status the bind actually experiences: the more restrictive of the account's
// own status and its customer's (ADR-0006), as for APIKeyPrincipal.
func (c BindCredential) EffectiveStatus() AccountStatus {
	return EffectiveAccountStatus(c.CustomerStatus, c.AccountStatus)
}
