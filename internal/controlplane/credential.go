package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// Credential is the masked view of an account credential (control_plane.credentials). It has NO
// secret field, by construction: the read path physically cannot leak a secret because there is
// nowhere to put one. The secret travels only as a separate value returned once, at creation and
// rotation.
type Credential struct {
	ID             uuid.UUID
	AccountID      uuid.UUID
	Type           CredentialType
	SystemID       *string
	Status         CredentialStatus
	LastUsedAt     *time.Time
	GraceExpiresAt *time.Time
	CreatedAt      time.Time
	RotatedAt      *time.Time
}

// NewCredential is the input to issue a credential. The hashes are computed by internal/credential
// before this reaches the repository; SystemID is set for a smpp_bind and nil for an api_key,
// matching credentials_shape_ck.
type NewCredential struct {
	AccountID    uuid.UUID
	Type         CredentialType
	SystemID     *string
	PasswordHash *string // set for smpp_bind
	APIKeyHash   *string // set for api_key
}

// CredentialRotation is the input to rotate a credential's secret. NewHash is the freshly hashed
// secret. When Grace is non-nil the previous hash stays valid until now+Grace, recorded in
// previous_secret_hash and grace_expires_at; a nil Grace is an immediate cutover.
type CredentialRotation struct {
	NewHash string
	Grace   *time.Duration
}
