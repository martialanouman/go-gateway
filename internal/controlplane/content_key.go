package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// ContentKeyStatus is the lifecycle state of a per-customer content key (control_plane.content_keys.status).
// A customer has at most one active key; rotation retires the old one (its CDR rows stay decryptable) and
// makes a new one active. destroyed is the crypto-shred terminal state (RGPD), reached only by a deliberate
// destroy, never by rotation.
type ContentKeyStatus string

// The content-key lifecycle states (mirrors the content_keys.status CHECK).
const (
	ContentKeyActive    ContentKeyStatus = "active"
	ContentKeyRetired   ContentKeyStatus = "retired"
	ContentKeyDestroyed ContentKeyStatus = "destroyed"
)

// Valid reports whether s is a published content-key status.
func (s ContentKeyStatus) Valid() bool {
	switch s {
	case ContentKeyActive, ContentKeyRetired, ContentKeyDestroyed:
		return true
	default:
		return false
	}
}

// ContentKey is a per-customer data key (DEK) sealed by the KMS (control_plane.content_keys, §6.14). WrappedKey
// is the KMS-sealed DEK; the plaintext DEK is never stored here. KMSKeyRef records which master key (KEK)
// sealed it, so the wrapped key can be unwrapped by the right provider. RetiredAt/DestroyedAt are set when the
// key leaves the active state.
type ContentKey struct {
	ID          uuid.UUID
	CustomerID  uuid.UUID
	WrappedKey  []byte
	KMSKeyRef   string
	Status      ContentKeyStatus
	CreatedAt   time.Time
	RetiredAt   *time.Time
	DestroyedAt *time.Time
}
