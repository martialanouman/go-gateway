package content

import (
	"context"
	"crypto/cipher"
	"errors"
)

// ErrEmptyKeyRef is returned when a LocalKMS is built without a key reference. An empty reference would be
// bound as empty GCM additional data — indistinguishable from none — which silently disables the
// domain-separation guard, so it is rejected up front.
var ErrEmptyKeyRef = errors.New("content: KMS key reference must not be empty")

// LocalKMS is the development KMS: an in-memory AES-256 master key (KEK) that wraps DEKs with AES-256-GCM.
// It exists so the encryption path is real on a laptop and in tests while the production KMS is an infra
// decision (§14) — it is swapped wholesale for the real provider behind the KMS interface and adds nothing
// to the production path. It never persists or logs its master key.
//
// The key reference is bound as GCM additional data, so a wrapped key sealed under one KeyRef cannot be
// unwrapped by a LocalKMS advertising a different one (a cheap domain-separation guard).
type LocalKMS struct {
	keyRef string
	gcm    cipher.AEAD
}

// NewLocalKMS builds a LocalKMS from a 32-byte master key and the reference to record on the keys it wraps.
// The master key must be AES-256 (32 bytes); any other length is rejected.
func NewLocalKMS(masterKey []byte, keyRef string) (*LocalKMS, error) {
	if keyRef == "" {
		return nil, ErrEmptyKeyRef
	}
	gcm, err := newGCM(masterKey)
	if err != nil {
		return nil, err
	}
	return &LocalKMS{keyRef: keyRef, gcm: gcm}, nil
}

// NewDevKMS builds a LocalKMS with a fresh random in-memory master key. It is for tests and single-process
// dev only: the master key is lost on exit, so any keys it wrapped become unreadable across restarts. A
// persistent laptop setup should construct NewLocalKMS from a dev key file instead.
func NewDevKMS() *LocalKMS {
	master, err := GenerateDataKey()
	if err != nil {
		// crypto/rand failing is fatal and unrecoverable; there is no safe way to proceed without a master key.
		panic("content: dev KMS master key: " + err.Error())
	}
	kms, err := NewLocalKMS(master, "local-dev/v1")
	if err != nil {
		panic("content: dev KMS: " + err.Error())
	}
	return kms
}

// KeyRef reports the master-key reference to persist with a wrapped key.
func (k *LocalKMS) KeyRef() string { return k.keyRef }

// WrapDataKey seals the DEK under the master key, binding KeyRef as additional data.
func (k *LocalKMS) WrapDataKey(_ context.Context, dek []byte) ([]byte, error) {
	return gcmSeal(k.gcm, dek, []byte(k.keyRef))
}

// UnwrapDataKey reverses WrapDataKey. A wrapped key sealed under a different master or KeyRef fails with
// ErrDecrypt and yields no key material.
func (k *LocalKMS) UnwrapDataKey(_ context.Context, wrapped []byte) ([]byte, error) {
	return gcmOpen(k.gcm, wrapped, []byte(k.keyRef))
}
