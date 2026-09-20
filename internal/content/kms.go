package content

import (
	"context"
	"errors"
)

// KMS seals and unseals a data key with a master key (KEK) it holds — envelope encryption. The plaintext
// DEK never leaves the caller; the KMS only ever sees it to wrap it, and returns the wrapped form to
// persist (content_keys.wrapped_key). The real provider (AWS/GCP/Vault) is an infra decision (§14) and is
// interchangeable behind this interface; no cloud SDK is imported here. LocalKMS is the dev implementation.
//
// The interface is intentionally small: the caller generates the DEK (GenerateDataKey), so the KMS is only
// a wrap/unwrap oracle over an opaque master key.
type KMS interface {
	// KeyRef is a stable reference to the master key currently used to wrap new DEKs. It is persisted
	// alongside a wrapped key (content_keys.kms_key_ref) so an operator can tell which KEK a key belongs to.
	KeyRef() string
	// WrapDataKey seals a plaintext DEK under the master key and returns the wrapped form. The plaintext DEK
	// is not retained.
	WrapDataKey(ctx context.Context, dek []byte) ([]byte, error)
	// UnwrapDataKey reverses WrapDataKey. Tampering or the wrong master key fails cleanly (ErrDecrypt) and
	// returns no key material.
	UnwrapDataKey(ctx context.Context, wrapped []byte) ([]byte, error)
}

// ErrNoKeyRef is returned when a KMS wraps without naming the master key it used. It carries no key
// material.
var ErrNoKeyRef = errors.New("content: KMS returned no key reference")

// KeyRefOf reads the reference to persist beside a wrapped key, refusing an empty one.
//
// Nothing in the KMS contract promises a non-empty KeyRef — only LocalKMS refuses one at construction
// (ErrEmptyKeyRef), and LocalKMS is the development implementation a real AWS/GCP/Vault provider
// replaces. An empty reference is not caught downstream either: the *_kms_key_ref columns are NOT NULL,
// and ” satisfies that. The row then opens today and a future master-key rotation cannot place it —
// a failure that surfaces only once the key it needed is gone.
//
// Every caller that persists a wrapped key goes through here rather than calling KeyRef directly, so the
// check cannot be present on one write path and missing on its neighbour.
func KeyRefOf(kms KMS) (string, error) {
	ref := kms.KeyRef()
	if ref == "" {
		return "", ErrNoKeyRef
	}
	return ref, nil
}
