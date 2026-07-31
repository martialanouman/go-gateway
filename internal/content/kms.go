package content

import "context"

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
