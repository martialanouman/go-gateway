// Package content holds the envelope-encryption primitive for message content (§14, spec §6.14/§6.23):
// a per-customer AES-256 data key (DEK) encrypts the body, and a KMS-held master key (KEK) seals the DEK.
// Only ciphertext and wrapped keys are ever persisted; the plaintext body and the plaintext DEK exist
// only transiently in memory. Nothing here logs a key or a plaintext — that is invariant (a). This package
// is the crypto primitive only: it holds no cloud SDK and knows nothing of the CDR or the control plane
// (that wiring lands in later M10 steps).
package content

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// dekSize is the AES-256 data-key length in bytes. Both DEKs and the local KEK are 256-bit.
const dekSize = 32

// ErrKeySize is returned when a key is not the required AES-256 length. It carries no key material.
var ErrKeySize = errors.New("content: key must be 32 bytes (AES-256)")

// ErrDecrypt is returned when authenticated decryption fails — a wrong key, a mismatched AAD, a truncated
// or tampered ciphertext. It is deliberately opaque: it reveals neither the key nor any plaintext, so it is
// safe to log or return to a caller.
var ErrDecrypt = errors.New("content: decryption failed")

// GenerateDataKey returns a fresh random 256-bit data key (DEK). The caller wraps it with a KMS before
// persisting it (content_keys.wrapped_key) and uses the plaintext only transiently to seal a body.
//
// SealContent uses a random 96-bit nonce, which is safe up to roughly 2^32 seals under one key (NIST SP
// 800-38D); beyond that, nonce-collision risk stops being negligible. A per-customer DEK on a high-volume
// account can approach that ceiling, so the step that persists and assigns DEKs (content_keys lifecycle)
// must enforce a rotation cadence well below 2^32 messages per key.
func GenerateDataKey() ([]byte, error) {
	key := make([]byte, dekSize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("content: generate data key: %w", err)
	}
	return key, nil
}

// SealContent encrypts plaintext under the 256-bit key with AES-256-GCM and returns nonce||ciphertext. aad
// is authenticated but not encrypted (nil is allowed); binding a stable context to it (e.g. a message id)
// makes a ciphertext undecryptable outside that context. The nonce is random per call, so sealing the same
// plaintext twice yields different ciphertexts.
func SealContent(key, plaintext, aad []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	return gcmSeal(gcm, plaintext, aad)
}

// OpenContent reverses SealContent: it authenticates and decrypts nonce||ciphertext under the key and aad.
// Any tampering, a wrong key, or a mismatched aad returns ErrDecrypt and no plaintext.
func OpenContent(key, ciphertext, aad []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	return gcmOpen(gcm, ciphertext, aad)
}

// newGCM builds an AES-256-GCM AEAD from a 32-byte key, rejecting any other length rather than letting the
// cipher silently pick AES-128/192.
func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != dekSize {
		return nil, ErrKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("content: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("content: new gcm: %w", err)
	}
	return gcm, nil
}

// gcmSeal prepends a fresh random nonce to the sealed output: Seal appends the ciphertext (and tag) to the
// nonce slice it is given, so the result is nonce||ciphertext||tag in one allocation.
func gcmSeal(gcm cipher.AEAD, plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("content: nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plaintext, aad), nil
}

// gcmOpen splits nonce||ciphertext and authenticates+decrypts it. A blob shorter than the nonce, or any
// authentication failure, returns ErrDecrypt without exposing key or plaintext.
func gcmOpen(gcm cipher.AEAD, blob, aad []byte) ([]byte, error) {
	ns := gcm.NonceSize()
	if len(blob) < ns {
		return nil, ErrDecrypt
	}
	nonce, ciphertext := blob[:ns], blob[ns:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plaintext, nil
}
