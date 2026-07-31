package content

import (
	"crypto/hkdf"
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
)

// EnvelopeVersion1 is the first content-envelope format: a leading version byte, then nonce||ciphertext of an
// AES-256-GCM seal under a PER-MESSAGE subkey. Versioning the format up front is what lets the decryption path
// (and any future algorithm change) stay forward-compatible.
const EnvelopeVersion1 byte = 0x01

// ErrEnvelope is returned when an envelope is malformed (too short, or an unknown version). Like ErrDecrypt it
// is opaque — no key or plaintext.
var ErrEnvelope = errors.New("content: malformed envelope")

// SealBody encrypts a message body for CDR storage, returning EnvelopeVersion1 || nonce || ciphertext.
//
// It does NOT seal directly under the per-customer DEK. A DEK is shared across every message of a customer and
// across every data-plane replica; with random 96-bit GCM nonces that would approach the ~2^32 nonce-reuse
// bound (NIST SP 800-38D) on a high-volume account, with no way to coordinate nonces between pods. Instead it
// derives a fresh subkey per message via HKDF(DEK, info=message_id): each message gets its own key, so a nonce
// collision between two messages cannot break confidentiality. The customer, key and message ids are bound as
// additional data, so an envelope cannot be replayed under a different customer/key/message.
func SealBody(dek []byte, customerID, keyID, messageID uuid.UUID, plaintext []byte) ([]byte, error) {
	subkey, err := deriveSubkey(dek, messageID)
	if err != nil {
		return nil, err
	}
	gcm, err := newGCM(subkey)
	if err != nil {
		return nil, err
	}
	sealed, err := gcmSeal(gcm, plaintext, bodyAAD(EnvelopeVersion1, customerID, keyID, messageID))
	if err != nil {
		return nil, err
	}
	return append([]byte{EnvelopeVersion1}, sealed...), nil
}

// OpenBody reverses SealBody: it validates the version, derives the same per-message subkey and authenticates
// and decrypts under the same bound ids. Any tamper, wrong id or wrong DEK returns an error and no plaintext.
func OpenBody(dek []byte, customerID, keyID, messageID uuid.UUID, envelope []byte) ([]byte, error) {
	if len(envelope) < 1 || envelope[0] != EnvelopeVersion1 {
		return nil, ErrEnvelope
	}
	subkey, err := deriveSubkey(dek, messageID)
	if err != nil {
		return nil, err
	}
	gcm, err := newGCM(subkey)
	if err != nil {
		return nil, err
	}
	return gcmOpen(gcm, envelope[1:], bodyAAD(envelope[0], customerID, keyID, messageID))
}

// deriveSubkey derives the per-message AES-256 subkey from the DEK, binding the message id as HKDF info so each
// message keys independently. A non-32-byte DEK (ErrKeySize) or a nil message id (ErrEnvelope) is rejected: the
// whole nonce-bound mitigation rests on message_id being unique, so a zero id — which would collapse distinct
// messages onto one subkey — must never be accepted.
func deriveSubkey(dek []byte, messageID uuid.UUID) ([]byte, error) {
	if len(dek) != dekSize {
		return nil, ErrKeySize
	}
	if messageID == uuid.Nil {
		return nil, ErrEnvelope
	}
	return hkdf.Key(sha256.New, dek, nil, messageID.String(), dekSize)
}

// bodyAAD is the additional authenticated data binding a body ciphertext to its envelope version, customer, key
// and message, so a ciphertext is undecryptable outside that exact context (defence in depth against a
// cross-customer or cross-message swap, and against a cross-version downgrade).
func bodyAAD(version byte, customerID, keyID, messageID uuid.UUID) []byte {
	aad := make([]byte, 0, 49)
	aad = append(aad, version)
	aad = append(aad, customerID[:]...)
	aad = append(aad, keyID[:]...)
	aad = append(aad, messageID[:]...)
	return aad
}
