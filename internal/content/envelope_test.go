package content_test

import (
	"bytes"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/content"
)

// TestSealOpenBodyRoundTrip: a body sealed for a (customer, key, message) opens back to the original under the
// same DEK and identifiers, and the envelope never exposes the plaintext.
func TestSealOpenBodyRoundTrip(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	cust, key, msg := uuid.New(), uuid.New(), uuid.New()
	plaintext := []byte("the message body that must be stored only encrypted")

	env, err := content.SealBody(dek, cust, key, msg, plaintext)
	if err != nil {
		t.Fatalf("SealBody: %v", err)
	}
	if bytes.Contains(env, plaintext) {
		t.Fatal("envelope exposes the plaintext")
	}
	if env[0] != content.EnvelopeVersion1 {
		t.Fatalf("envelope version byte = %d, want %d", env[0], content.EnvelopeVersion1)
	}
	got, err := content.OpenBody(dek, cust, key, msg, env)
	if err != nil {
		t.Fatalf("OpenBody: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip = %q, want %q", got, plaintext)
	}
}

// TestSealBodyPerMessageSubkeyDodgesNonceReuse: sealing two DIFFERENT messages under the SAME DEK derives
// distinct per-message subkeys, so a nonce collision between them cannot break confidentiality — the crux of
// the GCM nonce-bound mitigation. A body sealed for one message must not open under another message id even
// with the same DEK.
func TestSealBodyPerMessageSubkeyDodgesNonceReuse(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	cust, key := uuid.New(), uuid.New()
	msgA, msgB := uuid.New(), uuid.New()
	pt := []byte("same plaintext, two messages")

	envA, _ := content.SealBody(dek, cust, key, msgA, pt)
	envB, _ := content.SealBody(dek, cust, key, msgB, pt)
	if bytes.Equal(envA, envB) {
		t.Fatal("two messages sealed identically — per-message subkey not applied")
	}
	// Opening A under B's message id must fail (subkey + AAD are message-bound).
	if _, err := content.OpenBody(dek, cust, key, msgB, envA); err == nil {
		t.Fatal("envelope for message A opened under message B — not message-bound")
	}
}

// TestOpenBodyRejectsWrongBinding: a mismatch in ANY bound identifier (customer, key, message) or a tampered
// byte fails authentication cleanly (ErrDecrypt), never a wrong plaintext.
func TestOpenBodyRejectsWrongBinding(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	cust, key, msg := uuid.New(), uuid.New(), uuid.New()
	env, _ := content.SealBody(dek, cust, key, msg, []byte("bound payload"))

	cases := map[string][3]uuid.UUID{
		"wrong customer": {uuid.New(), key, msg},
		"wrong key":      {cust, uuid.New(), msg},
		"wrong message":  {cust, key, uuid.New()},
	}
	for name, ids := range cases {
		if _, err := content.OpenBody(dek, ids[0], ids[1], ids[2], env); err == nil {
			t.Errorf("%s: opened despite mismatched binding, want error", name)
		}
	}
	// Wrong DEK.
	other, _ := content.GenerateDataKey()
	if _, err := content.OpenBody(other, cust, key, msg, env); err == nil {
		t.Error("opened under a different DEK, want error")
	}
	// Tampered ciphertext and tampered nonce.
	for _, i := range []int{1, len(env) - 1} { // i=1 is inside the nonce, last is the tag/ct
		bad := bytes.Clone(env)
		bad[i] ^= 0xFF
		if _, err := content.OpenBody(dek, cust, key, msg, bad); err == nil {
			t.Errorf("tampered byte %d: opened, want error", i)
		}
	}
}

// TestOpenBodyRejectsShortOrBadVersion: a truncated envelope or an unknown version byte fails cleanly.
func TestOpenBodyRejectsShortOrBadVersion(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	cust, key, msg := uuid.New(), uuid.New(), uuid.New()
	if _, err := content.OpenBody(dek, cust, key, msg, []byte{content.EnvelopeVersion1, 0, 1}); err == nil {
		t.Error("short envelope opened, want error")
	}
	env, _ := content.SealBody(dek, cust, key, msg, []byte("x"))
	env[0] = 0xEE // unknown version
	if _, err := content.OpenBody(dek, cust, key, msg, env); err == nil {
		t.Error("unknown version opened, want error")
	}
}

// TestSealBodyRejectsNilMessageID: a zero message id would collapse distinct messages onto one subkey — it
// must be rejected, on seal and on open.
func TestSealBodyRejectsNilMessageID(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	cust, key := uuid.New(), uuid.New()
	if _, err := content.SealBody(dek, cust, key, uuid.Nil, []byte("x")); err == nil {
		t.Error("SealBody accepted a nil message id, want error")
	}
	if _, err := content.OpenBody(dek, cust, key, uuid.Nil, []byte{content.EnvelopeVersion1, 0, 0}); err == nil {
		t.Error("OpenBody accepted a nil message id, want error")
	}
}
