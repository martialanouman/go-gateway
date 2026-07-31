package content_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/martialanouman/go-gateway/internal/content"
)

// TestGenerateDataKeyIsAES256: a fresh DEK is 32 bytes (AES-256) and two DEKs differ.
func TestGenerateDataKeyIsAES256(t *testing.T) {
	a, err := content.GenerateDataKey()
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	if len(a) != 32 {
		t.Fatalf("DEK length = %d, want 32", len(a))
	}
	b, err := content.GenerateDataKey()
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two DEKs are identical — not random")
	}
}

// TestSealOpenContentRoundTrip: encrypting then decrypting a blob with a DEK returns the original, and the
// ciphertext neither equals nor contains the plaintext.
func TestSealOpenContentRoundTrip(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	plaintext := []byte("hello, this is a message body that must never leak in the clear")
	aad := []byte("message-id-123")

	ct, err := content.SealContent(dek, plaintext, aad)
	if err != nil {
		t.Fatalf("SealContent: %v", err)
	}
	if bytes.Equal(ct, plaintext) || bytes.Contains(ct, plaintext) {
		t.Fatal("ciphertext exposes the plaintext")
	}
	got, err := content.OpenContent(dek, ct, aad)
	if err != nil {
		t.Fatalf("OpenContent: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip = %q, want %q", got, plaintext)
	}
}

// TestSealContentIsNonDeterministic: sealing the same plaintext twice yields different ciphertexts (random
// nonce), so identical bodies are not linkable by their ciphertext.
func TestSealContentIsNonDeterministic(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	pt := []byte("same body")
	c1, _ := content.SealContent(dek, pt, nil)
	c2, _ := content.SealContent(dek, pt, nil)
	if bytes.Equal(c1, c2) {
		t.Fatal("two seals of the same plaintext are identical — nonce not random")
	}
}

// TestOpenContentTamperFails: a flipped byte or a mismatched AAD fails authentication cleanly, and no
// plaintext is returned.
func TestOpenContentTamperFails(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	pt := []byte("authenticated payload")
	aad := []byte("bind")
	ct, _ := content.SealContent(dek, pt, aad)

	tampered := bytes.Clone(ct)
	tampered[len(tampered)-1] ^= 0xFF
	if got, err := content.OpenContent(dek, tampered, aad); err == nil {
		t.Fatalf("tampered ciphertext opened to %q, want error", got)
	}
	// Flipping a byte inside the nonce prefix must also fail authentication.
	nonceFlip := bytes.Clone(ct)
	nonceFlip[0] ^= 0xFF
	if got, err := content.OpenContent(dek, nonceFlip, aad); err == nil {
		t.Fatalf("nonce-corrupted ciphertext opened to %q, want error", got)
	}
	if got, err := content.OpenContent(dek, ct, []byte("wrong-aad")); err == nil {
		t.Fatalf("mismatched AAD opened to %q, want error", got)
	}
	// The right key + right AAD still works (proves the tamper test isn't a false positive).
	if _, err := content.OpenContent(dek, ct, aad); err != nil {
		t.Fatalf("clean open failed: %v", err)
	}
}

// TestContentKeySizeRejected: a DEK that is not 32 bytes is rejected, not silently truncated.
func TestContentKeySizeRejected(t *testing.T) {
	if _, err := content.SealContent([]byte("short"), []byte("x"), nil); err == nil {
		t.Fatal("SealContent accepted a 5-byte key, want error")
	}
	if _, err := content.OpenContent([]byte("short"), []byte("x"), nil); err == nil {
		t.Fatal("OpenContent accepted a 5-byte key, want error")
	}
}

// TestLocalKMSWrapUnwrapRoundTrip: the local KMS seals a DEK (envelope) and unwraps it back to the original,
// and it reports a stable key reference to persist alongside the wrapped key (content_keys.kms_key_ref).
func TestLocalKMSWrapUnwrapRoundTrip(t *testing.T) {
	ctx := context.Background()
	kms := content.NewDevKMS()
	if kms.KeyRef() == "" {
		t.Fatal("KeyRef is empty")
	}
	dek, _ := content.GenerateDataKey()

	wrapped, err := kms.WrapDataKey(ctx, dek)
	if err != nil {
		t.Fatalf("WrapDataKey: %v", err)
	}
	if bytes.Equal(wrapped, dek) || bytes.Contains(wrapped, dek) {
		t.Fatal("wrapped key exposes the plaintext DEK")
	}
	got, err := kms.UnwrapDataKey(ctx, wrapped)
	if err != nil {
		t.Fatalf("UnwrapDataKey: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("unwrapped DEK differs from the original")
	}
}

// TestLocalKMSUnwrapTamperFails: an altered wrapped key fails to unwrap cleanly.
func TestLocalKMSUnwrapTamperFails(t *testing.T) {
	ctx := context.Background()
	kms := content.NewDevKMS()
	dek, _ := content.GenerateDataKey()
	wrapped, _ := kms.WrapDataKey(ctx, dek)

	tampered := bytes.Clone(wrapped)
	tampered[0] ^= 0xFF
	if got, err := kms.UnwrapDataKey(ctx, tampered); err == nil {
		t.Fatalf("tampered wrapped key unwrapped to %x, want error", got)
	}
}

// TestLocalKMSDistinctMastersDoNotUnwrap: a DEK wrapped under one master key cannot be unwrapped under a
// different master (isolation — a stolen wrapped_key is useless without the KEK).
func TestLocalKMSDistinctMastersDoNotUnwrap(t *testing.T) {
	ctx := context.Background()
	master1 := mustMaster(t)
	master2 := mustMaster(t)
	kms1, err := content.NewLocalKMS(master1, "kek/1")
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}
	kms2, err := content.NewLocalKMS(master2, "kek/2")
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}
	dek, _ := content.GenerateDataKey()
	wrapped, _ := kms1.WrapDataKey(ctx, dek)
	if _, err := kms2.UnwrapDataKey(ctx, wrapped); err == nil {
		t.Fatal("kms2 unwrapped a DEK sealed by kms1 — masters are not isolated")
	}
}

// TestLocalKMSSameMasterDifferentKeyRefIsolated: the keyRef bound as AAD is a real domain-separation guard —
// two KMS instances over the SAME master key but different keyRefs cannot unwrap each other's keys.
func TestLocalKMSSameMasterDifferentKeyRefIsolated(t *testing.T) {
	ctx := context.Background()
	master := mustMaster(t)
	a, err := content.NewLocalKMS(master, "kek/a")
	if err != nil {
		t.Fatalf("NewLocalKMS a: %v", err)
	}
	b, err := content.NewLocalKMS(master, "kek/b")
	if err != nil {
		t.Fatalf("NewLocalKMS b: %v", err)
	}
	dek, _ := content.GenerateDataKey()
	wrapped, _ := a.WrapDataKey(ctx, dek)
	if _, err := b.UnwrapDataKey(ctx, wrapped); err == nil {
		t.Fatal("kms b unwrapped a key sealed by kms a under the same master — keyRef guard is a no-op")
	}
	// Same keyRef over the same master DOES unwrap (proves the isolation is due to keyRef, not the master).
	a2, _ := content.NewLocalKMS(master, "kek/a")
	if _, err := a2.UnwrapDataKey(ctx, wrapped); err != nil {
		t.Fatalf("same master + same keyRef failed to unwrap: %v", err)
	}
}

// TestNewLocalKMSRejectsBadMaster: a master key that is not 32 bytes is rejected.
func TestNewLocalKMSRejectsBadMaster(t *testing.T) {
	if _, err := content.NewLocalKMS([]byte("too-short"), "kek"); err == nil {
		t.Fatal("NewLocalKMS accepted a short master key, want error")
	}
}

// TestNewLocalKMSRejectsEmptyKeyRef: an empty key reference would bind empty AAD and silently disable the
// domain-separation guard, so it is rejected.
func TestNewLocalKMSRejectsEmptyKeyRef(t *testing.T) {
	if _, err := content.NewLocalKMS(mustMaster(t), ""); err == nil {
		t.Fatal("NewLocalKMS accepted an empty keyRef, want error")
	}
}

// TestOpenContentShortAndNilInputs: a blob shorter than the nonce fails cleanly (no panic), and an
// empty-plaintext seal round-trips.
func TestOpenContentShortAndNilInputs(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	if _, err := content.OpenContent(dek, []byte{0, 1, 2}, nil); err == nil {
		t.Fatal("OpenContent accepted a blob shorter than the nonce, want error")
	}
	ct, err := content.SealContent(dek, nil, nil)
	if err != nil {
		t.Fatalf("SealContent(nil plaintext): %v", err)
	}
	got, err := content.OpenContent(dek, ct, nil)
	if err != nil {
		t.Fatalf("OpenContent of empty-plaintext seal: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty-plaintext round-trip = %q, want empty", got)
	}
}

func mustMaster(t *testing.T) []byte {
	t.Helper()
	m, err := content.GenerateDataKey() // 32 random bytes doubles as a dev master key
	if err != nil {
		t.Fatalf("master: %v", err)
	}
	return m
}
