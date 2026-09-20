package configsecrets_test

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/configsecrets"
	"github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	"github.com/martialanouman/go-gateway/internal/content"
)

// The whole point of step-295: the stored form of a REPLAYED secret must come back usable. A hash cannot,
// which is why smsc_connectors.password_hash could never serve the outbound bind it existed for.
func TestSealedSecretComesBackByteForByte(t *testing.T) {
	kms := content.NewDevKMS()
	srv := configsecrets.NewServer(kms)
	secret := []byte("s3cr3t!") // an SMPP bind password: <= 8 bytes, not 32

	sealed, err := srv.Seal(t.Context(), &pb.SealRequest{Plaintext: secret})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Contains(sealed.GetSealed(), secret) {
		t.Errorf("the sealed bytes contain the plaintext, so nothing was encrypted")
	}
	// Compared to the KMS, not merely non-empty: a hard-coded constant would satisfy "not empty" and lie
	// about which master key the row belongs to, which is the one thing this field is for.
	if got, want := sealed.GetKmsKeyRef(), kms.KeyRef(); got != want {
		t.Errorf("kms_key_ref = %q, want %q — the row does not name the key that sealed it", got, want)
	}

	opened, err := srv.Open(t.Context(), &pb.OpenRequest{Sealed: sealed.GetSealed()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(opened.GetPlaintext(), secret) {
		t.Errorf("Open returned %q, want %q", opened.GetPlaintext(), secret)
	}
}

// Two connectors sharing a password must not share a ciphertext: equal sealed bytes would turn the column
// into an oracle telling an operator which connectors have the same secret.
func TestSealingTheSameSecretTwiceGivesDifferentBytes(t *testing.T) {
	srv := configsecrets.NewServer(content.NewDevKMS())
	secret := []byte("same-password")

	first, err := srv.Seal(t.Context(), &pb.SealRequest{Plaintext: secret})
	if err != nil {
		t.Fatalf("Seal (first): %v", err)
	}
	second, err := srv.Seal(t.Context(), &pb.SealRequest{Plaintext: secret})
	if err != nil {
		t.Fatalf("Seal (second): %v", err)
	}
	if bytes.Equal(first.GetSealed(), second.GetSealed()) {
		t.Error("sealing the same secret twice produced identical bytes, so the nonce is not per-call")
	}
}

// A sealed value is per-master-key. Opening one under a different KEK must fail cleanly and hand back no
// key material — the caller gets an error, never a half-decrypted secret.
func TestOpeningUnderAnotherMasterKeyFailsAndReturnsNothing(t *testing.T) {
	sealed, err := configsecrets.NewServer(content.NewDevKMS()).Seal(t.Context(), &pb.SealRequest{Plaintext: []byte("s3cr3t!")})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opened, err := configsecrets.NewServer(content.NewDevKMS()).Open(t.Context(), &pb.OpenRequest{Sealed: sealed.GetSealed()})
	if err == nil {
		t.Fatalf("Open accepted bytes sealed under another master key, returned %q", opened.GetPlaintext())
	}
	if opened != nil {
		t.Errorf("Open returned a response alongside its error: %v", opened)
	}
	if code := status.Code(err); code != codes.Internal {
		t.Errorf("status code = %s, want %s: a bad ciphertext is a key-integrity fault, not a client error", code, codes.Internal)
	}
}

// Open must not be a decryption oracle over everything the SHARED master key ever sealed.
//
// content-key-svc hands ConfigSecrets the same content.KMS it hands ContentKeys (one deployment, one
// master key), and LocalKMS binds only its own KeyRef as additional data — so every blob wrapped under it
// belongs to one undifferentiated space. A content_keys.wrapped_key is such a blob. Without a domain of
// its own, Open would unwrap it and hand back a customer's plaintext DEK: no customer_id, no key id, and
// none of GetContentKey's guards — in particular not the destroyed check that makes crypto-shred final.
// The caller needs no privilege it does not already have: admin-api-svc holds this port AND a Postgres
// pool, so it can read the wrapped key itself.
func TestOpenRefusesAKeyWrappedForAnotherPurpose(t *testing.T) {
	kms := content.NewDevKMS()
	srv := configsecrets.NewServer(kms)

	// Exactly what contentkeys.newWrappedDataKey persists in content_keys.wrapped_key.
	dek, err := content.GenerateDataKey()
	if err != nil {
		t.Fatalf("GenerateDataKey: %v", err)
	}
	wrappedDEK, err := kms.WrapDataKey(t.Context(), dek)
	if err != nil {
		t.Fatalf("WrapDataKey: %v", err)
	}

	opened, err := srv.Open(t.Context(), &pb.OpenRequest{Sealed: wrappedDEK})
	if err == nil {
		t.Fatalf("Open unwrapped a content key: it returned %d bytes of key material", len(opened.GetPlaintext()))
	}
	if opened != nil {
		t.Errorf("Open returned a response alongside its error: %v", opened)
	}
}

// An empty secret is a caller mistake, not something to seal: a connector with no password would bind with
// an empty one and the failure would surface at the SMSC, far from its cause.
func TestSealRefusesAnEmptySecret(t *testing.T) {
	_, err := configsecrets.NewServer(content.NewDevKMS()).Seal(t.Context(), &pb.SealRequest{})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("status code = %s, want %s", code, codes.InvalidArgument)
	}
}

// Nothing Seal produces may pass for an AES-256 data key once unwrapped, whatever the secret's length.
// That is what stops the other direction of the shared-KMS problem: a caller who can have a secret sealed
// and write content_keys.wrapped_key would otherwise plant key material it chose.
//
// The property is the domain tag being exactly a data key long, plus Seal refusing an empty secret — so a
// 16-byte tag with a 16-byte password would land on 32 and reopen the hole. This test is what says so.
func TestNothingSealedCanPassForADataKey(t *testing.T) {
	kms := content.NewDevKMS()
	srv := configsecrets.NewServer(kms)

	for _, n := range []int{1, 8, 15, 16, 17, 31, 32, 64} {
		sealed, err := srv.Seal(t.Context(), &pb.SealRequest{Plaintext: bytes.Repeat([]byte("a"), n)})
		if err != nil {
			t.Fatalf("Seal(%d bytes): %v", n, err)
		}
		inner, err := kms.UnwrapDataKey(t.Context(), sealed.GetSealed())
		if err != nil {
			t.Fatalf("UnwrapDataKey(%d bytes): %v", n, err)
		}
		if len(inner) == 32 {
			t.Errorf("a %d-byte secret seals to exactly a data key's length; contentkeys would accept it as one", n)
		}
	}
}

// A KMS that answers with no key reference must not produce a storable secret. The column is NOT NULL, but
// ” is not NULL: an empty reference passes the constraint and leaves a row that opens today and that a
// future KEK rotation cannot place. The only thing standing in the way otherwise is NewLocalKMS refusing an
// empty keyRef — a property of the DEVELOPMENT KMS, not of the content.KMS contract that a real AWS/GCP
// provider will implement.
func TestSealRefusesToProduceASecretWithNoKeyReference(t *testing.T) {
	_, err := configsecrets.NewServer(keyRefLessKMS{}).Seal(t.Context(), &pb.SealRequest{Plaintext: []byte("s3cr3t")})
	if err == nil {
		t.Fatal("Seal produced a secret whose key reference is empty")
	}
	if code := status.Code(err); code != codes.Internal {
		t.Errorf("status code = %s, want %s", code, codes.Internal)
	}
}

// keyRefLessKMS wraps normally but names no key — what a provider implementation may legitimately do, and
// what LocalKMS happens never to do.
type keyRefLessKMS struct{ content.KMS }

func (keyRefLessKMS) KeyRef() string { return "" }

func (keyRefLessKMS) WrapDataKey(_ context.Context, b []byte) ([]byte, error) {
	return append([]byte("wrapped:"), b...), nil
}
