package configsecrets_test

import (
	"bytes"
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
	srv := configsecrets.NewServer(content.NewDevKMS())
	secret := []byte("s3cr3t!") // an SMPP bind password: <= 8 bytes, not 32

	sealed, err := srv.Seal(t.Context(), &pb.SealRequest{Plaintext: secret})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if bytes.Contains(sealed.GetSealed(), secret) {
		t.Errorf("the sealed bytes contain the plaintext, so nothing was encrypted")
	}
	if sealed.GetKmsKeyRef() == "" {
		t.Error("kms_key_ref is empty: nothing says which master key sealed this row")
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

// An empty secret is a caller mistake, not something to seal: a connector with no password would bind with
// an empty one and the failure would surface at the SMSC, far from its cause.
func TestSealRefusesAnEmptySecret(t *testing.T) {
	_, err := configsecrets.NewServer(content.NewDevKMS()).Seal(t.Context(), &pb.SealRequest{})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("status code = %s, want %s", code, codes.InvalidArgument)
	}
}
