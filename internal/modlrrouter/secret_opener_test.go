package modlrrouter_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

type fakeConfigSecretsClient struct {
	pb.ConfigSecretsClient
	plaintext []byte
	err       error
	sealedGot []byte
}

func (f *fakeConfigSecretsClient) Open(_ context.Context, in *pb.OpenRequest, _ ...grpc.CallOption) (*pb.OpenResponse, error) {
	f.sealedGot = in.GetSealed()
	if f.err != nil {
		return nil, f.err
	}
	return &pb.OpenResponse{Plaintext: f.plaintext}, nil
}

func TestGRPCSecretOpenerReturnsThePlaintext(t *testing.T) {
	client := &fakeConfigSecretsClient{plaintext: []byte("the-signing-key")}
	opener := modlrrouter.NewGRPCSecretOpener(client)

	got, err := opener.Open(context.Background(), cp.SealedSecret{Sealed: []byte("ciphertext"), KMSKeyRef: "kek"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(got) != "the-signing-key" {
		t.Errorf("plaintext = %q, want the signing key", got)
	}
	if string(client.sealedGot) != "ciphertext" {
		t.Errorf("sent %q to the key service, want the stored ciphertext", client.sealedGot)
	}
}

func TestGRPCSecretOpenerMarksOnlyTheReachabilityFailuresTransient(t *testing.T) {
	for _, tc := range []struct {
		name      string
		code      codes.Code
		transient bool
	}{
		{"service down", codes.Unavailable, true},
		{"deadline", codes.DeadlineExceeded, true},
		{"cancelled", codes.Canceled, true},
		{"wrong domain tag", codes.InvalidArgument, false},
		{"caller not allowed", codes.PermissionDenied, false},
		{"unwrap failed", codes.Internal, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opener := modlrrouter.NewGRPCSecretOpener(&fakeConfigSecretsClient{err: status.Error(tc.code, "nope")})
			_, err := opener.Open(context.Background(), cp.SealedSecret{Sealed: []byte("c")})
			if err == nil {
				t.Fatal("Open returned no error")
			}
			if got := errors.Is(err, errs.ErrServiceUnavailable); got != tc.transient {
				t.Errorf("transient = %v, want %v (err = %v)", got, tc.transient, err)
			}
		})
	}
}

func sealedFor(plaintext string) cp.SealedSecret {
	return cp.SealedSecret{Sealed: []byte("sealed:" + plaintext), KMSKeyRef: "local/test-kek"}
}

type sealedForOpener struct{}

func (sealedForOpener) Open(_ context.Context, sealed cp.SealedSecret) ([]byte, error) {
	return bytes.TrimPrefix(sealed.Sealed, []byte("sealed:")), nil
}
