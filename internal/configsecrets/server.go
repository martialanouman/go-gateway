// Package configsecrets seals and opens the control-plane secrets the gateway REPLAYS to a third party:
// the outbound SMSC bind password and the external billing provider's credentials. They cannot be
// hashed — an outbound bind puts its password in clear in the bind_transceiver PDU (SMPP v3.4 §4.1.1) —
// so their stored form has to be reversible. ADR-0016.
//
// It is stateless on purpose: no store, no customer, no key lifecycle. That is what separates it from
// internal/contentkeys, which shares the same KMS and the same binary but is none of those things.
package configsecrets

import (
	"bytes"
	"context"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	"github.com/martialanouman/go-gateway/internal/content"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/grpcerr"
)

// domainTag prefixes every secret before it is wrapped, and is required back on open.
//
// It exists because the KMS is SHARED with ContentKeys — one deployment, one master key (ADR-0011) — and
// LocalKMS binds only its own KeyRef as additional data. Every blob wrapped under that key therefore
// belongs to one undifferentiated space, and a content_keys.wrapped_key is such a blob: without this tag,
// Open would unwrap one and hand back a customer's plaintext DEK, with none of GetContentKey's guards —
// in particular not the destroyed check that makes crypto-shred final. The tag is inside the ciphertext,
// so it cannot be forged without the master key.
//
// It is exactly 32 bytes — the size of an AES-256 data key — and Seal refuses an empty secret, so what
// this service produces is always STRICTLY longer than a DEK. That closes the other direction: a caller
// cannot have a secret sealed here and plant the result in content_keys.wrapped_key to make a customer's
// content readable under a key it chose. contentkeys rejects the wrong length on the way out.
//
// Changing it invalidates every stored secret. It is versioned so a future format can be told apart
// rather than silently mis-parsed.
var domainTag = []byte("go-gateway/config-secret/v1/seal")

// Server implements the ConfigSecrets gRPC service over the KMS held by content-key-svc. It keeps no
// state of its own: a sealed secret is self-contained, and the only thing the caller stores beside it is
// the key reference saying which master key sealed it.
type Server struct {
	pb.UnimplementedConfigSecretsServer
	kms content.KMS
}

// NewServer builds the service over kms — the same KMS instance content-key-svc gives ContentKeys, so a
// deployment has exactly one master key whatever it seals.
func NewServer(kms content.KMS) *Server { return &Server{kms: kms} }

// Seal encrypts a secret under the master key and returns the bytes to persist plus the reference of the
// key that sealed them. The plaintext is neither logged nor retained.
func (s *Server) Seal(ctx context.Context, req *pb.SealRequest) (*pb.SealResponse, error) {
	// An empty secret is a caller mistake, not something to seal: the connector would bind with an empty
	// password and the failure would surface at the far SMSC, far from its cause.
	if len(req.GetPlaintext()) == 0 {
		return nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	// WrapDataKey straight on the secret's bytes, with no data key of its own. The KMS is a wrap oracle
	// over arbitrary bytes, and an envelope would cost a 32-byte DEK to protect a password of eight: the
	// GCM nonce bound (~2^32 seals per key) that justifies the per-message HKDF of content.SealBody is out
	// of reach for configuration writes. The nonce is drawn per call, so two connectors sharing a password
	// do not share a ciphertext.
	sealed, err := s.kms.WrapDataKey(ctx, append(slices.Clone(domainTag), req.GetPlaintext()...))
	if err != nil {
		return nil, grpcerr.Status(err)
	}
	return &pb.SealResponse{Sealed: sealed, KmsKeyRef: s.kms.KeyRef()}, nil
}

// Open reverses Seal. A tampered ciphertext, or one sealed under a different master key, fails cleanly
// and returns no plaintext.
func (s *Server) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	tagged, err := s.kms.UnwrapDataKey(ctx, req.GetSealed())
	if err != nil {
		// Tampering, truncation or the wrong master key all land here. It is a key-integrity fault, not a
		// client error: opaque Internal, and no fragment of the secret in the message.
		return nil, grpcerr.Status(err)
	}
	// Authentic under the master key, but sealed for something else — a content key, most plausibly. It
	// opens no further here, and the error says nothing about what it turned out to be.
	if !bytes.HasPrefix(tagged, domainTag) {
		return nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	return &pb.OpenResponse{Plaintext: tagged[len(domainTag):]}, nil
}
