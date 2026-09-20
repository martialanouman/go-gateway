// Package configsecrets seals and opens the control-plane secrets the gateway REPLAYS to a third party:
// the outbound SMSC bind password and the external billing provider's credentials. They cannot be
// hashed — an outbound bind puts its password in clear in the bind_transceiver PDU (SMPP v3.4 §4.1.1) —
// so their stored form has to be reversible. ADR-0016.
//
// It is stateless on purpose: no store, no customer, no key lifecycle. That is what separates it from
// internal/contentkeys, which shares the same KMS and the same binary but is none of those things.
package configsecrets

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	"github.com/martialanouman/go-gateway/internal/content"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/grpcerr"
)

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
	sealed, err := s.kms.WrapDataKey(ctx, req.GetPlaintext())
	if err != nil {
		return nil, grpcerr.Status(err)
	}
	return &pb.SealResponse{Sealed: sealed, KmsKeyRef: s.kms.KeyRef()}, nil
}

// Open reverses Seal. A tampered ciphertext, or one sealed under a different master key, fails cleanly
// and returns no plaintext.
func (s *Server) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	plaintext, err := s.kms.UnwrapDataKey(ctx, req.GetSealed())
	if err != nil {
		// Tampering, truncation or the wrong master key all land here. It is a key-integrity fault, not a
		// client error: opaque Internal, and no fragment of the secret in the message.
		return nil, grpcerr.Status(err)
	}
	return &pb.OpenResponse{Plaintext: plaintext}, nil
}
