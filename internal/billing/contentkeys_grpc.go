package billing

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// ContentKeyStore is the durable content-key surface the server drives (control_plane.content_keys, §6.14).
// *postgres.ContentKeyRepo satisfies it; declared consumer-side. It persists only the KMS-wrapped data key —
// no plaintext key material passes through it.
type ContentKeyStore interface {
	GetActive(ctx context.Context, customerID uuid.UUID) (cp.ContentKey, error)
	CreateIfAbsent(ctx context.Context, customerID uuid.UUID, wrapped []byte, keyRef string) (cp.ContentKey, bool, error)
	Rotate(ctx context.Context, customerID uuid.UUID, wrapped []byte, keyRef string) (cp.ContentKey, error)
}

// ContentKeyServer serves the ContentKeys gRPC API from billing-svc, the sole holder of the KMS. It generates
// a data key (DEK) and seals it with the KMS *before* any database write (so a failed commit strands no
// state), then persists only the wrapped DEK. It never returns plaintext or wrapped key material — responses
// carry metadata only, upholding invariant (a): no key ever crosses a wire or a log in a usable form here.
//
// It is intentionally independent of the billing opt-in: content keys serve every customer, billing on or off.
type ContentKeyServer struct {
	pb.UnimplementedContentKeysServer
	kms   content.KMS
	store ContentKeyStore
}

// NewContentKeyServer wires the server to its KMS and store.
func NewContentKeyServer(kms content.KMS, store ContentKeyStore) *ContentKeyServer {
	return &ContentKeyServer{kms: kms, store: store}
}

// GetOrCreateContentKey returns the customer's active key, creating one only if none exists. The cheap read
// path (an active key already exists) does no crypto; only the create path generates and wraps a DEK.
func (s *ContentKeyServer) GetOrCreateContentKey(ctx context.Context, req *pb.GetOrCreateContentKeyRequest) (*pb.ContentKeyResponse, error) {
	customerID, err := uuid.Parse(req.GetCustomerId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	key, err := s.getOrCreateActive(ctx, customerID)
	if err != nil {
		return nil, toStatus(err)
	}
	return contentKeyResponse(key), nil
}

// GetContentEncryptionKey returns the active key id and its UNWRAPPED data key so the data plane can encrypt a
// body without the body reaching billing-svc. It is the only path that unseals the DEK: it get-or-creates the
// active key, then KMS-unwraps its sealed bytes. The plaintext DEK is returned but never logged or persisted.
func (s *ContentKeyServer) GetContentEncryptionKey(ctx context.Context, req *pb.GetContentEncryptionKeyRequest) (*pb.ContentEncryptionKeyResponse, error) {
	customerID, err := uuid.Parse(req.GetCustomerId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	key, err := s.getOrCreateActive(ctx, customerID)
	if err != nil {
		return nil, toStatus(err)
	}
	dek, err := s.kms.UnwrapDataKey(ctx, key.WrappedKey)
	if err != nil {
		// An unwrap failure is a KMS/key integrity fault, not a client error — opaque Internal, no key material.
		return nil, toStatus(err)
	}
	return &pb.ContentEncryptionKeyResponse{KeyId: key.ID.String(), Dek: dek}, nil
}

// getOrCreateActive returns the customer's active content key, creating one (fresh DEK sealed by the KMS) if
// none exists. The read path (an active key already exists) does no crypto.
func (s *ContentKeyServer) getOrCreateActive(ctx context.Context, customerID uuid.UUID) (cp.ContentKey, error) {
	switch key, err := s.store.GetActive(ctx, customerID); {
	case err == nil:
		return key, nil
	case !errors.Is(err, errs.ErrNotFound):
		return cp.ContentKey{}, err
	}
	wrapped, keyRef, err := s.newWrappedDataKey(ctx)
	if err != nil {
		return cp.ContentKey{}, err
	}
	key, _, err := s.store.CreateIfAbsent(ctx, customerID, wrapped, keyRef)
	if err != nil {
		return cp.ContentKey{}, err
	}
	return key, nil
}

// RotateContentKey makes a new active key and retires the previous one. The new key's DEK is sealed before the
// rotation transaction runs.
func (s *ContentKeyServer) RotateContentKey(ctx context.Context, req *pb.RotateContentKeyRequest) (*pb.ContentKeyResponse, error) {
	customerID, err := uuid.Parse(req.GetCustomerId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	wrapped, keyRef, err := s.newWrappedDataKey(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	key, err := s.store.Rotate(ctx, customerID, wrapped, keyRef)
	if err != nil {
		return nil, toStatus(err)
	}
	return contentKeyResponse(key), nil
}

// newWrappedDataKey generates a fresh DEK and seals it with the KMS, returning the wrapped bytes and the KMS
// key reference to persist. The plaintext DEK is never returned and does not escape this call.
func (s *ContentKeyServer) newWrappedDataKey(ctx context.Context) ([]byte, string, error) {
	dek, err := content.GenerateDataKey()
	if err != nil {
		return nil, "", err
	}
	wrapped, err := s.kms.WrapDataKey(ctx, dek)
	if err != nil {
		return nil, "", err
	}
	return wrapped, s.kms.KeyRef(), nil
}

// contentKeyResponse maps a domain key to its gRPC metadata — never wrapped_key or any plaintext.
func contentKeyResponse(k cp.ContentKey) *pb.ContentKeyResponse {
	return &pb.ContentKeyResponse{
		KeyId:      k.ID.String(),
		CustomerId: k.CustomerID.String(),
		KmsKeyRef:  k.KMSKeyRef,
		Status:     string(k.Status),
		CreatedAt:  k.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}
