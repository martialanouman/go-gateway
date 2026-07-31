package adminapi

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// GRPCContentKeyRotator adapts the ContentKeys gRPC client (billing-svc, the KMS holder) to the
// ContentKeyRotator the Admin handler uses. It is the first admin→billing gRPC dependency: content keys are
// hosted by billing-svc, so the Admin API delegates the rotation rather than touching the KMS itself.
type GRPCContentKeyRotator struct {
	client pb.ContentKeysClient
}

// NewGRPCContentKeyRotator returns a ContentKeyRotator backed by the ContentKeys client.
func NewGRPCContentKeyRotator(client pb.ContentKeysClient) *GRPCContentKeyRotator {
	return &GRPCContentKeyRotator{client: client}
}

// Rotate delegates to billing-svc and maps its gRPC status back onto the shared error model.
func (r *GRPCContentKeyRotator) Rotate(ctx context.Context, customerID uuid.UUID) (ContentKeyView, error) {
	resp, err := r.client.RotateContentKey(ctx, &pb.RotateContentKeyRequest{CustomerId: customerID.String()})
	if err != nil {
		return ContentKeyView{}, mapContentKeyErr(err)
	}
	return ContentKeyView{
		ID:         resp.GetKeyId(),
		CustomerID: resp.GetCustomerId(),
		KMSKeyRef:  resp.GetKmsKeyRef(),
		Status:     resp.GetStatus(),
		CreatedAt:  resp.GetCreatedAt(),
	}, nil
}

// ContentKeyReader fetches the plaintext data key for a SPECIFIC content key id (the guarded decrypt path,
// step-163): a CDR body may be sealed under a since-rotated key, so the reader asks by the row's key id.
// destroyed=true means the key was crypto-shredded → the content is permanently unreadable. Declared
// consumer-side; *GRPCContentKeyReader satisfies it.
type ContentKeyReader interface {
	Fetch(ctx context.Context, keyID uuid.UUID) (dek []byte, destroyed bool, err error)
}

// GRPCContentKeyReader adapts the ContentKeys gRPC client to the ContentKeyReader the content-read handler
// uses: it fetches the plaintext data key for a SPECIFIC content key id (the id a CDR body was sealed under),
// so a body encrypted under a since-rotated key still decrypts. A destroyed (crypto-shredded) key reports
// destroyed=true and no key material.
type GRPCContentKeyReader struct {
	client pb.ContentKeysClient
}

// NewGRPCContentKeyReader returns a ContentKeyReader backed by the ContentKeys client.
func NewGRPCContentKeyReader(client pb.ContentKeysClient) *GRPCContentKeyReader {
	return &GRPCContentKeyReader{client: client}
}

// Fetch returns the data key for keyID (destroyed=true when the key was crypto-shredded).
func (r *GRPCContentKeyReader) Fetch(ctx context.Context, keyID uuid.UUID) (dek []byte, destroyed bool, err error) {
	resp, ferr := r.client.GetContentKey(ctx, &pb.GetContentKeyRequest{KeyId: keyID.String()})
	if ferr != nil {
		return nil, false, mapContentKeyErr(ferr)
	}
	return resp.GetDek(), resp.GetDestroyed(), nil
}

// ContentKeyEraser crypto-shreds all of a customer's content keys (erase-customer-content, step-164) via
// billing-svc. Returns how many keys were destroyed. Declared consumer-side; *GRPCContentKeyEraser satisfies it.
type ContentKeyEraser interface {
	Erase(ctx context.Context, customerID uuid.UUID) (destroyedCount int, err error)
}

// GRPCContentKeyEraser adapts the ContentKeys gRPC client to the ContentKeyEraser the erase handler uses.
type GRPCContentKeyEraser struct {
	client pb.ContentKeysClient
}

// NewGRPCContentKeyEraser returns a ContentKeyEraser backed by the ContentKeys client.
func NewGRPCContentKeyEraser(client pb.ContentKeysClient) *GRPCContentKeyEraser {
	return &GRPCContentKeyEraser{client: client}
}

// Erase crypto-shreds the customer's content keys and reports the count.
func (e *GRPCContentKeyEraser) Erase(ctx context.Context, customerID uuid.UUID) (int, error) {
	resp, err := e.client.DestroyContentKeys(ctx, &pb.DestroyContentKeysRequest{CustomerId: customerID.String()})
	if err != nil {
		return 0, mapContentKeyErr(err)
	}
	return int(resp.GetDestroyedCount()), nil
}

// mapContentKeyErr translates a billing-svc gRPC status into the platform error the Admin error model turns
// into an HTTP status: an unknown customer is 404; a bad request 422; a conflict 409; billing-svc unreachable
// or slow is a transient 503 (retry once it recovers); anything else a 500.
// It is shared by the rotate and the read paths, so its messages are operation-neutral.
func mapContentKeyErr(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("content key: %w", errs.ErrNotFound)
	case codes.InvalidArgument:
		return fmt.Errorf("content key: %w", errs.ErrValidation)
	case codes.Aborted:
		return fmt.Errorf("content key: %w", errs.ErrConflict)
	case codes.Unavailable, codes.DeadlineExceeded:
		return fmt.Errorf("content-key service unavailable: %w", errs.ErrServiceUnavailable)
	default:
		return fmt.Errorf("content key: %w", errs.ErrInternal)
	}
}
