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

// mapContentKeyErr translates a billing-svc gRPC status into the platform error the Admin error model turns
// into an HTTP status: an unknown customer is 404; a bad request 422; a conflict 409; billing-svc unreachable
// or slow is a transient 503 (retry once it recovers); anything else a 500.
func mapContentKeyErr(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("rotate content key: %w", errs.ErrNotFound)
	case codes.InvalidArgument:
		return fmt.Errorf("rotate content key: %w", errs.ErrValidation)
	case codes.Aborted:
		return fmt.Errorf("rotate content key: %w", errs.ErrConflict)
	case codes.Unavailable, codes.DeadlineExceeded:
		return fmt.Errorf("content-key service unavailable: %w", errs.ErrServiceUnavailable)
	default:
		return fmt.Errorf("rotate content key: %w", errs.ErrInternal)
	}
}
