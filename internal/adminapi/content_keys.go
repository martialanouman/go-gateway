package adminapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// ContentKeyView is the metadata of a customer's content key as returned to the Admin API. It never carries
// key material — only the id, the KMS reference, the lifecycle status and the creation time.
type ContentKeyView struct {
	ID         string
	CustomerID string
	KMSKeyRef  string
	Status     string
	CreatedAt  string
}

// ContentKeyRotator rotates a customer's content key. The Admin API does not touch the KMS or the database
// directly: content keys are hosted by billing-svc (the sole KMS holder), so this delegates over gRPC
// (GRPCContentKeyRotator). Declared consumer-side.
type ContentKeyRotator interface {
	Rotate(ctx context.Context, customerID uuid.UUID) (ContentKeyView, error)
}

type contentKeyHandlers struct {
	rotator ContentKeyRotator
}

func registerContentKeys(api huma.API, rotator ContentKeyRotator) {
	h := &contentKeyHandlers{rotator: rotator}
	register(api, huma.Operation{
		OperationID: "rotate-content-key", Method: http.MethodPost, Path: "/admin/customers/{id}/content/rotate-key",
		Summary: "Rotate a customer's content encryption key", Tags: []string{"Content & RGPD"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusServiceUnavailable},
	}, h.rotate)
}

// contentKeyDTO conforms to api/openapi-admin.yaml ContentKey. wrapped_key is deliberately absent: the
// wrapped DEK never leaves billing-svc.
type contentKeyDTO struct {
	ID          string  `json:"id" format:"uuid"`
	CustomerID  string  `json:"customer_id" format:"uuid"`
	KMSKeyRef   string  `json:"kms_key_ref"`
	Status      string  `json:"status" enum:"active,retired,destroyed"`
	CreatedAt   string  `json:"created_at" format:"date-time"`
	RetiredAt   *string `json:"retired_at,omitempty" nullable:"true" format:"date-time"`
	DestroyedAt *string `json:"destroyed_at,omitempty" nullable:"true" format:"date-time"`
}

type rotateContentKeyOutput struct{ Body contentKeyDTO }

func (h *contentKeyHandlers) rotate(ctx context.Context, in *customerIDPathInput) (*rotateContentKeyOutput, error) {
	customerID, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	key, err := h.rotator.Rotate(ctx, customerID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &rotateContentKeyOutput{Body: contentKeyDTO{
		ID: key.ID, CustomerID: key.CustomerID, KMSKeyRef: key.KMSKeyRef, Status: key.Status, CreatedAt: key.CreatedAt,
	}}, nil
}
