package adminapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

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
// directly: content keys are hosted by content-key-svc (the sole KMS holder), so this delegates over gRPC
// (GRPCContentKeyRotator). Declared consumer-side.
type ContentKeyRotator interface {
	Rotate(ctx context.Context, customerID uuid.UUID) (ContentKeyView, error)
}

type contentKeyHandlers struct {
	rotator ContentKeyRotator
	eraser  ContentKeyEraser
	logger  *slog.Logger
}

func registerContentKeys(api huma.API, rotator ContentKeyRotator, eraser ContentKeyEraser, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	h := &contentKeyHandlers{rotator: rotator, eraser: eraser, logger: logger}
	register(api, huma.Operation{
		OperationID: "rotate-content-key", Method: http.MethodPost, Path: "/admin/customers/{id}/content/rotate-key",
		Summary: "Rotate a customer's content encryption key", Tags: []string{"Content & RGPD"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusServiceUnavailable},
	}, h.rotate)
	register(api, huma.Operation{
		OperationID: "erase-customer-content", Method: http.MethodPost, Path: "/admin/customers/{id}/content/erase",
		DefaultStatus: http.StatusAccepted, Summary: "Content-only crypto-shred (scope content:erase)", Tags: []string{"Content & RGPD"},
		Security: scopeSecurity(auth.ScopeContentErase),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusServiceUnavailable},
	}, h.eraseContent)
}

type eraseContentOutput struct{ Body asyncJobDTO }

// eraseContent crypto-shreds a customer's content keys (erase-customer-content, §14). The shred is a fast
// key-metadata update, so it runs synchronously and the 202 carries an already-completed job. It never
// rewrites the CDR: the ciphertext stays, its keys become unrecoverable. Audit: WHAT and WHEN are durable in
// content_keys.destroyed_at; WHO is logged here (operator). A durable operator-attestation for erasures — the
// stronger compliance record — lands with the RGPD attestation of step-166 (gdpr_erase_jobs).
func (h *contentKeyHandlers) eraseContent(ctx context.Context, in *customerIDPathInput) (*eraseContentOutput, error) {
	customerID, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	destroyed, err := h.eraser.Erase(ctx, customerID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	h.logger.InfoContext(ctx, "content crypto-shred",
		"operator", operatorSubject(ctx), "customer_id", customerID, "keys_destroyed", destroyed)

	now := time.Now().UTC()
	detail := fmt.Sprintf("crypto-shred complete: %d content key(s) destroyed", destroyed)
	return &eraseContentOutput{Body: asyncJobDTO{
		JobID: uuid.NewString(), Status: "completed", Progress: ptr(1.0), CreatedAt: now, FinishedAt: &now, Detail: &detail,
	}}, nil
}

// contentKeyDTO conforms to api/openapi-admin.yaml ContentKey. wrapped_key is deliberately absent: the
// wrapped DEK never leaves content-key-svc.
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
