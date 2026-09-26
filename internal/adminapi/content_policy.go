package adminapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// platformContentRetentionDays is the body retention the CDR applies: the content-column TTL of
// migrations/clickhouse/0003_cdr_content_ttl.up.sql. A test holds the two equal; changing it is an ALTER
// on ClickHouse, not a write through this API.
const platformContentRetentionDays = 30

type contentPolicyDTO struct {
	ContentStorage       string `json:"content_storage" enum:"inherit,off,stored_plaintext,stored_encrypted"`
	ContentRetentionDays *int   `json:"content_retention_days,omitempty" minimum:"0" nullable:"true"`
}

type contentPolicyOutput struct{ Body contentPolicyDTO }

type contentPolicyHandlers struct {
	platform  PlatformContentPolicyStore
	customers CustomerStore
}

func registerContentPolicies(api huma.API, platform PlatformContentPolicyStore, customers CustomerStore) {
	h := &contentPolicyHandlers{platform: platform, customers: customers}
	tags := []string{"Content & RGPD"}

	register(api, huma.Operation{
		OperationID: "get-platform-content-policy", Method: http.MethodGet, Path: "/admin/platform/content-policy",
		Summary: "Get platform default content policy", Tags: tags,
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden},
	}, h.getPlatform)

	register(api, huma.Operation{
		OperationID: "update-platform-content-policy", Method: http.MethodPatch, Path: "/admin/platform/content-policy",
		Summary: "Update platform default content policy", Tags: tags,
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.updatePlatform)

	register(api, huma.Operation{
		OperationID: "get-customer-content-policy", Method: http.MethodGet, Path: "/admin/customers/{id}/content-policy",
		Summary: "Get a customer's content policy", Tags: tags,
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.getCustomer)

	register(api, huma.Operation{
		OperationID: "update-customer-content-policy", Method: http.MethodPatch, Path: "/admin/customers/{id}/content-policy",
		Summary: "Update a customer's content policy", Tags: tags,
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.updateCustomer)
}

func (h *contentPolicyHandlers) getPlatform(ctx context.Context, _ *struct{}) (*contentPolicyOutput, error) {
	cs, err := h.platform.PlatformContentStorage(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return platformOutput(cs), nil
}

type updateContentPolicyInput struct{ Body contentPolicyDTO }

func (h *contentPolicyHandlers) updatePlatform(ctx context.Context, in *updateContentPolicyInput) (*contentPolicyOutput, error) {
	if d := in.Body.ContentRetentionDays; d != nil && *d != platformContentRetentionDays {
		return nil, humaerr.FailValidation("content_retention_days is not settable",
			humaerr.FieldError{Field: "content_retention_days", Message: "the platform body retention is a ClickHouse column TTL, altered as an operations task"})
	}
	// The table's CHECK is the guard; this only names the field the dashboard must correct.
	if in.Body.ContentStorage != string(cp.ContentOff) && in.Body.ContentStorage != string(cp.ContentStoredEncrypted) {
		return nil, humaerr.FailValidation("the platform default is off or stored_encrypted",
			humaerr.FieldError{Field: "content_storage", Message: "inherit has no meaning here, and storage in clear needs a customer's own contract"})
	}
	cs, err := h.platform.SetPlatformContentStorage(ctx, cp.ContentStorage(in.Body.ContentStorage))
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return platformOutput(cs), nil
}

func platformOutput(cs cp.ContentStorage) *contentPolicyOutput {
	return &contentPolicyOutput{Body: contentPolicyDTO{ContentStorage: string(cs), ContentRetentionDays: ptr(platformContentRetentionDays)}}
}

func (h *contentPolicyHandlers) getCustomer(ctx context.Context, in *customerIDInput) (*contentPolicyOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	c, err := h.customers.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return customerPolicyOutput(c), nil
}

type updateCustomerContentPolicyInput struct {
	ID   string `path:"id" format:"uuid"`
	Body contentPolicyDTO
}

func (h *contentPolicyHandlers) updateCustomer(ctx context.Context, in *updateCustomerContentPolicyInput) (*contentPolicyOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	cs := cp.ContentStorage(in.Body.ContentStorage)
	c, err := h.customers.Update(ctx, id, cp.CustomerPatch{ContentStorage: &cs, ContentRetentionDays: in.Body.ContentRetentionDays})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return customerPolicyOutput(c), nil
}

func customerPolicyOutput(c cp.Customer) *contentPolicyOutput {
	return &contentPolicyOutput{Body: contentPolicyDTO{ContentStorage: string(c.ContentStorage), ContentRetentionDays: c.ContentRetentionDays}}
}
