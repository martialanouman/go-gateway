package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// webhookDTO is the wire form of a Webhook (contract schema Webhook). Secret is deliberately absent:
// the operator supplies it and nothing ever reads it back out — see the create and update bodies.
type webhookDTO struct {
	ID          string         `json:"id" format:"uuid"`
	AccountID   string         `json:"account_id" format:"uuid"`
	EventType   string         `json:"event_type" enum:"mo,dlr"`
	URL         string         `json:"url" format:"uri"`
	RetryPolicy map[string]any `json:"retry_policy_json,omitempty" doc:"Retry bounds. Only max_attempts applies to deferred retries; the back-off fields pace the in-band sender, which the deferred retry path replaces in production."`
	Status      string         `json:"status" enum:"active,disabled"`
	// created_at and updated_at are not required by the contract, so they carry omitempty to stay out
	// of the generated schema's required list; a time.Time is never "empty" to encoding/json, so they
	// are still always serialised.
	CreatedAt time.Time `json:"created_at,omitempty" format:"date-time"`
	UpdatedAt time.Time `json:"updated_at,omitempty" format:"date-time"`
}

func toWebhookDTO(wh cp.Webhook) webhookDTO {
	return webhookDTO{
		ID:          idString(wh.ID),
		AccountID:   idString(wh.AccountID),
		EventType:   string(wh.EventType),
		URL:         wh.URL,
		RetryPolicy: decodePolicy(wh.RetryPolicyJSON),
		Status:      string(wh.Status),
		CreatedAt:   wh.CreatedAt,
		UpdatedAt:   wh.UpdatedAt,
	}
}

// decodePolicy renders the stored jsonb as the object the contract declares. A column that somehow
// holds something other than an object is reported as absent rather than as a 500: the policy is a
// setting, and one bad row must not make an account's webhooks unreadable.
func decodePolicy(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

// encodePolicy turns a supplied policy back into the jsonb the column holds. A nil map means the field
// was omitted, which leaves the stored policy alone.
func encodePolicy(m map[string]any) (json.RawMessage, error) {
	if m == nil {
		return nil, nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, humaerr.FailValidation("invalid retry policy",
			humaerr.FieldError{Field: "retry_policy_json", Message: "not a JSON object"})
	}
	return raw, nil
}

// webhookCreateBody is the contract schema WebhookCreate. secret is write-only and required: it is the
// HMAC key the receiver verifies each delivery with, so an empty one would make every signature
// computable by anyone who knows the scheme.
type webhookCreateBody struct {
	EventType   string         `json:"event_type" enum:"mo,dlr"`
	URL         string         `json:"url" format:"uri" minLength:"1"`
	Secret      string         `json:"secret" minLength:"16" doc:"Write-only HMAC-SHA256 signing secret."`
	RetryPolicy map[string]any `json:"retry_policy_json,omitempty" doc:"Retry bounds. Only max_attempts applies to deferred retries; the back-off fields pace the in-band sender, which the deferred retry path replaces in production."`
}

// webhookUpdateBody is the contract schema WebhookUpdate: every field optional. event_type is absent
// on purpose — it is the identity of the subscription, not a setting.
type webhookUpdateBody struct {
	URL         *string        `json:"url,omitempty" format:"uri" minLength:"1"`
	Secret      *string        `json:"secret,omitempty" minLength:"16" doc:"Write-only; rotates the signing secret."`
	RetryPolicy map[string]any `json:"retry_policy_json,omitempty" doc:"Retry bounds. Only max_attempts applies to deferred retries; the back-off fields pace the in-band sender, which the deferred retry path replaces in production."`
	Status      *string        `json:"status,omitempty" enum:"active,disabled"`
}

type webhookHandlers struct {
	hooks    WebhookStore
	accounts AccountStore
}

func registerWebhooks(api huma.API, hooks WebhookStore, accounts AccountStore) {
	h := &webhookHandlers{hooks: hooks, accounts: accounts}

	register(api, huma.Operation{
		OperationID: "list-webhooks", Method: http.MethodGet, Path: "/admin/smpp-accounts/{id}/webhooks",
		Summary: "List webhooks", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-webhook", Method: http.MethodPost, Path: "/admin/smpp-accounts/{id}/webhooks",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a webhook (one per event_type)", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusConflict, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-webhook", Method: http.MethodPatch,
		Path:    "/admin/smpp-accounts/{id}/webhooks/{webhookId}",
		Summary: "Update a webhook", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-webhook", Method: http.MethodDelete,
		Path:          "/admin/smpp-accounts/{id}/webhooks/{webhookId}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a webhook", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusUnprocessableEntity},
	}, h.delete)
}

type webhookAccountInput struct {
	ID string `path:"id" format:"uuid"`
}

// listWebhooksOutput is a bare array, not a page: the contract says so, and webhooks_uq bounds the
// list at two rows per account anyway.
type listWebhooksOutput struct{ Body []webhookDTO }

// account resolves the path's account segment, and exists to honour the 404 the contract declares.
// Without it an unknown account would answer 200 with an empty list, and a create would reach the
// foreign key and come back as a validation failure — a path to a resource that does not exist is a
// missing resource.
func (h *webhookHandlers) account(ctx context.Context, raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, notFound("smpp account")
	}
	if _, err := h.accounts.Get(ctx, id); err != nil {
		return uuid.Nil, humaerr.FromError(err)
	}
	return id, nil
}

func (h *webhookHandlers) list(ctx context.Context, in *webhookAccountInput) (*listWebhooksOutput, error) {
	accountID, err := h.account(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	hooks, err := h.hooks.List(ctx, accountID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listWebhooksOutput{Body: make([]webhookDTO, 0, len(hooks))}
	for _, wh := range hooks {
		out.Body = append(out.Body, toWebhookDTO(wh))
	}
	return out, nil
}

type createWebhookInput struct {
	ID   string `path:"id" format:"uuid"`
	Body webhookCreateBody
}
type webhookOutput struct{ Body webhookDTO }

func (h *webhookHandlers) create(ctx context.Context, in *createWebhookInput) (*webhookOutput, error) {
	accountID, err := h.account(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	policy, err := encodePolicy(in.Body.RetryPolicy)
	if err != nil {
		return nil, err
	}
	wh, err := h.hooks.Create(ctx, cp.NewWebhook{
		AccountID:       accountID,
		EventType:       cp.WebhookEventType(in.Body.EventType),
		URL:             in.Body.URL,
		Secret:          in.Body.Secret,
		RetryPolicyJSON: policy,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &webhookOutput{Body: toWebhookDTO(wh)}, nil
}

// webhookIDInput carries both path segments. They are used together in every store call: the webhook
// id is a UUID the caller chooses, and the account it belongs to is the only thing that keeps one
// account's operator from rewriting another's delivery URL.
type webhookIDInput struct {
	ID        string `path:"id" format:"uuid"`
	WebhookID string `path:"webhookId" format:"uuid"`
}

type updateWebhookInput struct {
	ID        string `path:"id" format:"uuid"`
	WebhookID string `path:"webhookId" format:"uuid"`
	Body      webhookUpdateBody
}

func (h *webhookHandlers) update(ctx context.Context, in *updateWebhookInput) (*webhookOutput, error) {
	accountID, err := h.account(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(in.WebhookID)
	if err != nil {
		return nil, notFound("webhook")
	}
	policy, err := encodePolicy(in.Body.RetryPolicy)
	if err != nil {
		return nil, err
	}
	wh, err := h.hooks.Update(ctx, accountID, id, cp.WebhookPatch{
		URL:             in.Body.URL,
		Secret:          in.Body.Secret,
		RetryPolicyJSON: policy,
		Status:          enumPtr[cp.WebhookStatus](in.Body.Status),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &webhookOutput{Body: toWebhookDTO(wh)}, nil
}

func (h *webhookHandlers) delete(ctx context.Context, in *webhookIDInput) (*deleteOutput, error) {
	accountID, err := h.account(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(in.WebhookID)
	if err != nil {
		return nil, notFound("webhook")
	}
	if err := h.hooks.Delete(ctx, accountID, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}
