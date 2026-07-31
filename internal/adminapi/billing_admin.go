package adminapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// billingAdminHandlers serves the billing ledger read plus rate-plan and external-provider administration
// (step-149). The message body never reaches here (invariant a): the ledger carries only ids and credits.
type billingAdminHandlers struct {
	customers CustomerStore
	billing   BillingStore
	ratePlans RatePlanStore
	providers BillingProviderStore
}

func registerBillingAdmin(api huma.API, customers CustomerStore, billing BillingStore, ratePlans RatePlanStore, providers BillingProviderStore) {
	h := &billingAdminHandlers{customers: customers, billing: billing, ratePlans: ratePlans, providers: providers}
	// Every op is secured, so 401/403 are always possible — documented on all ten to stay consistent with the
	// rest of the Admin contract (a secured endpoint that hides its auth failures publishes a lie). The extra
	// codes track exactly what each handler can return: a path/customer 404, a body/query 422, a delete 409.
	authList := []int{http.StatusUnauthorized, http.StatusForbidden}
	authNotFound := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound}
	authValidation := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity}
	authNotFoundValidation := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity}

	register(api, huma.Operation{
		OperationID: "get-billing-ledger", Method: http.MethodGet, Path: "/admin/customers/{id}/billing/ledger",
		Summary: "Read the billing ledger", Tags: []string{"Billing"},
		Security: scopeSecurity(auth.ScopeAdminRead), Errors: authNotFoundValidation,
	}, h.getLedger)

	register(api, huma.Operation{
		OperationID: "list-rate-plans", Method: http.MethodGet, Path: "/admin/rate-plans",
		Summary: "List rate plans", Tags: []string{"Rate Plans"},
		Security: scopeSecurity(auth.ScopeAdminRead), Errors: authList,
	}, h.listRatePlans)
	register(api, huma.Operation{
		OperationID: "create-rate-plan", Method: http.MethodPost, Path: "/admin/rate-plans",
		DefaultStatus: http.StatusCreated, Summary: "Create a rate plan", Tags: []string{"Rate Plans"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: authValidation,
	}, h.createRatePlan)
	register(api, huma.Operation{
		OperationID: "update-rate-plan", Method: http.MethodPatch, Path: "/admin/rate-plans/{id}",
		Summary: "Update a rate plan", Tags: []string{"Rate Plans"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: authNotFoundValidation,
	}, h.updateRatePlan)
	register(api, huma.Operation{
		OperationID: "delete-rate-plan", Method: http.MethodDelete, Path: "/admin/rate-plans/{id}",
		DefaultStatus: http.StatusNoContent, Summary: "Delete a rate plan", Tags: []string{"Rate Plans"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict},
	}, h.deleteRatePlan)

	register(api, huma.Operation{
		OperationID: "list-billing-providers", Method: http.MethodGet, Path: "/admin/billing-providers",
		Summary: "List external billing providers", Tags: []string{"Billing Providers"},
		Security: scopeSecurity(auth.ScopeAdminRead), Errors: authList,
	}, h.listProviders)
	register(api, huma.Operation{
		OperationID: "create-billing-provider", Method: http.MethodPost, Path: "/admin/billing-providers",
		DefaultStatus: http.StatusCreated, Summary: "Create an external billing provider", Tags: []string{"Billing Providers"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: authValidation,
	}, h.createProvider)
	register(api, huma.Operation{
		OperationID: "update-billing-provider", Method: http.MethodPatch, Path: "/admin/billing-providers/{id}",
		Summary: "Update an external billing provider", Tags: []string{"Billing Providers"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: authNotFoundValidation,
	}, h.updateProvider)
	register(api, huma.Operation{
		OperationID: "delete-billing-provider", Method: http.MethodDelete, Path: "/admin/billing-providers/{id}",
		DefaultStatus: http.StatusNoContent, Summary: "Delete an external billing provider", Tags: []string{"Billing Providers"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: authNotFound,
	}, h.deleteProvider)
	register(api, huma.Operation{
		OperationID: "test-billing-provider", Method: http.MethodPost, Path: "/admin/billing-providers/{id}/test-connection",
		Summary: "Probe an external billing provider's connectivity", Tags: []string{"Billing Providers"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: authNotFound,
	}, h.testProvider)
}

// --- DTOs (conform to api/openapi-admin.yaml: LedgerPage, RatePlan, ExternalBillingProvider) ---

type ledgerPageDTO struct {
	PageMeta
	Data []ledgerEntryDTO `json:"data"`
}

type ratePlanDTO struct {
	ID                  string         `json:"id" format:"uuid"`
	Name                string         `json:"name"`
	CreditsPerSegmentMT map[string]any `json:"credits_per_segment_mt_json"`
	CreditsPerSegmentMO map[string]any `json:"credits_per_segment_mo_json"`
	BillingMode         string         `json:"billing_mode" enum:"prepaid,postpaid,either"`
	ChargeOn            string         `json:"charge_on" enum:"submission,delivery"`
	Status              string         `json:"status" enum:"active,disabled"`
	CreatedAt           *string        `json:"created_at,omitempty" format:"date-time"`
	UpdatedAt           *string        `json:"updated_at,omitempty" format:"date-time"`
}

type ratePlanCreateBody struct {
	Name                string         `json:"name"`
	CreditsPerSegmentMT map[string]any `json:"credits_per_segment_mt_json"`
	CreditsPerSegmentMO map[string]any `json:"credits_per_segment_mo_json"`
	BillingMode         *string        `json:"billing_mode,omitempty" enum:"prepaid,postpaid,either"`
	ChargeOn            *string        `json:"charge_on,omitempty" enum:"submission,delivery"`
}

type ratePlanUpdateBody struct {
	Name                *string        `json:"name,omitempty"`
	CreditsPerSegmentMT map[string]any `json:"credits_per_segment_mt_json,omitempty"`
	CreditsPerSegmentMO map[string]any `json:"credits_per_segment_mo_json,omitempty"`
	BillingMode         *string        `json:"billing_mode,omitempty" enum:"prepaid,postpaid,either"`
	ChargeOn            *string        `json:"charge_on,omitempty" enum:"submission,delivery"`
	Status              *string        `json:"status,omitempty" enum:"active,disabled"`
}

type providerDTO struct {
	ID                string         `json:"id" format:"uuid"`
	Name              string         `json:"name"`
	BaseURL           string         `json:"base_url" format:"uri"`
	AuthConfig        map[string]any `json:"auth_config_json,omitempty"`
	Mode              string         `json:"mode" enum:"balance_check,consume_delegate_async,consume_delegate_sync,both"`
	CacheTTLMs        *int           `json:"cache_ttl_ms,omitempty" minimum:"0"`
	SyncCallTimeoutMs *int           `json:"sync_call_timeout_ms,omitempty" nullable:"true" minimum:"1"`
	FailurePolicy     string         `json:"failure_policy" enum:"fail_open,fail_closed"`
	Status            string         `json:"status" enum:"active,disabled"`
}

type providerCreateBody struct {
	Name              string         `json:"name"`
	BaseURL           string         `json:"base_url" format:"uri"`
	AuthConfig        map[string]any `json:"auth_config_json,omitempty"`
	Mode              string         `json:"mode" enum:"balance_check,consume_delegate_async,consume_delegate_sync,both"`
	CacheTTLMs        *int           `json:"cache_ttl_ms,omitempty" minimum:"0"`
	SyncCallTimeoutMs *int           `json:"sync_call_timeout_ms,omitempty" nullable:"true" minimum:"1"`
	FailurePolicy     *string        `json:"failure_policy,omitempty" enum:"fail_open,fail_closed"`
}

type providerUpdateBody struct {
	Name              *string        `json:"name,omitempty"`
	BaseURL           *string        `json:"base_url,omitempty" format:"uri"`
	AuthConfig        map[string]any `json:"auth_config_json,omitempty"`
	Mode              *string        `json:"mode,omitempty" enum:"balance_check,consume_delegate_async,consume_delegate_sync,both"`
	CacheTTLMs        *int           `json:"cache_ttl_ms,omitempty" minimum:"0"`
	SyncCallTimeoutMs *int           `json:"sync_call_timeout_ms,omitempty" nullable:"true" minimum:"1"`
	FailurePolicy     *string        `json:"failure_policy,omitempty" enum:"fail_open,fail_closed"`
	Status            *string        `json:"status,omitempty" enum:"active,disabled"`
}

type testConnectionDTO struct {
	OK        bool    `json:"ok"`
	LatencyMs *int    `json:"latency_ms,omitempty" nullable:"true"`
	Detail    *string `json:"detail,omitempty" nullable:"true"`
}

// I/O wrappers
type getLedgerInput struct {
	ID        string `path:"id" format:"uuid"`
	Direction string `query:"direction" enum:"mt,mo"`
	AccountID string `query:"accountId" format:"uuid"`
	Cursor    string `query:"cursor"`
	Limit     int    `query:"limit" minimum:"1" maximum:"500" default:"50"`
}
type ledgerPageOutput struct{ Body ledgerPageDTO }
type ratePlansOutput struct {
	Body []ratePlanDTO
}
type ratePlanOutput struct{ Body ratePlanDTO }
type createRatePlanInput struct{ Body ratePlanCreateBody }
type updateRatePlanInput struct {
	ID   string `path:"id" format:"uuid"`
	Body ratePlanUpdateBody
}
type providersOutput struct {
	Body []providerDTO
}
type providerOutput struct{ Body providerDTO }
type createProviderInput struct{ Body providerCreateBody }
type updateProviderInput struct {
	ID   string `path:"id" format:"uuid"`
	Body providerUpdateBody
}
type resourceIDInput struct {
	ID string `path:"id" format:"uuid"`
}
type testConnectionOutput struct{ Body testConnectionDTO }

// --- ledger ---

func (h *billingAdminHandlers) getLedger(ctx context.Context, in *getLedgerInput) (*ledgerPageOutput, error) {
	customerID, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	// Resolve the customer so an unknown (but well-formed) id is a 404, consistent with the sibling
	// billing endpoints and the contract, rather than an empty 200 that hides the missing customer.
	if _, err := h.customers.Get(ctx, customerID); err != nil {
		return nil, humaerr.FromError(err)
	}
	f := cp.LedgerFilter{CustomerID: customerID, Limit: in.Limit}
	if in.Direction != "" {
		d := in.Direction
		f.Direction = &d
	}
	if in.AccountID != "" {
		aid, aerr := uuid.Parse(in.AccountID)
		if aerr != nil {
			return nil, humaerr.FailValidation("invalid accountId", humaerr.FieldError{Field: "accountId", Message: "must be a UUID"})
		}
		f.AccountID = &aid
	}
	if in.Cursor != "" {
		key, cerr := decodeLedgerCursor(in.Cursor)
		if cerr != nil {
			return nil, humaerr.FailValidation("invalid cursor", humaerr.FieldError{Field: "cursor", Message: "malformed page cursor"})
		}
		f.After = key
	}
	rows, hasMore, err := h.billing.Ledger(ctx, f)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	page := ledgerPageDTO{PageMeta: PageMeta{HasMore: hasMore}, Data: make([]ledgerEntryDTO, 0, len(rows))}
	for _, r := range rows {
		page.Data = append(page.Data, toLedgerEntryDTO(r))
	}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		page.NextCursor = ptr(encodeLedgerCursor(cp.LedgerKey{CreatedAt: last.CreatedAt, ID: last.ID}))
	}
	return &ledgerPageOutput{Body: page}, nil
}

const ledgerCursorSep = "|"

func encodeLedgerCursor(k cp.LedgerKey) string {
	payload := strconv.FormatInt(k.CreatedAt.UnixMicro(), 10) + ledgerCursorSep + k.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func decodeLedgerCursor(s string) (cp.LedgerKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cp.LedgerKey{}, err
	}
	ts, id, ok := strings.Cut(string(raw), ledgerCursorSep)
	if !ok {
		return cp.LedgerKey{}, errors.New("cursor: missing separator")
	}
	us, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return cp.LedgerKey{}, err
	}
	entryID, err := uuid.Parse(id)
	if err != nil {
		return cp.LedgerKey{}, err
	}
	return cp.LedgerKey{CreatedAt: time.UnixMicro(us).UTC(), ID: entryID}, nil
}

// --- rate plans ---

func (h *billingAdminHandlers) listRatePlans(ctx context.Context, _ *struct{}) (*ratePlansOutput, error) {
	plans, err := h.ratePlans.List(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := make([]ratePlanDTO, 0, len(plans))
	for _, p := range plans {
		out = append(out, toRatePlanDTO(p))
	}
	return &ratePlansOutput{Body: out}, nil
}

func (h *billingAdminHandlers) createRatePlan(ctx context.Context, in *createRatePlanInput) (*ratePlanOutput, error) {
	mt, err := marshalJSONMap("credits_per_segment_mt_json", in.Body.CreditsPerSegmentMT)
	if err != nil {
		return nil, err
	}
	mo, err := marshalJSONMap("credits_per_segment_mo_json", in.Body.CreditsPerSegmentMO)
	if err != nil {
		return nil, err
	}
	p, err := h.ratePlans.Create(ctx, cp.NewRatePlan{
		Name: in.Body.Name, CreditsPerSegmentMT: mt, CreditsPerSegmentMO: mo,
		BillingMode: in.Body.BillingMode, ChargeOn: in.Body.ChargeOn,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &ratePlanOutput{Body: toRatePlanDTO(p)}, nil
}

func (h *billingAdminHandlers) updateRatePlan(ctx context.Context, in *updateRatePlanInput) (*ratePlanOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("rate plan")
	}
	patch := cp.RatePlanPatch{Name: in.Body.Name, BillingMode: in.Body.BillingMode, ChargeOn: in.Body.ChargeOn, Status: in.Body.Status}
	if in.Body.CreditsPerSegmentMT != nil {
		if patch.CreditsPerSegmentMT, err = marshalJSONMap("credits_per_segment_mt_json", in.Body.CreditsPerSegmentMT); err != nil {
			return nil, err
		}
	}
	if in.Body.CreditsPerSegmentMO != nil {
		if patch.CreditsPerSegmentMO, err = marshalJSONMap("credits_per_segment_mo_json", in.Body.CreditsPerSegmentMO); err != nil {
			return nil, err
		}
	}
	p, err := h.ratePlans.Update(ctx, id, patch)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &ratePlanOutput{Body: toRatePlanDTO(p)}, nil
}

func (h *billingAdminHandlers) deleteRatePlan(ctx context.Context, in *resourceIDInput) (*struct{}, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("rate plan")
	}
	if err := h.ratePlans.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return nil, nil
}

// --- providers ---

func (h *billingAdminHandlers) listProviders(ctx context.Context, _ *struct{}) (*providersOutput, error) {
	providers, err := h.providers.List(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := make([]providerDTO, 0, len(providers))
	for _, p := range providers {
		out = append(out, toProviderDTO(p))
	}
	return &providersOutput{Body: out}, nil
}

func (h *billingAdminHandlers) createProvider(ctx context.Context, in *createProviderInput) (*providerOutput, error) {
	auth, err := marshalOptionalJSONMap("auth_config_json", in.Body.AuthConfig)
	if err != nil {
		return nil, err
	}
	p, err := h.providers.Create(ctx, cp.NewExternalBillingProvider{
		Name: in.Body.Name, BaseURL: in.Body.BaseURL, AuthConfig: auth, Mode: in.Body.Mode,
		CacheTTLMs: in.Body.CacheTTLMs, SyncCallTimeoutMs: in.Body.SyncCallTimeoutMs, FailurePolicy: in.Body.FailurePolicy,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &providerOutput{Body: toProviderDTO(p)}, nil
}

func (h *billingAdminHandlers) updateProvider(ctx context.Context, in *updateProviderInput) (*providerOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("billing provider")
	}
	patch := cp.ExternalBillingProviderPatch{
		Name: in.Body.Name, BaseURL: in.Body.BaseURL, Mode: in.Body.Mode, CacheTTLMs: in.Body.CacheTTLMs,
		SyncCallTimeoutMs: in.Body.SyncCallTimeoutMs, FailurePolicy: in.Body.FailurePolicy, Status: in.Body.Status,
	}
	// Guard the read-modify-write footgun: a client that reads a provider (auth_config_json comes back
	// masked as {"masked":true}) and PATCHes the whole object back would otherwise overwrite the real
	// credentials with the mask sentinel. Treat the sentinel as "unchanged" so the secret survives.
	if in.Body.AuthConfig != nil && !isMaskedSentinel(in.Body.AuthConfig) {
		if patch.AuthConfig, err = marshalOptionalJSONMap("auth_config_json", in.Body.AuthConfig); err != nil {
			return nil, err
		}
	}
	p, err := h.providers.Update(ctx, id, patch)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &providerOutput{Body: toProviderDTO(p)}, nil
}

func (h *billingAdminHandlers) deleteProvider(ctx context.Context, in *resourceIDInput) (*struct{}, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("billing provider")
	}
	if err := h.providers.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return nil, nil
}

func (h *billingAdminHandlers) testProvider(ctx context.Context, in *resourceIDInput) (*testConnectionOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("billing provider")
	}
	// Loading the provider validates it exists (404 otherwise). The real HTTP probe over base_url ships with
	// the production HTTP provider (deferred, step-147 follow-up); until then the connectivity check reports a
	// stub OK so the endpoint is exercisable end to end.
	if _, err := h.providers.Get(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	zero := 0
	detail := "stub provider: real HTTP connectivity probe deferred"
	return &testConnectionOutput{Body: testConnectionDTO{OK: true, LatencyMs: &zero, Detail: &detail}}, nil
}

// --- mappers ---

func marshalJSONMap(field string, m map[string]any) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, humaerr.FailValidation("invalid "+field, humaerr.FieldError{Field: field, Message: "must be a JSON object"})
	}
	return b, nil
}

func marshalOptionalJSONMap(field string, m map[string]any) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return marshalJSONMap(field, m)
}

func unmarshalJSONMap(b []byte) map[string]any {
	if len(b) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

func toRatePlanDTO(p cp.RatePlan) ratePlanDTO {
	return ratePlanDTO{
		ID: p.ID.String(), Name: p.Name,
		CreditsPerSegmentMT: unmarshalJSONMap(p.CreditsPerSegmentMT), CreditsPerSegmentMO: unmarshalJSONMap(p.CreditsPerSegmentMO),
		BillingMode: p.BillingMode, ChargeOn: p.ChargeOn, Status: p.Status,
		CreatedAt: ptr(p.CreatedAt.Format(timeRFC3339)), UpdatedAt: ptr(p.UpdatedAt.Format(timeRFC3339)),
	}
}

func toProviderDTO(p cp.ExternalBillingProvider) providerDTO {
	return providerDTO{
		ID: p.ID.String(), Name: p.Name, BaseURL: p.BaseURL,
		// auth_config_json is MASKED on read (§6.10, CLAUDE.md secrets): the credentials never leave the server.
		AuthConfig: maskedAuthConfig(),
		Mode:       p.Mode, CacheTTLMs: ptr(p.CacheTTLMs), SyncCallTimeoutMs: p.SyncCallTimeoutMs,
		FailurePolicy: p.FailurePolicy, Status: p.Status,
	}
}

// maskedAuthConfig is the placeholder returned in place of a provider's real credentials on read. Recognized
// again on write (isMaskedSentinel) so a read-modify-write round-trip cannot overwrite the secret with it.
func maskedAuthConfig() map[string]any { return map[string]any{"masked": true} }

// isMaskedSentinel reports whether m is exactly the {"masked": true} placeholder toProviderDTO emits.
func isMaskedSentinel(m map[string]any) bool {
	if len(m) != 1 {
		return false
	}
	v, ok := m["masked"]
	return ok && v == true
}
