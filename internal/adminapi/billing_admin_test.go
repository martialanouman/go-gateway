package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// --- fakes ---

type fakeRatePlanStore struct {
	plans     []cp.RatePlan
	created   cp.NewRatePlan
	patched   cp.RatePlanPatch
	deleteErr error
	deletedID uuid.UUID
}

func (s *fakeRatePlanStore) List(context.Context) ([]cp.RatePlan, error) { return s.plans, nil }
func (s *fakeRatePlanStore) Create(_ context.Context, in cp.NewRatePlan) (cp.RatePlan, error) {
	s.created = in
	return cp.RatePlan{
		ID: uuid.New(), Name: in.Name, CreditsPerSegmentMT: in.CreditsPerSegmentMT, CreditsPerSegmentMO: in.CreditsPerSegmentMO,
		BillingMode: "either", ChargeOn: "submission", Status: "active", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}, nil
}
func (s *fakeRatePlanStore) Update(_ context.Context, id uuid.UUID, p cp.RatePlanPatch) (cp.RatePlan, error) {
	s.patched = p
	return cp.RatePlan{ID: id, Name: "plan", BillingMode: "either", ChargeOn: "submission", Status: "disabled"}, nil
}
func (s *fakeRatePlanStore) Delete(_ context.Context, id uuid.UUID) error {
	s.deletedID = id
	return s.deleteErr
}

type fakeProviderStore struct {
	providers []cp.ExternalBillingProvider
	one       cp.ExternalBillingProvider
	getErr    error
	created   cp.NewExternalBillingProvider
}

func (s *fakeProviderStore) List(context.Context) ([]cp.ExternalBillingProvider, error) {
	return s.providers, nil
}
func (s *fakeProviderStore) Get(context.Context, uuid.UUID) (cp.ExternalBillingProvider, error) {
	return s.one, s.getErr
}
func (s *fakeProviderStore) Create(_ context.Context, in cp.NewExternalBillingProvider) (cp.ExternalBillingProvider, error) {
	s.created = in
	return cp.ExternalBillingProvider{
		ID: uuid.New(), Name: in.Name, BaseURL: in.BaseURL, AuthConfig: in.AuthConfig,
		Mode: in.Mode, CacheTTLMs: 1000, FailurePolicy: "fail_open", Status: "active",
	}, nil
}
func (s *fakeProviderStore) Update(_ context.Context, id uuid.UUID, _ cp.ExternalBillingProviderPatch) (cp.ExternalBillingProvider, error) {
	return cp.ExternalBillingProvider{ID: id, Name: "p", BaseURL: "https://x", Mode: "balance_check", CacheTTLMs: 1000, FailurePolicy: "fail_open", Status: "disabled"}, nil
}
func (s *fakeProviderStore) Delete(context.Context, uuid.UUID) error { return nil }

// captureProviderStore records the patch passed to Update so a test can assert what would be persisted.
type captureProviderStore struct {
	fakeProviderStore
	onUpdate func(cp.ExternalBillingProviderPatch)
}

func (s *captureProviderStore) Update(_ context.Context, id uuid.UUID, p cp.ExternalBillingProviderPatch) (cp.ExternalBillingProvider, error) {
	if s.onUpdate != nil {
		s.onUpdate(p)
	}
	return cp.ExternalBillingProvider{ID: id, Name: "p", BaseURL: "https://x", Mode: "balance_check", CacheTTLMs: 1000, FailurePolicy: "fail_open", Status: "disabled"}, nil
}

// --- ledger ---

// TestGetLedgerReturnsPageWithNextCursor: two rows plus has_more emit a page with a next_cursor that,
// replayed as ?cursor=, decodes into the last row's (created_at, id) keyset position — no row is skipped.
func TestGetLedgerReturnsPageWithNextCursor(t *testing.T) {
	cust := uuid.New()
	last := cp.LedgerRow{ID: uuid.New(), OwnerType: "customer", OwnerID: cust, Direction: "mt", CustomerID: cust, EntryType: cp.EntryTopup, Credits: 10, CreatedAt: time.UnixMicro(1_700_000_000_123_789).UTC()}
	first := cp.LedgerRow{ID: uuid.New(), OwnerType: "customer", OwnerID: cust, Direction: "mt", CustomerID: cust, EntryType: cp.EntryTopup, Credits: 20, CreatedAt: time.UnixMicro(1_700_000_001_000_000).UTC()}
	billing := &fakeBillingStore{ledgerRows: []cp.LedgerRow{first, last}, ledgerMore: true}
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: billing})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/"+cust.String()+"/billing/ledger", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var page struct {
		Data       []map[string]any `json:"data"`
		HasMore    bool             `json:"has_more"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Data) != 2 || !page.HasMore || page.NextCursor == nil {
		t.Fatalf("page = %d rows, has_more=%v, cursor=%v", len(page.Data), page.HasMore, page.NextCursor)
	}

	// Replay the cursor: the store must receive the last row's keyset position.
	var gotAfter cp.LedgerKey
	billing.ledgerFn = func(f cp.LedgerFilter) ([]cp.LedgerRow, bool, error) {
		gotAfter = f.After
		return nil, false, nil
	}
	w2 := httptest.NewRecorder()
	api.ServeHTTP(w2, authed(t, http.MethodGet, "/v1/admin/customers/"+cust.String()+"/billing/ledger?cursor="+*page.NextCursor, ""))
	if w2.Code != http.StatusOK {
		t.Fatalf("replay status = %d; body=%s", w2.Code, w2.Body.String())
	}
	if gotAfter.ID != last.ID || !gotAfter.CreatedAt.Equal(last.CreatedAt) {
		t.Errorf("cursor decoded to (%s, %s), want (%s, %s)", gotAfter.ID, gotAfter.CreatedAt, last.ID, last.CreatedAt)
	}
}

// TestGetLedgerFiltersByDirectionAndAccount: the optional query filters reach the store filter.
func TestGetLedgerFiltersByDirectionAndAccount(t *testing.T) {
	cust := uuid.New()
	acc := uuid.New()
	var got cp.LedgerFilter
	billing := &fakeBillingStore{ledgerFn: func(f cp.LedgerFilter) ([]cp.LedgerRow, bool, error) {
		got = f
		return nil, false, nil
	}}
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: billing})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/"+cust.String()+"/billing/ledger?direction=mt&accountId="+acc.String()+"&limit=10", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	if got.CustomerID != cust || got.Direction == nil || *got.Direction != "mt" || got.AccountID == nil || *got.AccountID != acc || got.Limit != 10 {
		t.Errorf("filter = %+v (dir=%v acct=%v)", got, got.Direction, got.AccountID)
	}
}

// TestGetLedgerInvalidCursorAndAccountAre422: malformed keyset cursor or accountId is a client error.
func TestGetLedgerInvalidCursorAndAccountAre422(t *testing.T) {
	cust := uuid.New()
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: &fakeBillingStore{}})
	base := "/v1/admin/customers/" + cust.String() + "/billing/ledger"
	for _, q := range []string{"?cursor=!!!not-base64", "?accountId=not-a-uuid"} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodGet, base+q, ""))
		if w.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s: status = %d, want 422; body=%s", q, w.Code, w.Body.String())
		}
	}
}

// --- rate plans ---

// TestListRatePlansDecodesCreditMaps: the jsonb credit maps surface as JSON objects, never raw bytes.
func TestListRatePlansDecodesCreditMaps(t *testing.T) {
	plans := []cp.RatePlan{{
		ID: uuid.New(), Name: "std", CreditsPerSegmentMT: []byte(`{"default":2}`), CreditsPerSegmentMO: []byte(`{"default":0}`),
		BillingMode: "either", ChargeOn: "submission", Status: "active",
	}}
	api := newTestAPIWith(t, adminapi.Deps{RatePlans: &fakeRatePlanStore{plans: plans}})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/rate-plans", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	var out []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	mt, ok := out[0]["credits_per_segment_mt_json"].(map[string]any)
	if !ok || mt["default"].(float64) != 2 {
		t.Errorf("credits map = %v, want {default:2}", out[0]["credits_per_segment_mt_json"])
	}
}

// TestCreateRatePlanMarshalsCredits: the request's credit objects are persisted as jsonb; 201 on success.
func TestCreateRatePlanMarshalsCredits(t *testing.T) {
	store := &fakeRatePlanStore{}
	api := newTestAPIWith(t, adminapi.Deps{RatePlans: store})
	body := `{"name":"std","credits_per_segment_mt_json":{"default":3},"credits_per_segment_mo_json":{"default":0}}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/rate-plans", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(string(store.created.CreditsPerSegmentMT), `"default":3`) {
		t.Errorf("persisted MT credits = %s", store.created.CreditsPerSegmentMT)
	}
}

// TestDeleteRatePlanInUseReturns409: a plan still assigned to a customer cannot be deleted (FK conflict).
func TestDeleteRatePlanInUseReturns409(t *testing.T) {
	store := &fakeRatePlanStore{deleteErr: errs.ErrConflict}
	api := newTestAPIWith(t, adminapi.Deps{RatePlans: store})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/rate-plans/"+uuid.NewString(), ""))
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
}

// --- providers ---

// TestProviderAuthConfigMaskedOnRead is the secrets invariant (CLAUDE.md): the stored auth credentials
// never appear in a read response — auth_config_json comes back masked, and the secret value is absent.
func TestProviderAuthConfigMaskedOnRead(t *testing.T) {
	prov := cp.ExternalBillingProvider{
		ID: uuid.New(), Name: "acme", BaseURL: "https://acme.example", AuthConfig: []byte(`{"api_key":"SUPER-SECRET"}`),
		Mode: "balance_check", CacheTTLMs: 1000, FailurePolicy: "fail_open", Status: "active",
	}
	api := newTestAPIWith(t, adminapi.Deps{BillingProviders: &fakeProviderStore{providers: []cp.ExternalBillingProvider{prov}}})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/billing-providers", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "SUPER-SECRET") {
		t.Fatalf("secret leaked in list response: %s", w.Body.String())
	}
	var out []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	auth, ok := out[0]["auth_config_json"].(map[string]any)
	if !ok || auth["masked"] != true {
		t.Errorf("auth_config_json = %v, want {masked:true}", out[0]["auth_config_json"])
	}
}

// TestTestProviderStubOK: the connectivity probe loads the provider (404 if absent) and reports a stub OK
// until the real HTTP probe ships.
func TestTestProviderStubOK(t *testing.T) {
	prov := cp.ExternalBillingProvider{ID: uuid.New(), Name: "acme", BaseURL: "https://acme.example", Mode: "balance_check", CacheTTLMs: 1000, FailurePolicy: "fail_open", Status: "active"}
	api := newTestAPIWith(t, adminapi.Deps{BillingProviders: &fakeProviderStore{one: prov}})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/billing-providers/"+prov.ID.String()+"/test-connection", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var got struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.OK {
		t.Errorf("ok = false, want true")
	}
}

// TestUpdateProviderIgnoresMaskedSentinel: a client that reads a provider (auth masked) and PATCHes the
// whole object back sends auth_config_json:{masked:true}; the server must NOT persist that over the real
// secret — the mask sentinel is treated as "unchanged".
func TestUpdateProviderIgnoresMaskedSentinel(t *testing.T) {
	var gotPatch cp.ExternalBillingProviderPatch
	store := &captureProviderStore{onUpdate: func(p cp.ExternalBillingProviderPatch) { gotPatch = p }}
	api := newTestAPIWith(t, adminapi.Deps{BillingProviders: store})
	body := `{"auth_config_json":{"masked":true},"status":"disabled"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/billing-providers/"+uuid.NewString(), body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if gotPatch.AuthConfig != nil {
		t.Errorf("AuthConfig patch = %s, want nil (mask sentinel must not clobber the secret)", gotPatch.AuthConfig)
	}
	if gotPatch.Status == nil || *gotPatch.Status != "disabled" {
		t.Errorf("Status patch = %v, want disabled (other fields still applied)", gotPatch.Status)
	}
}

// TestUpdateProviderPersistsRealAuthConfig: a genuine auth object IS persisted (the sentinel guard must not
// swallow real credential updates).
func TestUpdateProviderPersistsRealAuthConfig(t *testing.T) {
	var gotPatch cp.ExternalBillingProviderPatch
	store := &captureProviderStore{onUpdate: func(p cp.ExternalBillingProviderPatch) { gotPatch = p }}
	api := newTestAPIWith(t, adminapi.Deps{BillingProviders: store})
	body := `{"auth_config_json":{"api_key":"new-secret"}}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/billing-providers/"+uuid.NewString(), body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(string(gotPatch.AuthConfig), "new-secret") {
		t.Errorf("AuthConfig patch = %s, want it to carry the real secret", gotPatch.AuthConfig)
	}
}

// TestGetLedgerUnknownCustomerReturns404: a well-formed but unknown customer id is a 404, not an empty 200.
func TestGetLedgerUnknownCustomerReturns404(t *testing.T) {
	known := uuid.New()
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(known)}, Billing: &fakeBillingStore{}})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/"+uuid.NewString()+"/billing/ledger", ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
}

// TestTestProviderNotFound: probing an unknown provider is a 404.
func TestTestProviderNotFound(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{BillingProviders: &fakeProviderStore{getErr: errs.ErrNotFound}})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/billing-providers/"+uuid.NewString()+"/test-connection", ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
}
