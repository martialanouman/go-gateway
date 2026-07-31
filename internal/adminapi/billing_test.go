package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// --- fakes ---

type fakeBillingCustomerStore struct{ c cp.Customer }

func (s fakeBillingCustomerStore) Create(context.Context, cp.NewCustomer) (cp.Customer, error) {
	return cp.Customer{}, nil
}
func (s fakeBillingCustomerStore) Get(_ context.Context, id uuid.UUID) (cp.Customer, error) {
	if id != s.c.ID {
		return cp.Customer{}, errs.ErrNotFound
	}
	return s.c, nil
}
func (s fakeBillingCustomerStore) List(context.Context, cp.CustomerFilter) (cp.Page[cp.Customer], error) {
	return cp.Page[cp.Customer]{}, nil
}
func (s fakeBillingCustomerStore) Update(_ context.Context, _ uuid.UUID, _ cp.CustomerPatch) (cp.Customer, error) {
	return s.c, nil
}
func (s fakeBillingCustomerStore) Delete(context.Context, uuid.UUID) error { return nil }
func (s fakeBillingCustomerStore) Suspend(context.Context, uuid.UUID) (cp.Customer, error) {
	return s.c, nil
}

type fakeBillingStore struct {
	topupRow    cp.LedgerRow
	transferRow []cp.LedgerRow
	scopeErr    error
	balances    []cp.BalanceRow
	topupCalls  int
	replay      bool
	ledgerRows  []cp.LedgerRow
	ledgerMore  bool
	ledgerFn    func(cp.LedgerFilter) ([]cp.LedgerRow, bool, error)
}

func (s *fakeBillingStore) Balances(context.Context, []cp.BalanceOwner) ([]cp.BalanceRow, error) {
	return s.balances, nil
}
func (s *fakeBillingStore) Topup(_ context.Context, e cp.LedgerEntry) (cp.LedgerRow, bool, error) {
	s.topupCalls++
	if s.replay {
		return cp.LedgerRow{}, false, nil
	}
	row := s.topupRow
	row.Credits = e.Credits
	row.OwnerType = e.OwnerType
	row.OwnerID = e.OwnerID
	return row, true, nil
}
func (s *fakeBillingStore) Transfer(context.Context, cp.LedgerEntry, cp.LedgerEntry, uuid.UUID) ([]cp.LedgerRow, bool, error) {
	return s.transferRow, true, nil
}
func (s *fakeBillingStore) Ledger(_ context.Context, f cp.LedgerFilter) ([]cp.LedgerRow, bool, error) {
	if s.ledgerFn != nil {
		return s.ledgerFn(f)
	}
	return s.ledgerRows, s.ledgerMore, nil
}

func (s *fakeBillingStore) ChangeBalanceScope(context.Context, uuid.UUID, []cp.BalanceOwner, string) error {
	return s.scopeErr
}

type fakeBalanceCache struct{ deleted []string }

func (c *fakeBalanceCache) Del(_ context.Context, keys ...string) error {
	c.deleted = append(c.deleted, keys...)
	return nil
}

func customerScoped(id uuid.UUID) cp.Customer {
	return cp.Customer{ID: id, BalanceScope: cp.BalanceScopeCustomer, UpdatedAt: time.Now().UTC()}
}

// TestTopupCreditsAndInvalidatesCache: a top-up returns 200 with the created ledger entry and deletes the
// owner's MT+MO balance-cache keys so the credit is usable immediately.
func TestTopupCreditsAndInvalidatesCache(t *testing.T) {
	cust := uuid.New()
	billing := &fakeBillingStore{topupRow: cp.LedgerRow{ID: uuid.New(), Direction: cp.BillingDirectionMT, BalanceAfter: 100, CreatedAt: time.Now().UTC(), EntryType: cp.EntryTopup, CustomerID: cust}}
	cache := &fakeBalanceCache{}
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: billing, BalanceCache: cache})

	body := `{"credits":100,"direction":"mt","idempotency_key":"` + uuid.NewString() + `"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+cust.String()+"/billing/topup", body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var entry struct {
		Credits      int `json:"credits"`
		BalanceAfter int `json:"balance_after"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &entry)
	if entry.Credits != 100 || entry.BalanceAfter != 100 {
		t.Errorf("ledger entry = %+v, want credits=100 balance_after=100", entry)
	}
	if billing.topupCalls != 1 {
		t.Errorf("topup calls = %d, want 1", billing.topupCalls)
	}
	if len(cache.deleted) != 2 {
		t.Errorf("cache keys deleted = %v, want 2 (MT+MO of the customer owner)", cache.deleted)
	}
}

// TestTopupRejectsAccountIDForCustomerScope: passing account_id for a customer-scoped customer is a 422 (the
// operator is confused about where the money goes), not silently ignored.
func TestTopupRejectsAccountIDForCustomerScope(t *testing.T) {
	cust := uuid.New()
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: &fakeBillingStore{}})

	body := `{"credits":100,"direction":"mt","account_id":"` + uuid.NewString() + `","idempotency_key":"` + uuid.NewString() + `"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+cust.String()+"/billing/topup", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body.String())
	}
}

// TestTopupReplayIsConflict: an idempotent replay (already applied) returns 409, not a misleading empty 200.
func TestTopupReplayIsConflict(t *testing.T) {
	cust := uuid.New()
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: &fakeBillingStore{replay: true}})

	body := `{"credits":100,"direction":"mt","idempotency_key":"` + uuid.NewString() + `"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+cust.String()+"/billing/topup", body))
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 on an idempotent replay; body=%s", w.Code, w.Body.String())
	}
}

// TestTopupRejectsMODirection: only the MT prepaid balance can be topped up; direction=mo is a 422.
func TestTopupRejectsMODirection(t *testing.T) {
	cust := uuid.New()
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: &fakeBillingStore{}})

	body := `{"credits":100,"direction":"mo","idempotency_key":"` + uuid.NewString() + `"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+cust.String()+"/billing/topup", body))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for direction=mo; body=%s", w.Code, w.Body.String())
	}
}

// TestChangeScopeConflictOnNonZeroBalance: the guarded flip surfaces the repo's conflict as a 409.
func TestChangeScopeConflictOnNonZeroBalance(t *testing.T) {
	cust := uuid.New()
	billing := &fakeBillingStore{scopeErr: errs.ErrConflict}
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: customerScoped(cust)}, Billing: billing})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+cust.String()+"/billing/scope", `{"balance_scope":"smpp_account"}`))
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
}

// TestGetBillingProjectsConfig: the config read defaults an unset billing_mode to prepaid and echoes the fields.
func TestGetBillingProjectsConfig(t *testing.T) {
	cust := uuid.New()
	c := customerScoped(cust)
	c.OverdraftEnabled = true
	api := newTestAPIWith(t, adminapi.Deps{Customers: fakeBillingCustomerStore{c: c}, Billing: &fakeBillingStore{}})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers/"+cust.String()+"/billing", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var dto struct {
		BillingMode      string `json:"billing_mode"`
		OverdraftEnabled bool   `json:"overdraft_enabled"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &dto)
	if dto.BillingMode != "prepaid" || !dto.OverdraftEnabled {
		t.Errorf("billing dto = %+v, want prepaid/overdraft-enabled", dto)
	}
}
