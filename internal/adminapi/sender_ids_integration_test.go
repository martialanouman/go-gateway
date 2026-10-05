package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestUsedSenderIDCanBeDisabledButNotDeleted drives ADR-0023 through the real repository: the mark is
// posed by the very method the mt.outcome consumer calls, and the 409 comes from the guarded DELETE.
func TestUsedSenderIDCanBeDisabledButNotDeleted(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := t.Context()
	senders := postgres.NewSenderIDRepo(pool)
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, newCustomerInput("sender-use-"+uuid.NewString()))
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{Customers: postgres.NewCustomerRepo(pool), SenderIDs: senders})
	base := "/v1/admin/customers/" + customer.ID.String() + "/sender-ids"

	create := func(address string) string {
		t.Helper()
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodPost, base, `{"address":"`+address+`"}`))
		if w.Code != http.StatusCreated {
			t.Fatalf("create %s: status = %d; body=%s", address, w.Code, w.Body)
		}
		var got struct{ ID string }
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		return got.ID
	}
	usedID, unusedID := create("USED"), create("NEVER")

	firstUse := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	if err := senders.MarkFirstUsed(ctx, []cp.SenderIDUse{{CustomerID: customer.ID, Address: "USED", UsedAt: firstUse}}); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := senders.MarkFirstUsed(ctx, []cp.SenderIDUse{{CustomerID: customer.ID, Address: "USED", UsedAt: firstUse.Add(time.Hour)}}); err != nil {
		t.Fatalf("re-mark: %v", err)
	}

	firstUsedAt := func() map[string]*time.Time {
		t.Helper()
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodGet, base, ""))
		var list []struct {
			ID          string     `json:"id"`
			FirstUsedAt *time.Time `json:"first_used_at"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
			t.Fatalf("list: %v; body=%s", err, w.Body)
		}
		out := map[string]*time.Time{}
		for _, s := range list {
			out[s.ID] = s.FirstUsedAt
		}
		return out
	}
	listed := firstUsedAt()
	if got := listed[usedID]; got == nil || !got.Equal(firstUse) {
		t.Fatalf("first_used_at of the used sender ID = %v, want %v (the first mark, never moved)", got, firstUse)
	}
	if got, ok := listed[unusedID]; !ok || got != nil {
		t.Fatalf("first_used_at of the unused sender ID = %v (listed %v), want null", got, ok)
	}

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, base+"/"+usedID, ""))
	if w.Code != http.StatusConflict {
		t.Fatalf("delete used: status = %d, want 409; body=%s", w.Code, w.Body)
	}
	var problem struct{ Code, Message string }
	_ = json.Unmarshal(w.Body.Bytes(), &problem)
	if problem.Code != "conflict" || !strings.Contains(problem.Message, "disable it instead") {
		t.Fatalf("delete used: body = %s, want code conflict telling the operator to disable it instead", w.Body)
	}
	if _, still := firstUsedAt()[usedID]; !still {
		t.Fatal("the 409 deleted the used sender ID anyway")
	}

	for _, id := range []string{usedID, unusedID} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodPatch, base+"/"+id, `{"status":"disabled"}`))
		if w.Code != http.StatusOK {
			t.Fatalf("disable %s: status = %d, want 200; body=%s", id, w.Code, w.Body)
		}
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, base+"/"+unusedID, ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete unused: status = %d, want 204; body=%s", w.Code, w.Body)
	}
	if _, still := firstUsedAt()[unusedID]; still {
		t.Fatal("the unused sender ID survived its 204")
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, base+"/"+uuid.NewString(), ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete unknown: status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

// TestSenderIDTrafficCategoryIsDeclaredAndFilterable drives ADR-0020 §1 through the real repository: the
// category defaults to marketing, is posed at creation or by PATCH, survives a status-only PATCH, and
// filters the list.
func TestSenderIDTrafficCategoryIsDeclaredAndFilterable(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := t.Context()
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, newCustomerInput("sender-category-"+uuid.NewString()))
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{Customers: postgres.NewCustomerRepo(pool), SenderIDs: postgres.NewSenderIDRepo(pool)})
	base := "/v1/admin/customers/" + customer.ID.String() + "/sender-ids"

	type sender struct {
		ID              string `json:"id"`
		Address         string `json:"address"`
		TrafficCategory string `json:"traffic_category"`
	}
	call := func(method, path, body string, want int) []byte {
		t.Helper()
		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, method, path, body))
		if w.Code != want {
			t.Fatalf("%s %s: status = %d, want %d; body=%s", method, path, w.Code, want, w.Body)
		}
		return w.Body.Bytes()
	}
	create := func(body string) sender {
		t.Helper()
		var s sender
		_ = json.Unmarshal(call(http.MethodPost, base, body, http.StatusCreated), &s)
		return s
	}

	promo := create(`{"address":"PROMO"}`)
	if promo.TrafficCategory != "marketing" {
		t.Fatalf("undeclared category = %q, want marketing", promo.TrafficCategory)
	}
	bank := create(`{"address":"BANK","traffic_category":"otp"}`)
	if bank.TrafficCategory != "otp" {
		t.Fatalf("declared category = %q, want otp", bank.TrafficCategory)
	}
	var statusOnly sender
	_ = json.Unmarshal(call(http.MethodPatch, base+"/"+bank.ID, `{"status":"active"}`, http.StatusOK), &statusOnly)
	if statusOnly.TrafficCategory != "otp" {
		t.Fatalf("category after a status-only PATCH = %q, want otp untouched", statusOnly.TrafficCategory)
	}
	var patched sender
	_ = json.Unmarshal(call(http.MethodPatch, base+"/"+promo.ID, `{"traffic_category":"transactional"}`, http.StatusOK), &patched)
	if patched.TrafficCategory != "transactional" {
		t.Fatalf("patched category = %q, want transactional", patched.TrafficCategory)
	}
	call(http.MethodPost, base, `{"address":"BAD","traffic_category":"urgent"}`, http.StatusUnprocessableEntity)
	call(http.MethodPatch, base+"/"+promo.ID, `{"traffic_category":"urgent"}`, http.StatusUnprocessableEntity)
	call(http.MethodGet, base+"?traffic_category=urgent", "", http.StatusUnprocessableEntity)

	var otp []sender
	_ = json.Unmarshal(call(http.MethodGet, base+"?traffic_category=otp", "", http.StatusOK), &otp)
	if len(otp) != 1 || otp[0].Address != "BANK" {
		t.Fatalf("list ?traffic_category=otp = %+v, want only BANK", otp)
	}
	var all []sender
	_ = json.Unmarshal(call(http.MethodGet, base, "", http.StatusOK), &all)
	if len(all) != 2 {
		t.Fatalf("unfiltered list = %+v, want both sender IDs", all)
	}
}
