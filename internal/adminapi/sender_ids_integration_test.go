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

// TestSenderIDRateLimitIsSetReadAndRemoved drives the sender ID's own limit (ADR-0021 §3) through the real
// repository: it is read in the list and after a PATCH, defaults its burst to the rate, is scoped to the
// customer, and leaves no orphan rate_limits row behind a deleted sender ID or customer.
func TestSenderIDRateLimitIsSetReadAndRemoved(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := t.Context()
	customers := postgres.NewCustomerRepo(pool)
	customer, err := customers.Create(ctx, newCustomerInput("sender-limit-"+uuid.NewString()))
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	other, err := customers.Create(ctx, newCustomerInput("sender-limit-other-"+uuid.NewString()))
	if err != nil {
		t.Fatalf("create other customer: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{Customers: customers, SenderIDs: postgres.NewSenderIDRepo(pool)})
	base := "/v1/admin/customers/" + customer.ID.String() + "/sender-ids"

	type limit struct {
		MaxPerSec     int `json:"max_per_sec"`
		BurstCapacity int `json:"burst_capacity"`
	}
	type sender struct {
		ID        string `json:"id"`
		Address   string `json:"address"`
		RateLimit *limit `json:"rate_limit"`
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
	listed := func() map[string]*limit {
		t.Helper()
		var list []sender
		_ = json.Unmarshal(call(http.MethodGet, base, "", http.StatusOK), &list)
		out := map[string]*limit{}
		for _, s := range list {
			out[s.Address] = s.RateLimit
		}
		return out
	}
	var otp, promo sender
	_ = json.Unmarshal(call(http.MethodPost, base, `{"address":"OTP"}`, http.StatusCreated), &otp)
	_ = json.Unmarshal(call(http.MethodPost, base, `{"address":"PROMO"}`, http.StatusCreated), &promo)
	if otp.RateLimit != nil {
		t.Fatalf("a new sender ID has rate_limit %+v, want null", otp.RateLimit)
	}

	var set sender
	_ = json.Unmarshal(call(http.MethodPut, base+"/"+otp.ID+"/rate-limit", `{"max_per_sec":50}`, http.StatusOK), &set)
	if set.RateLimit == nil || *set.RateLimit != (limit{MaxPerSec: 50, BurstCapacity: 50}) {
		t.Fatalf("PUT without a burst = %+v, want {50 50}", set.RateLimit)
	}
	call(http.MethodPut, base+"/"+promo.ID+"/rate-limit", `{"max_per_sec":5,"burst_capacity":20}`, http.StatusOK)
	var patched sender
	_ = json.Unmarshal(call(http.MethodPatch, base+"/"+promo.ID, `{"status":"active"}`, http.StatusOK), &patched)
	if patched.RateLimit == nil || *patched.RateLimit != (limit{5, 20}) {
		t.Fatalf("rate_limit in a PATCH response = %+v, want {5 20}", patched.RateLimit)
	}
	got := listed()
	if got["OTP"] == nil || *got["OTP"] != (limit{50, 50}) || got["PROMO"] == nil || *got["PROMO"] != (limit{5, 20}) {
		t.Fatalf("listed limits = OTP %+v PROMO %+v, want {50 50} and {5 20}", got["OTP"], got["PROMO"])
	}

	call(http.MethodPut, base+"/"+otp.ID+"/rate-limit", `{"max_per_sec":0}`, http.StatusUnprocessableEntity)
	call(http.MethodPut, base+"/"+otp.ID+"/rate-limit", `{"max_per_sec":5,"burst_capacity":0}`, http.StatusUnprocessableEntity)
	otherBase := "/v1/admin/customers/" + other.ID.String() + "/sender-ids/"
	call(http.MethodPut, otherBase+otp.ID+"/rate-limit", `{"max_per_sec":1}`, http.StatusNotFound)
	call(http.MethodDelete, otherBase+otp.ID+"/rate-limit", "", http.StatusNotFound)
	call(http.MethodPut, base+"/"+uuid.NewString()+"/rate-limit", `{"max_per_sec":1}`, http.StatusNotFound)
	if got := listed(); got["OTP"] == nil || *got["OTP"] != (limit{50, 50}) {
		t.Fatalf("after another customer's PUT, OTP = %+v, want {50 50} untouched", got["OTP"])
	}

	call(http.MethodDelete, base+"/"+otp.ID+"/rate-limit", "", http.StatusNoContent)
	call(http.MethodDelete, base+"/"+otp.ID+"/rate-limit", "", http.StatusNoContent)
	if got := listed(); got["OTP"] != nil || got["PROMO"] == nil || *got["PROMO"] != (limit{5, 20}) {
		t.Fatalf("after DELETE: OTP %+v PROMO %+v, want OTP null and PROMO {5 20}", got["OTP"], got["PROMO"])
	}

	orphans := func(id string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.rate_limits
			WHERE entity_type = 'sender_id' AND entity_id = $1`, id).Scan(&n); err != nil {
			t.Fatalf("count rate limits: %v", err)
		}
		return n
	}
	call(http.MethodPut, base+"/"+otp.ID+"/rate-limit", `{"max_per_sec":1}`, http.StatusOK)
	if n := orphans(otp.ID); n != 1 {
		t.Fatalf("before deleting the sender ID: %d rate_limits rows, want 1", n)
	}
	call(http.MethodDelete, base+"/"+otp.ID, "", http.StatusNoContent)
	if n := orphans(otp.ID); n != 0 {
		t.Fatalf("deleting the sender ID left %d rate_limits rows", n)
	}
	if err := customers.Delete(ctx, customer.ID); err != nil {
		t.Fatalf("delete customer: %v", err)
	}
	if n := orphans(promo.ID); n != 0 {
		t.Fatalf("deleting the customer left %d rate_limits rows for its sender IDs", n)
	}
}
