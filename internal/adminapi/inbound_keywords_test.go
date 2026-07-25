package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// keywordsPath is the collection endpoint for one shared inbound number's keywords.
func keywordsPath(numberID string) string {
	return "/v1/admin/inbound-numbers/" + numberID + "/keywords"
}

// TestCreateInboundKeywordReturns201 walks the happy path to the fake, checking the DDL defaults come
// back (match_type 'prefix', priority 0, status 'active') and the path id becomes inbound_number_id.
func TestCreateInboundKeywordReturns201(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberID := uuid.New().String()
	account := uuid.New().String()

	w := httptest.NewRecorder()
	body := `{"keyword":"INFO","account_id":"` + account + `"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, keywordsPath(numberID), body))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["keyword"] != "INFO" {
		t.Errorf("keyword = %v, want INFO", got["keyword"])
	}
	if got["match_type"] != "prefix" {
		t.Errorf("match_type = %v, want prefix (the DDL default)", got["match_type"])
	}
	if got["status"] != "active" {
		t.Errorf("status = %v, want active", got["status"])
	}
	if got["inbound_number_id"] != numberID {
		t.Errorf("inbound_number_id = %v, want the path id %s", got["inbound_number_id"], numberID)
	}
	if got["account_id"] != account {
		t.Errorf("account_id = %v, want %s", got["account_id"], account)
	}
	if p, ok := got["priority"].(float64); !ok || p != 0 {
		t.Errorf("priority = %v, want 0 (default)", got["priority"])
	}
}

// TestCreateInboundKeywordKeepsExplicitMatchTypeAndPriority: an explicit match_type and priority are
// passed through, not overwritten by the defaults.
func TestCreateInboundKeywordKeepsExplicitMatchTypeAndPriority(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberID := uuid.New().String()

	w := httptest.NewRecorder()
	body := `{"keyword":"STOP","match_type":"exact","priority":5,"account_id":"` + uuid.New().String() + `"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, keywordsPath(numberID), body))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["match_type"] != "exact" {
		t.Errorf("match_type = %v, want exact", got["match_type"])
	}
	if p, _ := got["priority"].(float64); p != 5 {
		t.Errorf("priority = %v, want 5", got["priority"])
	}
}

// TestCreateInboundKeywordConflictIs409: the store's ErrConflict surfaces as a 409 with the flat
// error code.
func TestCreateInboundKeywordConflictIs409(t *testing.T) {
	store := newFakeInboundKeywordStore()
	store.createErr = errs.ErrConflict
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: store})

	w := httptest.NewRecorder()
	body := `{"keyword":"INFO","account_id":"` + uuid.New().String() + `"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, keywordsPath(uuid.New().String()), body))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", got["code"])
	}
}

// TestCreateInboundKeywordPriorityOutOfRangeIs422: a priority above int32 is rejected before it can
// wrap silently on the way to the column.
func TestCreateInboundKeywordPriorityOutOfRangeIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})

	w := httptest.NewRecorder()
	body := `{"keyword":"INFO","priority":2147483648,"account_id":"` + uuid.New().String() + `"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, keywordsPath(uuid.New().String()), body))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestCreateInboundKeywordInvalidAccountIDIs422: account_id is a required uuid, so a malformed one is
// rejected as a 422 (huma format validation) before the store is touched.
func TestCreateInboundKeywordInvalidAccountIDIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberID := uuid.New().String()

	w := httptest.NewRecorder()
	body := `{"keyword":"INFO","account_id":"not-a-uuid"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, keywordsPath(numberID), body))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestListInboundKeywordsIsScopedToTheNumber: list returns only the keywords of the number in the
// path, not those of a different number.
func TestListInboundKeywordsIsScopedToTheNumber(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberA := uuid.New().String()
	numberB := uuid.New().String()

	create := func(numberID, kw string) {
		w := httptest.NewRecorder()
		body := `{"keyword":"` + kw + `","account_id":"` + uuid.New().String() + `"}`
		api.ServeHTTP(w, authed(t, http.MethodPost, keywordsPath(numberID), body))
		if w.Code != http.StatusCreated {
			t.Fatalf("seed keyword %s: status %d; body=%s", kw, w.Code, w.Body)
		}
	}
	create(numberA, "INFO")
	create(numberA, "HELP")
	create(numberB, "STOP")

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, keywordsPath(numberA), ""))
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got) != 2 {
		t.Fatalf("list returned %d keywords, want 2 (scoped to number A): %s", len(got), w.Body)
	}
	for _, kw := range got {
		if kw["inbound_number_id"] != numberA {
			t.Errorf("keyword scoped to %v, want %s", kw["inbound_number_id"], numberA)
		}
	}
}

// TestUpdateInboundKeywordChangesFields: a partial update reflects the new keyword, priority and
// status.
func TestUpdateInboundKeywordChangesFields(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberID := uuid.New().String()
	keywordID := seedInboundKeyword(t, api, numberID)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, keywordsPath(numberID)+"/"+keywordID,
		`{"keyword":"HELP","priority":9,"status":"disabled"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["keyword"] != "HELP" {
		t.Errorf("keyword = %v, want HELP", got["keyword"])
	}
	if p, _ := got["priority"].(float64); p != 9 {
		t.Errorf("priority = %v, want 9", got["priority"])
	}
	if got["status"] != "disabled" {
		t.Errorf("status = %v, want disabled", got["status"])
	}
}

// TestUpdateInboundKeywordChangesAccountAndMatchType covers the two patch fields the field-change
// test does not: account_id (a re-target) and match_type.
func TestUpdateInboundKeywordChangesAccountAndMatchType(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberID := uuid.New().String()
	keywordID := seedInboundKeyword(t, api, numberID)
	newAccount := uuid.New().String()

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, keywordsPath(numberID)+"/"+keywordID,
		`{"account_id":"`+newAccount+`","match_type":"regex"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["account_id"] != newAccount {
		t.Errorf("account_id = %v, want %s", got["account_id"], newAccount)
	}
	if got["match_type"] != "regex" {
		t.Errorf("match_type = %v, want regex", got["match_type"])
	}
}

// TestUpdateInboundKeywordUnknownIs404: patching a keyword that does not belong to the number is a
// 404.
func TestUpdateInboundKeywordUnknownIs404(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberID := uuid.New().String()

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, keywordsPath(numberID)+"/"+uuid.New().String(),
		`{"keyword":"HELP"}`))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

// TestDeleteInboundKeywordReturns204 then 404 on a second delete of the same id.
func TestDeleteInboundKeywordReturns204(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{InboundKeywords: newFakeInboundKeywordStore()})
	numberID := uuid.New().String()
	keywordID := seedInboundKeyword(t, api, numberID)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, keywordsPath(numberID)+"/"+keywordID, ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204; body=%s", w.Code, w.Body)
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, keywordsPath(numberID)+"/"+keywordID, ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("second delete status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

// seedInboundKeyword creates one keyword under numberID and returns its id.
func seedInboundKeyword(t *testing.T, api http.Handler, numberID string) string {
	t.Helper()
	w := httptest.NewRecorder()
	body := `{"keyword":"INFO","account_id":"` + uuid.New().String() + `"}`
	api.ServeHTTP(w, authed(t, http.MethodPost, keywordsPath(numberID), body))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed keyword status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	id, _ := got["id"].(string)
	if id == "" {
		t.Fatalf("seed keyword returned no id: %s", w.Body)
	}
	return id
}
