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
)

type auditLogPage struct {
	Data []struct {
		ID          string  `json:"id"`
		Operator    string  `json:"operator"`
		OperationID string  `json:"operation_id"`
		Method      string  `json:"method"`
		Target      string  `json:"target"`
		RequestID   *string `json:"request_id"`
		Status      *int    `json:"status"`
		At          string  `json:"at"`
		FinishedAt  *string `json:"finished_at"`
	} `json:"data"`
	NextCursor *string `json:"next_cursor"`
	HasMore    bool    `json:"has_more"`
}

func getAuditLog(t *testing.T, store *fakeAuditLog, scopes, query string) (int, auditLogPage) {
	t.Helper()
	w := httptest.NewRecorder()
	newTestAPIWithScopes(t, adminapi.Deps{AuditLog: store}, scopes).
		ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/audit-log"+query, ""))
	var page auditLogPage
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode: %v; body=%s", err, w.Body)
		}
	}
	return w.Code, page
}

// TestListAuditLogRequiresAuditRead: the trail is its own right — admin:read does not open it.
func TestListAuditLogRequiresAuditRead(t *testing.T) {
	if code, _ := getAuditLog(t, newFakeAuditLog(), "admin:read|admin:write", ""); code != http.StatusForbidden {
		t.Errorf("status = %d without audit:read, want 403", code)
	}
}

// TestListAuditLogMapsRowsAndPages: every column reaches its own field, a replay row passes the response
// schema, and a full page hands back a cursor that decodes to its last row.
func TestListAuditLogMapsRowsAndPages(t *testing.T) {
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	finished := at.Add(time.Second)
	status, reqID := 201, "req-1"
	closed := cp.AuditEntry{ID: uuid.New(), Operator: "tok_0123456789abcdef", OperationID: "create-customer",
		Method: "POST", Target: "/v1/admin/customers", RequestID: &reqID, Status: &status, At: at, FinishedAt: &finished}
	replay := cp.AuditEntry{ID: uuid.New(), Operator: "declared:alice", OperationID: "mt-replay",
		Method: "REPLAY", Target: "mt.dead-letter", At: at.Add(-time.Minute)}
	store := newFakeAuditLog()
	store.entries = []cp.AuditEntry{closed, replay, {ID: uuid.New(), At: at.Add(-time.Hour)}}

	code, page := getAuditLog(t, store, "audit:read", "?limit=2")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if store.listLimit != 3 {
		t.Errorf("store asked for %d rows, want limit+1 = 3", store.listLimit)
	}
	if len(page.Data) != 2 || !page.HasMore || page.NextCursor == nil {
		t.Fatalf("page = %d rows, has_more %v, cursor %v — want 2, true and a cursor", len(page.Data), page.HasMore, page.NextCursor)
	}
	got := page.Data[0]
	if got.ID != closed.ID.String() || got.Operator != closed.Operator || got.OperationID != "create-customer" ||
		got.Method != "POST" || got.Target != "/v1/admin/customers" || got.RequestID == nil || *got.RequestID != "req-1" ||
		got.Status == nil || *got.Status != 201 || got.FinishedAt == nil || got.At != "2026-09-01T12:00:00Z" {
		t.Errorf("closed row = %+v, want every column in its own field", got)
	}
	r := page.Data[1]
	if r.Operator != "declared:alice" || r.Method != "REPLAY" || r.Target != "mt.dead-letter" ||
		r.Status != nil || r.RequestID != nil || r.FinishedAt != nil {
		t.Errorf("replay row = %+v, want it served as recorded, its outcome null", r)
	}

	if code, _ = getAuditLog(t, store, "audit:read", "?cursor="+*page.NextCursor); code != http.StatusOK {
		t.Fatalf("next page status = %d", code)
	}
	if store.listAfter == nil || store.listAfter.ID != replay.ID || !store.listAfter.At.Equal(replay.At) {
		t.Errorf("cursor decoded to %+v, want the last row's (at, id)", store.listAfter)
	}
}

// TestListAuditLogPassesItsFilters: operator, from_date and to_date reach the store.
func TestListAuditLogPassesItsFilters(t *testing.T) {
	store := newFakeAuditLog()
	code, _ := getAuditLog(t, store, "audit:read",
		"?operator=declared:alice&from_date=2026-09-01T00:00:00Z&to_date=2026-09-02T00:00:00Z")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	f := store.listFilter
	if f.Operator != "declared:alice" || f.From == nil || !f.From.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) ||
		f.To == nil || !f.To.Equal(time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("filter = %+v, want operator and both bounds passed through", f)
	}
}

// TestListAuditLogRefusesAnInvertedWindow: 422 like search-messages, not an empty page that reads as "nothing happened".
func TestListAuditLogRefusesAnInvertedWindow(t *testing.T) {
	code, _ := getAuditLog(t, newFakeAuditLog(), "audit:read", "?from_date=2026-09-02T00:00:00Z&to_date=2026-09-01T00:00:00Z")
	if code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", code)
	}
}

// TestListAuditLogRefusesAMalformedCursor: 422, not a first page served as if nothing was asked.
func TestListAuditLogRefusesAMalformedCursor(t *testing.T) {
	if code, _ := getAuditLog(t, newFakeAuditLog(), "audit:read", "?cursor=not-a-cursor"); code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", code)
	}
}

// TestListAuditLogMasksTheNumberInATarget: an exact-route write records the subscriber number in its path.
// Reading the trail must not hand it out in clear without msisdn:reveal — and reading it with that scope
// reveals a number, so the read enters the trail itself.
func TestListAuditLogMasksTheNumberInATarget(t *testing.T) {
	const msisdn = "22507123456"
	// An escaped slash decodes into the recorded path: the whole remainder is the number, not its last segment.
	const slashed = "225/07123456"
	entries := []cp.AuditEntry{
		{ID: uuid.New(), OperationID: "delete-exact-route", Method: "DELETE", Target: "/v1/admin/exact-routes/" + msisdn, At: time.Now()},
		{ID: uuid.New(), OperationID: "update-exact-route", Method: "PATCH", Target: "/v1/admin/exact-routes/" + msisdn, At: time.Now()},
		{ID: uuid.New(), OperationID: "delete-exact-route", Method: "DELETE", Target: "/v1/admin/exact-routes/" + slashed, At: time.Now()},
		{ID: uuid.New(), OperationID: "import-exact-routes", Method: "POST", Target: "/v1/admin/exact-routes/import", At: time.Now()},
	}

	store := newFakeAuditLog()
	store.entries = entries
	_, page := getAuditLog(t, store, "audit:read", "")
	for _, e := range page.Data[:2] {
		if e.Target == "/v1/admin/exact-routes/"+msisdn || len(e.Target) != len("/v1/admin/exact-routes/"+msisdn) {
			t.Errorf("%s target = %q, want the number masked in place", e.OperationID, e.Target)
		}
	}
	if strings.Contains(page.Data[2].Target, "0712") {
		t.Errorf("target = %q, want the number masked past its first slash", page.Data[2].Target)
	}
	if page.Data[3].Target != "/v1/admin/exact-routes/import" {
		t.Errorf("import target = %q, want it untouched: it names no number", page.Data[3].Target)
	}
	if intents, _, _ := store.snapshot(); len(intents) != 0 {
		t.Errorf("a masked read was recorded %d times, want none: it reveals nothing", len(intents))
	}

	store = newFakeAuditLog()
	store.entries = entries
	_, page = getAuditLog(t, store, "audit:read|msisdn:reveal", "")
	for _, e := range page.Data[:2] {
		if e.Target != "/v1/admin/exact-routes/"+msisdn {
			t.Errorf("%s target = %q with msisdn:reveal, want the number in clear", e.OperationID, e.Target)
		}
	}
	if intents, _, _ := store.snapshot(); len(intents) != 1 || intents[0].OperationID != "list-audit-log" {
		t.Errorf("recorded intents = %+v, want the revealing read itself in the trail", intents)
	}
}
