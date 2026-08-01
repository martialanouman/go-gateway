package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

func unroutedItem(dest string, reason cp.UnroutedReason, at time.Time) cp.UnroutedMO {
	conn := uuid.New()
	return cp.UnroutedMO{
		ID: uuid.New(), ReceivedAt: at, ConnectorID: &conn,
		SourceAddr: "22507000001", DestAddr: dest, SegmentCount: 1, Encoding: "gsm7", Reason: reason,
	}
}

// TestListUnroutedMOEmpty: an empty queue returns 200 with an empty data array and no next cursor.
func TestListUnroutedMOEmpty(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{UnroutedMO: &fakeUnroutedMOStore{}})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/mo/unrouted", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	data, _ := got["data"].([]any)
	if data == nil || len(data) != 0 {
		t.Errorf("data = %v, want empty array", got["data"])
	}
	if got["has_more"] != false {
		t.Errorf("has_more = %v, want false", got["has_more"])
	}
}

// TestListUnroutedMOMapsFields: an unrouted MO is projected onto a MessageSummary with direction mo,
// status rejected, the reason in error_code, and the nil UUID for the account/customer it never had.
func TestListUnroutedMOMapsFields(t *testing.T) {
	item := unroutedItem("36000", cp.UnroutedNoKeywordMatch, time.Now().UTC())
	api := newTestAPIWith(t, adminapi.Deps{UnroutedMO: &fakeUnroutedMOStore{items: []cp.UnroutedMO{item}}})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/mo/unrouted", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got struct {
		Data []map[string]any `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got.Data) != 1 {
		t.Fatalf("data = %d rows, want 1", len(got.Data))
	}
	row := got.Data[0]
	if row["message_id"] != item.ID.String() {
		t.Errorf("message_id = %v, want %s", row["message_id"], item.ID)
	}
	if row["direction"] != "mo" || row["status"] != "rejected" {
		t.Errorf("direction/status = %v/%v, want mo/rejected", row["direction"], row["status"])
	}
	if row["error_code"] != "no_keyword_match" {
		t.Errorf("error_code = %v, want no_keyword_match", row["error_code"])
	}
	// The subscriber side of an MO is the SOURCE; the destination is the operator's own inbound number and
	// stays readable. Without msisdn:reveal a bulk list must not hand out numbers in clear — masking the
	// trace while leaving this endpoint open would be a door beside the wall.
	if row["dest_addr"] != "36000" {
		t.Errorf("dest_addr = %v, want the inbound number unmasked", row["dest_addr"])
	}
	if row["source_addr"] == "22507000001" {
		t.Error("the subscriber number was returned in clear without msisdn:reveal")
	}
	if row["source_addr"] != "2250*****01" {
		t.Errorf("source_addr = %v, want a masked subscriber number", row["source_addr"])
	}
	nilUUID := uuid.Nil.String()
	if row["account_id"] != nilUUID || row["customer_id"] != nilUUID {
		t.Errorf("account/customer = %v/%v, want nil uuid (unrouted)", row["account_id"], row["customer_id"])
	}
}

// TestListUnroutedMOPaginates: with more rows than the limit, the page carries has_more and a
// next_cursor that fetches the following page.
func TestListUnroutedMOPaginates(t *testing.T) {
	now := time.Now().UTC()
	items := []cp.UnroutedMO{
		unroutedItem("1", cp.UnroutedUnknownNumber, now),
		unroutedItem("2", cp.UnroutedUnknownNumber, now.Add(-time.Second)),
		unroutedItem("3", cp.UnroutedUnknownNumber, now.Add(-2*time.Second)),
	}
	api := newTestAPIWith(t, adminapi.Deps{UnroutedMO: &fakeUnroutedMOStore{items: items}})

	// First page of 2.
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/mo/unrouted?limit=2", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("page1 status = %d; body=%s", w.Code, w.Body)
	}
	var page1 struct {
		Data       []map[string]any `json:"data"`
		NextCursor *string          `json:"next_cursor"`
		HasMore    bool             `json:"has_more"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &page1)
	if len(page1.Data) != 2 || !page1.HasMore || page1.NextCursor == nil {
		t.Fatalf("page1 = %d rows, has_more=%v, cursor=%v; want 2 / true / set", len(page1.Data), page1.HasMore, page1.NextCursor)
	}

	// Next page via the cursor: the last row, no further page.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/mo/unrouted?limit=2&cursor="+*page1.NextCursor, ""))
	if w.Code != http.StatusOK {
		t.Fatalf("page2 status = %d; body=%s", w.Code, w.Body)
	}
	var page2 struct {
		Data    []map[string]any `json:"data"`
		HasMore bool             `json:"has_more"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &page2)
	if len(page2.Data) != 1 || page2.HasMore {
		t.Errorf("page2 = %d rows, has_more=%v; want 1 / false", len(page2.Data), page2.HasMore)
	}
	if page2.Data[0]["dest_addr"] != "3" {
		t.Errorf("page2 first dest = %v, want 3 (the oldest)", page2.Data[0]["dest_addr"])
	}
}

// TestListUnroutedMOBadCursorIs422: a malformed cursor is a 422.
func TestListUnroutedMOBadCursorIs422(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{UnroutedMO: &fakeUnroutedMOStore{}})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/mo/unrouted?cursor=notacursor", ""))
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}
