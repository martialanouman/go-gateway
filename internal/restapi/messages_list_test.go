package restapi_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// sampleCDRRow builds a minimal MT CDR row for list projection tests.
func sampleCDRRow(submittedAt time.Time) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID: uuid.New(), TraceID: uuid.New(), Direction: clickhouse.DirectionMT,
		SourceAddr: "ACME", DestAddr: "2250700000000", Status: clickhouse.StatusEnroute,
		SegmentCount: 1, Encoding: clickhouse.EncodingGSM7, SubmittedAt: submittedAt,
	}
}

func TestListMessagesRequiresAuth(t *testing.T) {
	h := newHarness(t, fakePrincipals{found: false}, &fakeCDRReader{})
	resp := h.do(t, http.MethodGet, "/v1/messages", "", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d want 401", resp.StatusCode)
	}
}

func TestListMessagesReturnsPage(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	rows := []clickhouse.CDRRow{sampleCDRRow(now), sampleCDRRow(now.Add(-time.Second))}
	reader := &fakeCDRReader{listRows: rows}
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, reader)

	resp := h.do(t, http.MethodGet, "/v1/messages", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}

	var page restapi.MessagePage
	decode(t, resp, &page)
	if len(page.Data) != 2 {
		t.Fatalf("data length: got %d want 2", len(page.Data))
	}
	if page.Data[0].ID != rows[0].MessageID.String() {
		t.Errorf("first message id: got %q", page.Data[0].ID)
	}
	if page.Data[0].Status != "enroute" {
		t.Errorf("status: got %q want enroute", page.Data[0].Status)
	}
	// A short page (fewer than the fetched limit+1) has no further page.
	if page.HasMore {
		t.Error("has_more should be false on a short page")
	}
	if page.NextCursor != nil {
		t.Errorf("next_cursor should be null on the last page, got %q", *page.NextCursor)
	}
	// Default limit is 50, so the handler fetches 51 to detect a further page.
	if reader.gotLimit != 51 {
		t.Errorf("fetched limit: got %d want 51 (default 50 + 1)", reader.gotLimit)
	}
}

func TestListMessagesEmptyPageIsEmptyArray(t *testing.T) {
	reader := &fakeCDRReader{listRows: nil}
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, reader)

	resp := h.do(t, http.MethodGet, "/v1/messages", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}

	// data must serialize as [] not null (the contract lists it required, non-nullable).
	var raw map[string]json.RawMessage
	decode(t, resp, &raw)
	if got := string(raw["data"]); got != "[]" {
		t.Errorf("data: got %q want []", got)
	}
	if got := string(raw["next_cursor"]); got != "null" {
		t.Errorf("next_cursor: got %q want null", got)
	}
	if got := string(raw["has_more"]); got != "false" {
		t.Errorf("has_more: got %q want false", got)
	}
}

// TestListMessagesHasMoreAndCursor covers the pagination boundary: when List returns limit+1 rows,
// the handler reports has_more=true, trims to limit, and returns a next_cursor that round-trips into
// the next page's keyset filter.
func TestListMessagesHasMoreAndCursor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	// limit=2 → the handler fetches 3; three rows means a further page exists.
	rows := []clickhouse.CDRRow{
		sampleCDRRow(now),
		sampleCDRRow(now.Add(-time.Second)),
		sampleCDRRow(now.Add(-2 * time.Second)),
	}
	reader := &fakeCDRReader{listRows: rows}
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, reader)

	resp := h.do(t, http.MethodGet, "/v1/messages?limit=2", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	if reader.gotLimit != 3 {
		t.Errorf("fetched limit: got %d want 3 (limit 2 + 1)", reader.gotLimit)
	}

	var page restapi.MessagePage
	decode(t, resp, &page)
	if len(page.Data) != 2 {
		t.Fatalf("data length: got %d want 2 (trimmed to limit)", len(page.Data))
	}
	if !page.HasMore {
		t.Error("has_more should be true when a further page exists")
	}
	if page.NextCursor == nil {
		t.Fatal("next_cursor should be set when has_more is true")
	}
	// The cursor points at the last returned row (index 1), so the next page starts strictly after it.
	next := *page.NextCursor

	// Follow the cursor: the handler must decode it into the keyset filter passed to List.
	resp2 := h.do(t, http.MethodGet, "/v1/messages?limit=2&cursor="+next, "sgw_key", nil)
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second page status: got %d want 200", resp2.StatusCode)
	}
	if reader.gotFilter.After == nil {
		t.Fatal("cursor was not decoded into the keyset filter")
	}
	if reader.gotFilter.After.MessageID != rows[1].MessageID {
		t.Errorf("cursor keyset message id: got %s want %s", reader.gotFilter.After.MessageID, rows[1].MessageID)
	}
	if !reader.gotFilter.After.SubmittedAt.Equal(rows[1].SubmittedAt) {
		t.Errorf("cursor keyset submitted_at: got %s want %s", reader.gotFilter.After.SubmittedAt, rows[1].SubmittedAt)
	}
}

func TestListMessagesInvalidCursorIs422(t *testing.T) {
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, &fakeCDRReader{})
	resp := h.do(t, http.MethodGet, "/v1/messages?cursor=not-a-valid-cursor", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("invalid cursor: got %d want 422", resp.StatusCode)
	}
}

// TestListMessagesFiltersPassedThrough asserts the contract's query filters reach the storage
// filter (the scope itself always comes from the principal, never the query).
func TestListMessagesFiltersPassedThrough(t *testing.T) {
	reader := &fakeCDRReader{}
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, reader)

	resp := h.do(t, http.MethodGet,
		"/v1/messages?status=delivered&direction=mt&from_date=2026-01-01T00:00:00Z&to_date=2026-02-01T00:00:00Z",
		"sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}

	f := reader.gotFilter
	if f.Status == nil || *f.Status != clickhouse.StatusDelivered {
		t.Errorf("status filter: got %v", f.Status)
	}
	if f.Direction == nil || *f.Direction != clickhouse.DirectionMT {
		t.Errorf("direction filter: got %v", f.Direction)
	}
	if f.FromDate == nil || !f.FromDate.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("from_date filter: got %v", f.FromDate)
	}
	if f.ToDate == nil || !f.ToDate.Equal(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("to_date filter: got %v", f.ToDate)
	}
}

func TestListMessagesRejectsBadStatusAndLimit(t *testing.T) {
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, &fakeCDRReader{})
	cases := []string{
		"/v1/messages?status=bogus",
		"/v1/messages?direction=xx",
		"/v1/messages?limit=0",
		"/v1/messages?limit=201",
	}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			resp := h.do(t, http.MethodGet, path, "sgw_key", nil)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusUnprocessableEntity {
				t.Errorf("%s: got %d want 422", path, resp.StatusCode)
			}
		})
	}
}

// TestListMessagesReaderErrorReturns500 confirms a storage failure is a clean 500, never a partial
// body.
func TestListMessagesReaderErrorReturns500(t *testing.T) {
	reader := &fakeCDRReader{listErr: errors.New("boom")}
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, reader)
	resp := h.do(t, http.MethodGet, "/v1/messages", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d want 500", resp.StatusCode)
	}
}

// TestListMessagesLeaksNoBody guards invariant (a): the list projection must never surface the
// message body or stored content, at any nesting depth.
func TestListMessagesLeaksNoBody(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	reader := &fakeCDRReader{listRows: []clickhouse.CDRRow{sampleCDRRow(now)}}
	h := newHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, reader)

	resp := h.do(t, http.MethodGet, "/v1/messages", "sgw_key", nil)
	defer func() { _ = resp.Body.Close() }()

	var raw any
	decode(t, resp, &raw)
	for _, key := range collectKeys(raw) {
		lower := strings.ToLower(key)
		for _, forbidden := range []string{"body", "text", "content", "ciphertext", "payload"} {
			if strings.Contains(lower, forbidden) {
				t.Errorf("list view leaks a body-like field: %q", key)
			}
		}
	}
}
