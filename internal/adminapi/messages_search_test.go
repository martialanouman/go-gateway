package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// --- fakes ---

// fakeSearchStore records the filter it was handed: most assertions here are about what the handler
// asks the store, not about what ClickHouse would answer (that is the integration test's job).
type fakeSearchStore struct {
	rows  []clickhouse.CDRRow
	got   clickhouse.CDRSearchFilter
	limit int
	calls int
}

func (f *fakeSearchStore) Search(_ context.Context, filter clickhouse.CDRSearchFilter, limit int) ([]clickhouse.CDRRow, error) {
	f.got, f.limit, f.calls = filter, limit, f.calls+1
	return f.rows, nil
}

// fakeSearchCustomers answers the group expansion. It records the filter so a test can prove the
// group was resolved rather than passed through.
type fakeSearchCustomers struct {
	items []cp.Customer
	got   cp.CustomerFilter
}

func (f *fakeSearchCustomers) Create(context.Context, cp.NewCustomer) (cp.Customer, error) {
	return cp.Customer{}, nil
}
func (f *fakeSearchCustomers) Get(context.Context, uuid.UUID) (cp.Customer, error) {
	return cp.Customer{}, nil
}
func (f *fakeSearchCustomers) List(_ context.Context, filter cp.CustomerFilter) (cp.Page[cp.Customer], error) {
	f.got = filter
	return cp.Page[cp.Customer]{Items: f.items}, nil
}
func (f *fakeSearchCustomers) Update(context.Context, uuid.UUID, cp.CustomerPatch) (cp.Customer, error) {
	return cp.Customer{}, nil
}
func (f *fakeSearchCustomers) Delete(context.Context, uuid.UUID) error { return nil }
func (f *fakeSearchCustomers) Suspend(context.Context, uuid.UUID) (cp.Customer, error) {
	return cp.Customer{}, nil
}

// --- harness ---

type messageSummaryBody struct {
	Data []struct {
		MessageID  string `json:"message_id"`
		Direction  string `json:"direction"`
		SourceAddr string `json:"source_addr"`
		DestAddr   string `json:"dest_addr"`
		Status     string `json:"status"`
	} `json:"data"`
	NextCursor *string `json:"next_cursor"`
	HasMore    bool    `json:"has_more"`
}

// searchWindow is a valid, in-bounds window every test starts from.
func searchWindow() url.Values {
	now := time.Now().UTC()
	return url.Values{
		"from_date": {now.Add(-24 * time.Hour).Format(time.RFC3339)},
		"to_date":   {now.Format(time.RFC3339)},
	}
}

func doSearch(t *testing.T, deps adminapi.Deps, scopes string, q url.Values) (int, messageSummaryBody, string) {
	t.Helper()

	handler := newTestAPIWithScopes(t, deps, scopes)
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/messages/search?"+q.Encode(), http.NoBody)
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var body messageSummaryBody
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v (%s)", err, rec.Body)
		}
	}
	return rec.Code, body, rec.Body.String()
}

// searchRowFixture is an MT to a subscriber, carrying a sealed body so the leak assertion can fail.
func searchRowFixture(dest string) clickhouse.CDRRow {
	ciphertext := secretBody
	keyID := uuid.New()
	return clickhouse.CDRRow{
		MessageID:         uuid.New(),
		TraceID:           uuid.New(),
		AccountID:         uuid.New(),
		CustomerID:        uuid.New(),
		Direction:         clickhouse.DirectionMT,
		SourceAddr:        "GATEWAY",
		DestAddr:          dest,
		SubmittedAt:       time.Now().UTC().Add(-time.Hour),
		Status:            clickhouse.StatusDelivered,
		SegmentCount:      1,
		Encoding:          clickhouse.EncodingGSM7,
		ContentCiphertext: &ciphertext,
		ContentKeyID:      &keyID,
	}
}

// --- masking ---

// TestSearchMasksMSISDNWithoutTheRevealScope is the step's headline criterion. The MT's subscriber is
// its DESTINATION; the source is a sender ID, which identifies no one and stays readable.
func TestSearchMasksMSISDNWithoutTheRevealScope(t *testing.T) {
	store := &fakeSearchStore{rows: []clickhouse.CDRRow{searchRowFixture("33612345678")}}

	code, body, _ := doSearch(t, adminapi.Deps{MessageSearch: store}, "admin:read", searchWindow())
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if len(body.Data) != 1 {
		t.Fatalf("got %d rows, want 1", len(body.Data))
	}
	if got := body.Data[0].DestAddr; got != "3361*****78" {
		t.Errorf("dest_addr = %q, want it masked", got)
	}
	if got := body.Data[0].SourceAddr; got != "GATEWAY" {
		t.Errorf("source_addr = %q, want the sender ID untouched", got)
	}
}

func TestSearchRevealsMSISDNWithTheScope(t *testing.T) {
	store := &fakeSearchStore{rows: []clickhouse.CDRRow{searchRowFixture("33612345678")}}

	code, body, _ := doSearch(t, adminapi.Deps{MessageSearch: store}, "admin:read|msisdn:reveal", searchWindow())
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := body.Data[0].DestAddr; got != "33612345678" {
		t.Errorf("dest_addr = %q, want it revealed", got)
	}
}

// TestSearchNeverSerialisesContent is invariant (a) at this endpoint: the store returns rows whose
// content columns are populated, and none of it may reach the response.
func TestSearchNeverSerialisesContent(t *testing.T) {
	store := &fakeSearchStore{rows: []clickhouse.CDRRow{searchRowFixture("33612345678")}}

	code, _, raw := doSearch(t, adminapi.Deps{MessageSearch: store}, "admin:read|msisdn:reveal", searchWindow())
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if strings.Contains(raw, secretBody) {
		t.Errorf("the response carries the message body: %s", raw)
	}
	if strings.Contains(raw, "ciphertext") || strings.Contains(raw, "content_key") {
		t.Errorf("the response exposes a content column: %s", raw)
	}
}

// --- filters ---

func TestSearchPassesEveryFilterToTheStore(t *testing.T) {
	store := &fakeSearchStore{}
	accountID, customerID, traceID := uuid.New(), uuid.New(), uuid.New()

	q := searchWindow()
	q.Set("accountId", accountID.String())
	q.Set("customerId", customerID.String())
	q.Set("traceId", traceID.String())
	q.Set("status", "delivered")
	q.Set("direction", "mt")
	q.Set("msisdn", "+33 6 12 34 56 78")

	if code, _, raw := doSearch(t, adminapi.Deps{MessageSearch: store}, "admin:read", q); code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", code, raw)
	}
	got := store.got
	if got.AccountID == nil || *got.AccountID != accountID {
		t.Errorf("AccountID = %v, want %s", got.AccountID, accountID)
	}
	if len(got.CustomerIDs) != 1 || got.CustomerIDs[0] != customerID {
		t.Errorf("CustomerIDs = %v, want [%s]", got.CustomerIDs, customerID)
	}
	if got.TraceID == nil || *got.TraceID != traceID {
		t.Errorf("TraceID = %v, want %s", got.TraceID, traceID)
	}
	if got.Status == nil || *got.Status != clickhouse.StatusDelivered {
		t.Errorf("Status = %v, want delivered", got.Status)
	}
	if got.Direction == nil || *got.Direction != clickhouse.DirectionMT {
		t.Errorf("Direction = %v, want mt", got.Direction)
	}
	// The number reaches the store NORMALISED: the CDR stores E.164, so a spaced or +-prefixed input
	// must not be compared literally.
	if got.MSISDN == nil || *got.MSISDN != "33612345678" {
		t.Errorf("MSISDN = %v, want the normalised 33612345678", got.MSISDN)
	}
}

// TestSearchResolvesAGroupIntoItsCustomers proves the group is expanded here rather than pushed to a
// CDR column: the CDR has none, and a customer's group can change after the message was sent.
func TestSearchResolvesAGroupIntoItsCustomers(t *testing.T) {
	groupID := uuid.New()
	first, second := uuid.New(), uuid.New()
	customers := &fakeSearchCustomers{items: []cp.Customer{{ID: first}, {ID: second}}}
	store := &fakeSearchStore{}

	q := searchWindow()
	q.Set("groupId", groupID.String())

	if code, _, raw := doSearch(t, adminapi.Deps{MessageSearch: store, Customers: customers}, "admin:read", q); code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", code, raw)
	}
	if customers.got.GroupID == nil || *customers.got.GroupID != groupID {
		t.Fatalf("the group was not resolved: filter = %+v", customers.got)
	}
	if len(store.got.CustomerIDs) != 2 {
		t.Fatalf("CustomerIDs = %v, want both customers of the group", store.got.CustomerIDs)
	}
}

// TestSearchIntersectsGroupAndCustomer: an extra predicate may only NARROW. A customer outside the
// group yields an empty page, never that customer's messages.
func TestSearchIntersectsGroupAndCustomer(t *testing.T) {
	inGroup, outside := uuid.New(), uuid.New()
	customers := &fakeSearchCustomers{items: []cp.Customer{{ID: inGroup}}}
	store := &fakeSearchStore{}

	q := searchWindow()
	q.Set("groupId", uuid.NewString())
	q.Set("customerId", outside.String())

	code, body, raw := doSearch(t, adminapi.Deps{MessageSearch: store, Customers: customers}, "admin:read", q)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", code, raw)
	}
	if len(body.Data) != 0 {
		t.Errorf("got %d rows, want none: the customer is not in the group", len(body.Data))
	}
	if store.calls != 0 {
		t.Errorf("the store was queried %d times for an empty intersection", store.calls)
	}
}

// --- pagination ---

func TestSearchPagesWithACursor(t *testing.T) {
	rows := []clickhouse.CDRRow{searchRowFixture("33611111111"), searchRowFixture("33622222222")}
	rows[1].SubmittedAt = rows[0].SubmittedAt.Add(-time.Minute)
	store := &fakeSearchStore{rows: rows}

	q := searchWindow()
	q.Set("limit", "1")

	code, body, raw := doSearch(t, adminapi.Deps{MessageSearch: store}, "admin:read", q)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", code, raw)
	}
	// limit+1 is fetched to detect a further page without a second query.
	if store.limit != 2 {
		t.Errorf("store queried with limit %d, want limit+1 = 2", store.limit)
	}
	if len(body.Data) != 1 || !body.HasMore {
		t.Fatalf("got %d rows (has_more=%v), want 1 and has_more", len(body.Data), body.HasMore)
	}
	if body.NextCursor == nil {
		t.Fatal("next_cursor is null on a page that has more")
	}
	// The cursor must point at the LAST RETURNED row, not at the extra row that was only a probe.
	key, err := clickhouse.DecodeCDRCursor(*body.NextCursor)
	if err != nil {
		t.Fatalf("decode next_cursor: %v", err)
	}
	if key.MessageID != rows[0].MessageID {
		t.Errorf("next_cursor points at %s, want the last returned row %s", key.MessageID, rows[0].MessageID)
	}
}

func TestSearchAcceptsItsOwnCursor(t *testing.T) {
	store := &fakeSearchStore{}
	key := clickhouse.CDRKey{SubmittedAt: time.Now().UTC().Truncate(time.Millisecond), MessageID: uuid.New()}

	q := searchWindow()
	q.Set("cursor", clickhouse.EncodeCDRCursor(key))

	if code, _, raw := doSearch(t, adminapi.Deps{MessageSearch: store}, "admin:read", q); code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", code, raw)
	}
	if store.got.After == nil || store.got.After.MessageID != key.MessageID {
		t.Errorf("After = %+v, want the decoded cursor %+v", store.got.After, key)
	}
}

// --- refusals ---

// TestSearchRefusesUnboundedOrMalformedInput: each refusal is a 422 naming its field. A bounded query
// is the whole reason the endpoint can exist — an unbounded one must be impossible to express, not
// merely discouraged.
func TestSearchRefusesUnboundedOrMalformedInput(t *testing.T) {
	now := time.Now().UTC()

	cases := map[string]struct {
		mutate func(url.Values)
		field  string
	}{
		"window inverted": {
			mutate: func(q url.Values) {
				q.Set("from_date", now.Format(time.RFC3339))
				q.Set("to_date", now.Add(-time.Hour).Format(time.RFC3339))
			},
			field: "to_date",
		},
		"window wider than 31 days": {
			mutate: func(q url.Values) {
				q.Set("from_date", now.Add(-32*24*time.Hour).Format(time.RFC3339))
				q.Set("to_date", now.Format(time.RFC3339))
			},
			field: "from_date",
		},
		"msisdn not an E.164 number": {
			mutate: func(q url.Values) { q.Set("msisdn", "not-a-number") },
			field:  "msisdn",
		},
		"malformed cursor": {
			mutate: func(q url.Values) { q.Set("cursor", "!!!not-a-cursor!!!") },
			field:  "cursor",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q := searchWindow()
			tc.mutate(q)

			code, _, raw := doSearch(t, adminapi.Deps{MessageSearch: &fakeSearchStore{}}, "admin:read", q)
			if code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422 (%s)", code, raw)
			}
			if !strings.Contains(raw, tc.field) {
				t.Errorf("the error does not name %q: %s", tc.field, raw)
			}
		})
	}
}

// TestSearchRefusesAMissingWindow: the two dates are required by the contract, so huma refuses the
// request before the handler runs. Without this the endpoint would silently scan the whole retention.
func TestSearchRefusesAMissingWindow(t *testing.T) {
	code, _, raw := doSearch(t, adminapi.Deps{MessageSearch: &fakeSearchStore{}}, "admin:read", url.Values{})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (%s)", code, raw)
	}
}

// TestSearchRefusesAnOversizedGroup: a mis-sized group would build an unbounded IN list. The refusal
// names the field so the operator knows to narrow rather than retry.
func TestSearchRefusesAnOversizedGroup(t *testing.T) {
	items := make([]cp.Customer, 501)
	for i := range items {
		items[i] = cp.Customer{ID: uuid.New()}
	}
	customers := &fakeSearchCustomers{items: items}

	q := searchWindow()
	q.Set("groupId", uuid.NewString())

	code, _, raw := doSearch(t, adminapi.Deps{MessageSearch: &fakeSearchStore{}, Customers: customers}, "admin:read", q)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (%s)", code, raw)
	}
	if !strings.Contains(raw, "groupId") {
		t.Errorf("the error does not name groupId: %s", raw)
	}
}
