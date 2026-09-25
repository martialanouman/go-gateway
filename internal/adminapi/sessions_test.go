package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// fakeSessions models the registry's contract: sessions in bind id order, a page strictly after the
// cursor, and a disconnect of a bind that is not live answers ErrSessionNotFound.
type fakeSessions struct {
	mu           sync.Mutex
	live         []adminapi.LiveSession
	active       map[uuid.UUID]int
	disconnected []string
	reasons      []string
}

func (f *fakeSessions) sorted() []adminapi.LiveSession {
	out := append([]adminapi.LiveSession(nil), f.live...)
	sort.Slice(out, func(i, j int) bool { return out[i].BindID < out[j].BindID })
	return out
}

func (f *fakeSessions) ListAccountSessions(_ context.Context, accountID uuid.UUID) ([]adminapi.LiveSession, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []adminapi.LiveSession
	for _, s := range f.sorted() {
		if s.AccountID == accountID {
			out = append(out, s)
		}
	}
	if n, ok := f.active[accountID]; ok {
		return out, n, nil
	}
	return out, len(out), nil
}

func (f *fakeSessions) ListSessions(_ context.Context, after string, limit int) ([]adminapi.LiveSession, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []adminapi.LiveSession
	for _, s := range f.sorted() {
		if s.BindID > after {
			out = append(out, s)
		}
	}
	if len(out) > limit {
		return out[:limit], out[limit-1].BindID, nil
	}
	return out, "", nil
}

func (f *fakeSessions) DisconnectSession(_ context.Context, bindID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.live {
		if s.BindID == bindID {
			f.disconnected = append(f.disconnected, bindID)
			f.reasons = append(f.reasons, reason)
			return nil
		}
	}
	return adminapi.ErrSessionNotFound
}

func liveSession(account uuid.UUID) adminapi.LiveSession {
	return adminapi.LiveSession{
		AccountID: account, BindID: uuid.NewString(), SystemID: "sys-1", PodID: "pod-1", BindType: "trx",
		RemoteAddr: "203.0.113.9", WindowSize: 10, ConnectedAt: time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC),
	}
}

func accountWithMax(t *testing.T, store *fakeAccountStore, maxSessions int) uuid.UUID {
	t.Helper()
	a, err := store.Create(context.Background(), cp.NewAccount{CustomerID: uuid.New(), Name: "acct", MaxSessions: &maxSessions})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	return a.ID
}

type accountSessionsBody struct {
	MaxSessions int              `json:"max_sessions"`
	Active      int              `json:"active"`
	Sessions    []map[string]any `json:"sessions"`
}

func getAccountSessions(t *testing.T, api http.Handler, id uuid.UUID) (int, accountSessionsBody) {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/smpp-accounts/"+id.String()+"/sessions", ""))
	var body accountSessionsBody
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body
}

func TestListAccountSessionsShowsAFullAccount(t *testing.T) {
	accounts := newFakeAccountStore()
	full := accountWithMax(t, accounts, 2)
	api := newTestAPIWith(t, adminapi.Deps{Accounts: accounts, Sessions: &fakeSessions{
		live: []adminapi.LiveSession{liveSession(full), liveSession(full)},
	}})

	code, body := getAccountSessions(t, api, full)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if body.MaxSessions != 2 || body.Active != 2 || len(body.Sessions) != 2 {
		t.Fatalf("max_sessions=%d active=%d sessions=%d, want a full account: 2, 2, 2",
			body.MaxSessions, body.Active, len(body.Sessions))
	}
}

// TestListAccountSessionsKeepsLimitAndLiveApart: two different non-zero numbers, so a swapped or merged
// field cannot pass; and active counts a bind the registry holds but has not described yet.
func TestListAccountSessionsKeepsLimitAndLiveApart(t *testing.T) {
	accounts := newFakeAccountStore()
	id := accountWithMax(t, accounts, 4)
	api := newTestAPIWith(t, adminapi.Deps{Accounts: accounts, Sessions: &fakeSessions{
		live:   []adminapi.LiveSession{liveSession(id), liveSession(id)},
		active: map[uuid.UUID]int{id: 3},
	}})

	_, body := getAccountSessions(t, api, id)
	if body.MaxSessions != 4 || body.Active != 3 || len(body.Sessions) != 2 {
		t.Fatalf("max_sessions=%d active=%d sessions=%d, want 4, 3, 2", body.MaxSessions, body.Active, len(body.Sessions))
	}
}

func TestListAccountSessionsOfAnUnknownAccountIsNotFound(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Accounts: newFakeAccountStore(), Sessions: &fakeSessions{}})
	if code, _ := getAccountSessions(t, api, uuid.New()); code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
}

type sessionPageBody struct {
	Data       []map[string]any `json:"data"`
	NextCursor *string          `json:"next_cursor"`
	HasMore    bool             `json:"has_more"`
}

func listSessions(t *testing.T, api http.Handler, query string) (int, sessionPageBody) {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/sessions"+query, ""))
	var body sessionPageBody
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body
}

func TestListSessionsRendersAClientBind(t *testing.T) {
	s := liveSession(uuid.New())
	api := newTestAPIWith(t, adminapi.Deps{Sessions: &fakeSessions{live: []adminapi.LiveSession{s}}})

	code, body := listSessions(t, api, "")
	if code != http.StatusOK || len(body.Data) != 1 {
		t.Fatalf("status=%d rows=%d, want 200 and 1", code, len(body.Data))
	}
	row := body.Data[0]
	want := map[string]any{
		"id": s.BindID, "bind_id": s.BindID, "account_id": s.AccountID.String(), "connector_id": nil,
		"bind_type": "trx", "direction": "user", "pod_id": "pod-1", "remote_addr": "203.0.113.9",
		"window_size": float64(10), "connected_at": "2026-09-25T08:00:00Z", "last_enquire_link": nil,
	}
	for k, v := range want {
		got, present := row[k]
		if v == nil && !present {
			continue // optional and nullable: huma omits a nil pointer
		}
		if !present || got != v {
			t.Errorf("%s = %v (present=%v), want %v", k, got, present, v)
		}
	}
}

func TestListSessionsPagesByCursor(t *testing.T) {
	fake := &fakeSessions{}
	for range 3 {
		fake.live = append(fake.live, liveSession(uuid.New()))
	}
	ids := fake.sorted()
	api := newTestAPIWith(t, adminapi.Deps{Sessions: fake})

	_, first := listSessions(t, api, "?limit=2")
	if len(first.Data) != 2 || !first.HasMore || first.NextCursor == nil || *first.NextCursor != ids[1].BindID {
		t.Fatalf("first page = %d rows, has_more=%v, next=%v; want 2, true, %s", len(first.Data), first.HasMore, first.NextCursor, ids[1].BindID)
	}
	_, second := listSessions(t, api, "?limit=2&cursor="+*first.NextCursor)
	if len(second.Data) != 1 || second.HasMore || second.NextCursor != nil || second.Data[0]["id"] != ids[2].BindID {
		t.Fatalf("second page = %+v, want the last session and no cursor", second)
	}
}

func TestListSessionsFiltersByAccountAndStillPages(t *testing.T) {
	mine, other := uuid.New(), uuid.New()
	fake := &fakeSessions{live: []adminapi.LiveSession{liveSession(mine), liveSession(other), liveSession(mine), liveSession(mine)}}
	var own []string
	for _, s := range fake.sorted() {
		if s.AccountID == mine {
			own = append(own, s.BindID)
		}
	}
	api := newTestAPIWith(t, adminapi.Deps{Sessions: fake})

	_, first := listSessions(t, api, "?limit=2&accountId="+mine.String())
	if len(first.Data) != 2 || !first.HasMore || first.Data[0]["id"] != own[0] || first.Data[1]["id"] != own[1] {
		t.Fatalf("first page = %+v, want the account's first two binds", first)
	}
	_, second := listSessions(t, api, "?limit=2&accountId="+mine.String()+"&cursor="+*first.NextCursor)
	if len(second.Data) != 1 || second.HasMore || second.Data[0]["id"] != own[2] {
		t.Fatalf("second page = %+v, want the account's last bind only", second)
	}
}

func TestListSessionsRefusesTheConnectorFilter(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Sessions: &fakeSessions{live: []adminapi.LiveSession{liveSession(uuid.New())}}})
	if code, _ := listSessions(t, api, "?connectorId="+uuid.NewString()); code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422: an empty page would claim the connector has no bind", code)
	}
}

func TestDisconnectSessionOrdersTheCloseWithItsReason(t *testing.T) {
	s := liveSession(uuid.New())
	fake := &fakeSessions{live: []adminapi.LiveSession{s}}
	api := newTestAPIWith(t, adminapi.Deps{Sessions: fake})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/sessions/"+s.BindID, ""))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body)
	}
	if len(fake.disconnected) != 1 || fake.disconnected[0] != s.BindID || fake.reasons[0] != "operator_disconnect" {
		t.Fatalf("disconnected=%v reasons=%v, want %s with operator_disconnect", fake.disconnected, fake.reasons, s.BindID)
	}
}

func TestDisconnectAnUnknownSessionIsNotFound(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Sessions: &fakeSessions{}})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/sessions/"+uuid.NewString(), ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, never a silent 204", w.Code)
	}
}
