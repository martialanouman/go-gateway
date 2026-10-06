package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/bindfailure"
)

type fakeBindFailures struct {
	byAccount map[uuid.UUID][]bindfailure.Failure
	since     time.Time
}

func (f *fakeBindFailures) List(_ context.Context, accountID uuid.UUID, since time.Time) ([]bindfailure.Failure, error) {
	f.since = since
	var out []bindfailure.Failure
	for _, b := range f.byAccount[accountID] {
		if !b.At.Before(since) {
			out = append(out, b)
		}
	}
	return out, nil
}

func getBindFailures(t *testing.T, api http.Handler, id uuid.UUID, query string) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/smpp-accounts/"+id.String()+"/bind-failures"+query, ""))
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body
}

func TestListAccountBindFailuresRendersTheRows(t *testing.T) {
	accounts := newFakeAccountStore()
	id := accountWithMax(t, accounts, 1)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	log := &fakeBindFailures{byAccount: map[uuid.UUID][]bindfailure.Failure{id: {
		{At: at, RemoteIP: "203.0.113.7", BindType: "trx", CommandStatus: "ESME_RINVPASWD", Reason: bindfailure.ReasonThrottled},
		{At: at.Add(-time.Hour), RemoteIP: "203.0.113.8", BindType: "tx", CommandStatus: "ESME_RBINDFAIL", Reason: bindfailure.ReasonMaxSessions},
	}}}
	api := newTestAPIWith(t, adminapi.Deps{Accounts: accounts, BindFailures: log})

	code, body := getBindFailures(t, api, id, "")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	want := []any{
		map[string]any{
			"at": at.Format(time.RFC3339), "remote_ip": "203.0.113.7", "bind_type": "trx",
			"command_status": "ESME_RINVPASWD", "reason": "throttled",
		},
		map[string]any{
			"at": at.Add(-time.Hour).Format(time.RFC3339), "remote_ip": "203.0.113.8", "bind_type": "tx",
			"command_status": "ESME_RBINDFAIL", "reason": "max_sessions_exceeded",
		},
	}
	if got, _ := json.Marshal(body["data"]); string(got) != mustJSON(t, want) {
		t.Fatalf("data = %s, want %s", got, mustJSON(t, want))
	}
	if age := time.Since(log.since); age < bindfailure.Retention-time.Minute || age > bindfailure.Retention+time.Minute {
		t.Fatalf("default since is %v ago, want the retention %v", age, bindfailure.Retention)
	}
}

func TestListAccountBindFailuresWithoutAnyIsAnEmptyList(t *testing.T) {
	accounts := newFakeAccountStore()
	id := accountWithMax(t, accounts, 1)
	api := newTestAPIWith(t, adminapi.Deps{Accounts: accounts, BindFailures: &fakeBindFailures{}})

	code, body := getBindFailures(t, api, id, "")
	if got, _ := json.Marshal(body["data"]); code != http.StatusOK || string(got) != "[]" {
		t.Fatalf("status = %d data = %s, want 200 []", code, got)
	}
}

func TestListAccountBindFailuresBoundsSince(t *testing.T) {
	accounts := newFakeAccountStore()
	id := accountWithMax(t, accounts, 1)
	log := &fakeBindFailures{}
	api := newTestAPIWith(t, adminapi.Deps{Accounts: accounts, BindFailures: log})

	since := time.Now().UTC().Add(-8 * time.Hour).Truncate(time.Second)
	if code, _ := getBindFailures(t, api, id, "?since="+url.QueryEscape(since.Format(time.RFC3339))); code != http.StatusOK || !log.since.Equal(since) {
		t.Fatalf("8h: status = %d since = %v, want 200 %v", code, log.since, since)
	}
	for name, s := range map[string]time.Time{
		"past the retention": time.Now().Add(-bindfailure.Retention - time.Minute),
		"in the future":      time.Now().Add(time.Minute),
	} {
		if code, _ := getBindFailures(t, api, id, "?since="+url.QueryEscape(s.Format(time.RFC3339))); code != http.StatusUnprocessableEntity {
			t.Errorf("%s: status = %d, want 422", name, code)
		}
	}
}

func TestListAccountBindFailuresOfAnUnknownAccountIsNotFound(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Accounts: newFakeAccountStore(), BindFailures: &fakeBindFailures{}})
	if code, _ := getBindFailures(t, api, uuid.New(), ""); code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
