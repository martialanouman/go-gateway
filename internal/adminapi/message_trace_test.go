package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

const secretBody = "MEET ME AT MIDNIGHT"

type fakeTraceStore struct {
	row        clickhouse.CDRRow
	found      bool
	milestones []clickhouse.CDRMilestone
}

func (f *fakeTraceStore) ByMessageID(context.Context, uuid.UUID) (clickhouse.CDRRow, bool, error) {
	return f.row, f.found, nil
}

func (f *fakeTraceStore) Timeline(context.Context, uuid.UUID) ([]clickhouse.CDRMilestone, error) {
	return f.milestones, nil
}

func traceFixture() *fakeTraceStore {
	id, trace := uuid.New(), uuid.New()
	code := "submit_failed"
	latency := uint32(1200)
	ciphertext := secretBody
	keyID := uuid.New()
	return &fakeTraceStore{
		found: true,
		row: clickhouse.CDRRow{
			MessageID: id, TraceID: trace,
			AccountID: uuid.New(), CustomerID: uuid.New(),
			Direction:  clickhouse.DirectionMT,
			SourceAddr: "GATEWAY", DestAddr: "33612345678",
			// The body MUST be present on the row: without it the leak assertion below cannot fail, which
			// is exactly how the first version of this test passed while a real leak went undetected.
			ContentCiphertext: &ciphertext,
			ContentKeyID:      &keyID,
		},
		milestones: []clickhouse.CDRMilestone{
			{Version: 1, Status: clickhouse.StatusAccepted, At: time.Now().Add(-time.Minute)},
			{Version: 2, Status: clickhouse.StatusEnroute, At: time.Now().Add(-30 * time.Second)},
			{Version: 3, Status: clickhouse.StatusFailed, At: time.Now(), ErrorCode: &code, LatencyMS: &latency},
		},
	}
}

func getTrace(t *testing.T, store adminapi.TraceStore, scopes string) (int, messageTraceBody) {
	t.Helper()
	handler := newTestAPIWithScopes(t, adminapi.Deps{Trace: store}, scopes)
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/messages/"+uuid.NewString()+"/trace", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var body messageTraceBody
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v (%s)", err, rec.Body)
		}
	}
	return rec.Code, body
}

type messageTraceBody struct {
	MessageID string `json:"message_id"`
	TraceID   string `json:"trace_id"`
	Spans     []struct {
		Name       string         `json:"name"`
		Status     string         `json:"status"`
		Attributes map[string]any `json:"attributes"`
	} `json:"spans"`
}

// TestTraceReturnsEveryLifecycleStageInOrder is the step's first criterion: the CDR keeps one row per stage,
// and the trace must show them all, oldest first — a trace that only shows the final status explains nothing.
func TestTraceReturnsEveryLifecycleStageInOrder(t *testing.T) {
	code, body := getTrace(t, traceFixture(), "admin:read")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	want := []string{"cdr.accepted", "cdr.enroute", "cdr.failed"}
	if len(body.Spans) != len(want) {
		t.Fatalf("got %d spans, want %d", len(body.Spans), len(want))
	}
	for i, name := range want {
		if body.Spans[i].Name != name {
			t.Errorf("span %d = %q, want %q", i, body.Spans[i].Name, name)
		}
	}
	if got := body.Spans[2].Status; got != "error" {
		t.Errorf("a failed stage has status %q, want error", got)
	}
	if got := body.Spans[2].Attributes["code"]; got != "submit_failed" {
		t.Errorf("the failure code is missing: %v", got)
	}
}

// TestTraceNeverCarriesABody is invariant (a) at this endpoint. The query selects no content column, so the
// assertion is on the whole serialized response rather than one field.
func TestTraceNeverCarriesABody(t *testing.T) {
	store := traceFixture()
	handler := newTestAPIWithScopes(t, adminapi.Deps{Trace: store}, "admin:read")
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/messages/"+uuid.NewString()+"/trace", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if strings.Contains(rec.Body.String(), secretBody) {
		t.Fatal("a message body reached the trace response")
	}
	for _, forbidden := range []string{"content_ciphertext", "content_key_id"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Errorf("the response exposes %q", forbidden)
		}
	}
}

// TestTraceMasksMSISDNByRole: the reveal scope is separate from content:read on purpose, so an operator who
// may see numbers is not thereby able to decrypt messages.
func TestTraceMasksMSISDNByRole(t *testing.T) {
	t.Run("without the scope", func(t *testing.T) {
		_, body := getTrace(t, traceFixture(), "admin:read")
		dest, _ := body.Spans[0].Attributes["dest_addr"].(string)
		if dest == "33612345678" {
			t.Fatal("the destination was revealed to a role without msisdn:reveal")
		}
		if !strings.HasPrefix(dest, "3361") || !strings.HasSuffix(dest, "78") {
			t.Errorf("dest_addr = %q, want a masked number keeping prefix and tail", dest)
		}
	})

	t.Run("with the scope", func(t *testing.T) {
		_, body := getTrace(t, traceFixture(), "admin:read|msisdn:reveal")
		if got, _ := body.Spans[0].Attributes["dest_addr"].(string); got != "33612345678" {
			t.Errorf("dest_addr = %q, want the number revealed", got)
		}
	})
}

func TestTraceUnknownMessageIs404(t *testing.T) {
	code, _ := getTrace(t, &fakeTraceStore{}, "admin:read")
	if code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", code)
	}
}
