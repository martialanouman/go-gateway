package stub_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/uuidx"
	"github.com/martialanouman/go-gateway/test/load/stub"
)

const testKey = "sgw_loadtestkey"

func validBody() string {
	return `{"to":"+2250700000000","from":"ACME","text":"Your OTP is 123456"}`
}

// post sends body to the handler with the given Authorization header value ("" = header absent).
func post(t *testing.T, h http.Handler, authz, body string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec.Result()
}

func decode(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()

	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode body %q: %v", raw, err)
	}

	return out
}

func TestSubmitAcceptedReturns202(t *testing.T) {
	t.Parallel()

	h := stub.NewHandler(stub.Config{})
	resp := post(t, h, "Bearer "+testKey, validBody())

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	body := decode(t, resp)
	for _, field := range []string{"id", "trace_id", "status", "accepted_at"} {
		if _, ok := body[field]; !ok {
			t.Errorf("response is missing required field %q", field)
		}
	}
	if _, err := uuidx.Parse(body["id"].(string)); err != nil {
		t.Errorf("id is not a uuid: %v", err)
	}
	if _, err := uuidx.Parse(body["trace_id"].(string)); err != nil {
		t.Errorf("trace_id is not a uuid: %v", err)
	}
	if body["status"] != "accepted" {
		t.Errorf("status = %v, want accepted", body["status"])
	}
	if _, err := time.Parse(time.RFC3339, body["accepted_at"].(string)); err != nil {
		t.Errorf("accepted_at is not RFC 3339: %v", err)
	}
}

func TestSubmitEchoesClientRef(t *testing.T) {
	t.Parallel()

	h := stub.NewHandler(stub.Config{})
	resp := post(t, h, "Bearer "+testKey,
		`{"to":"+2250700000000","from":"ACME","text":"hi","client_ref":"ref-42","priority":2,"encoding":"gsm7"}`)

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if got := decode(t, resp)["client_ref"]; got != "ref-42" {
		t.Errorf("client_ref = %v, want ref-42", got)
	}
}

func TestMissingOrMalformedBearerIs401(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"absent":       "",
		"no scheme":    testKey,
		"wrong scheme": "Basic " + testKey,
		"empty token":  "Bearer ",
		"wrong prefix": "Bearer nope_abcdef",
	}
	for name, authz := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := stub.NewHandler(stub.Config{})
			resp := post(t, h, authz, validBody())
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
			}
			if got := decode(t, resp)["code"]; got != "unauthenticated" {
				t.Errorf("code = %v, want unauthenticated", got)
			}
		})
	}
}

func TestUnknownFieldIs422(t *testing.T) {
	t.Parallel()

	h := stub.NewHandler(stub.Config{})
	resp := post(t, h, "Bearer "+testKey,
		`{"to":"+2250700000000","from":"ACME","text":"hi","surprise":true}`)

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnprocessableEntity)
	}
	if got := decode(t, resp)["code"]; got != "validation_error" {
		t.Errorf("code = %v, want validation_error", got)
	}
}

func TestInvalidBodyIs422(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		body  string
		field string
	}{
		"missing text":     {`{"to":"+2250700000000","from":"ACME"}`, "text"},
		"missing to":       {`{"from":"ACME","text":"hi"}`, "to"},
		"bad msisdn":       {`{"to":"0700000000","from":"ACME","text":"hi"}`, "to"},
		"sender too long":  {`{"to":"+2250700000000","from":"AAAAAAAAAAAAAAAAAAAAA","text":"hi"}`, "from"},
		"priority too big": {`{"to":"+2250700000000","from":"ACME","text":"hi","priority":9}`, "priority"},
		"bad encoding":     {`{"to":"+2250700000000","from":"ACME","text":"hi","encoding":"morse"}`, "encoding"},
		"not json":         {`{`, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := stub.NewHandler(stub.Config{})
			resp := post(t, h, "Bearer "+testKey, tc.body)
			if resp.StatusCode != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnprocessableEntity)
			}
			body := decode(t, resp)
			if got := body["code"]; got != "validation_error" {
				t.Errorf("code = %v, want validation_error", got)
			}
			if tc.field == "" {
				return
			}
			errs, ok := body["errors"].([]any)
			if !ok || len(errs) == 0 {
				t.Fatalf("errors[] is missing or empty: %v", body)
			}
			var fields []string
			for _, e := range errs {
				fields = append(fields, e.(map[string]any)["field"].(string))
			}
			found := false
			for _, f := range fields {
				if f == tc.field {
					found = true
				}
			}
			if !found {
				t.Errorf("errors[] fields = %v, want one named %q", fields, tc.field)
			}
		})
	}
}

// The repo invariant: the body of a message never reaches a serialization. A rejected
// submission is the likeliest leak — the handler holds the text and is writing an error.
func TestErrorResponseNeverEchoesText(t *testing.T) {
	t.Parallel()

	const secret = "TOP-SECRET-OTP-98765"
	h := stub.NewHandler(stub.Config{})
	resp := post(t, h, "Bearer "+testKey,
		`{"to":"nope","from":"ACME","text":"`+secret+`","surprise":1}`)

	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatalf("error response leaked the message body: %s", raw)
	}
}

// The load run must be able to FAIL. That capability rests entirely on this delay being real:
// the same k6 script is run against a fast stub (pass) and a slowed one (must exit non-zero).
func TestArtificialDelayIsApplied(t *testing.T) {
	t.Parallel()

	const delay = 200 * time.Millisecond

	slow := stub.NewHandler(stub.Config{Delay: delay})
	start := time.Now()
	resp := post(t, slow, "Bearer "+testKey, validBody())
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if elapsed < delay {
		t.Fatalf("served in %v, want at least the configured delay %v", elapsed, delay)
	}

	fast := stub.NewHandler(stub.Config{})
	start = time.Now()
	_ = post(t, fast, "Bearer "+testKey, validBody())
	if quick := time.Since(start); quick >= delay {
		t.Fatalf("zero-delay handler took %v, want well under %v — the delay is not configurable", quick, delay)
	}
}

// A rejected request must be delayed too, otherwise a run against the slowed stub could pass by
// answering errors instantly.
func TestArtificialDelayAppliesToRejections(t *testing.T) {
	t.Parallel()

	const delay = 150 * time.Millisecond

	h := stub.NewHandler(stub.Config{Delay: delay})
	start := time.Now()
	resp := post(t, h, "", validBody())
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	if elapsed < delay {
		t.Fatalf("rejected in %v, want at least the configured delay %v", elapsed, delay)
	}
}

func TestDelayStopsWithTheClient(t *testing.T) {
	t.Parallel()

	h := stub.NewHandler(stub.Config{Delay: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(validBody()))
	req.Header.Set("Authorization", "Bearer "+testKey)
	req = req.WithContext(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler is still sleeping after the client went away")
	}
}

func TestListenServesOnAnEphemeralPortAndShutsDown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv, err := stub.Listen(ctx, stub.Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	})

	if srv.Addr() == "" {
		t.Fatal("Addr() is empty, want the bound address")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+srv.Addr()+"/v1/messages", strings.NewReader(validBody()))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("server still answers after Shutdown")
	}
}
