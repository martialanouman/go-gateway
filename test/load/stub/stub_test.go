package stub_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// The repo invariant: the body of a message never reaches a serialization.
//
// Both paths that actually hold the decoded text must be covered. An earlier version of this test
// sent an unknown field alongside the secret, which made the decoder reject the body *before*
// validate() ever ran — so the one function that holds req.Text was never exercised, and a
// `Message: "rejected: " + req.Text` would have gone unnoticed.
func TestResponseNeverEchoesText(t *testing.T) {
	t.Parallel()

	const secret = "TOP-SECRET-OTP-98765"

	cases := map[string]struct {
		body string
		want int
	}{
		// Decodes cleanly, so validate() runs holding the text, and rejects on `to`.
		"rejected by validation": {`{"to":"nope","from":"ACME","text":"` + secret + `"}`, http.StatusUnprocessableEntity},
		// The accepted path builds a response object while holding the text too.
		"accepted": {`{"to":"+2250700000000","from":"ACME","text":"` + secret + `"}`, http.StatusAccepted},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := stub.NewHandler(stub.Config{})
			resp := post(t, h, "Bearer "+testKey, tc.body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tc.want {
				t.Fatalf("status: got %d want %d — the intended code path was not reached", resp.StatusCode, tc.want)
			}
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if strings.Contains(string(raw), secret) {
				t.Fatalf("response leaked the message body: %s", raw)
			}
		})
	}
}

// idempotencyHeader is the contract's header name (api/openapi-public.yaml, IdempotencyKey).
const idempotencyHeader = "Idempotency-Key"

// postIdem sends a valid submission carrying idemKey. present=false omits the header entirely;
// present=true with an empty idemKey sends the header with an empty value — the two cases the stub
// must tell apart, and the exact mistake D10 exists to catch in the k6 script.
func postIdem(t *testing.T, h http.Handler, present bool, idemKey string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(validBody()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testKey)
	if present {
		req.Header.Set(idempotencyHeader, idemKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec.Result()
}

// The default must be strictly the step-200 behaviour: the header is neither required nor refused,
// and a repeat is not a conflict. Anything else would change what every existing run measures.
func TestIdempotencyIgnoreIsTheDefault(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]stub.Config{
		"zero value":      {},
		"explicit ignore": {Idempotency: stub.IdempotencyIgnore},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := stub.NewHandler(cfg)
			for _, tc := range []struct {
				name    string
				present bool
				key     string
			}{
				{"absent", false, ""},
				{"empty", true, ""},
				{"present", true, "k6-abc-1"},
				{"repeated", true, "k6-abc-1"},
				{"over 128 chars", true, strings.Repeat("x", 129)},
			} {
				resp := postIdem(t, h, tc.present, tc.key)
				if resp.StatusCode != http.StatusAccepted {
					t.Errorf("%s: status = %d, want %d", tc.name, resp.StatusCode, http.StatusAccepted)
				}
				_ = resp.Body.Close()
			}
		})
	}
}

func TestRequireUniqueRejectsWhatTheContractCannotAccept(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		present bool
		key     string
	}{
		"absent":            {false, ""},
		"present but empty": {true, ""},
		"129 characters":    {true, strings.Repeat("x", 129)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := stub.NewHandler(stub.Config{Idempotency: stub.IdempotencyRequireUnique})
			resp := postIdem(t, h, tc.present, tc.key)
			if resp.StatusCode != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnprocessableEntity)
			}
			body := decode(t, resp)
			if got := body["code"]; got != "validation_error" {
				t.Errorf("code = %v, want validation_error", got)
			}
			errs, ok := body["errors"].([]any)
			if !ok || len(errs) == 0 {
				t.Fatalf("errors[] is missing or empty: %v", body)
			}
			if got := errs[0].(map[string]any)["field"]; got != idempotencyHeader {
				t.Errorf("errors[0].field = %v, want %q", got, idempotencyHeader)
			}
		})
	}
}

// A header holding only spaces reaches the server as an empty value — net/http trims it while
// parsing — which the real gateway would read as "no idempotency" and answer 202 to. This one has to
// go over a real connection: httptest.NewRequest builds the header map directly and never parses
// anything, so the trimming that makes the case interesting would not happen.
func TestWhitespaceOnlyKeyArrivesEmptyAndIsRejected(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv, err := stub.Listen(ctx, stub.Config{Addr: "127.0.0.1:0", Idempotency: stub.IdempotencyRequireUnique})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+srv.Addr()+"/v1/messages", strings.NewReader(validBody()))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(idempotencyHeader, "   ")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != http.StatusUnprocessableEntity {
		_ = resp.Body.Close()
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnprocessableEntity)
	}
	if got := decode(t, resp)["code"]; got != "validation_error" {
		t.Errorf("code = %v, want validation_error", got)
	}
}

// The boundary belongs to the contract (maxLength: 128), so 128 must be accepted. A stub that
// rejected it would fail a k6 run for a key the real gateway takes.
func TestRequireUniqueAcceptsExactly128Characters(t *testing.T) {
	t.Parallel()

	h := stub.NewHandler(stub.Config{Idempotency: stub.IdempotencyRequireUnique})
	resp := postIdem(t, h, true, strings.Repeat("x", 128))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
}

// This is the assertion the whole option rests on: a constant key would measure the idempotency
// cache instead of the idempotent path, and only "already seen" can catch it.
func TestRequireUniqueRejectsARepeatedKey(t *testing.T) {
	t.Parallel()

	h := stub.NewHandler(stub.Config{Idempotency: stub.IdempotencyRequireUnique})

	first := postIdem(t, h, true, "k6-seed-0")
	_ = first.Body.Close()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first request: status = %d, want %d", first.StatusCode, http.StatusAccepted)
	}

	other := postIdem(t, h, true, "k6-seed-1")
	_ = other.Body.Close()
	if other.StatusCode != http.StatusAccepted {
		t.Fatalf("distinct key: status = %d, want %d", other.StatusCode, http.StatusAccepted)
	}

	repeat := postIdem(t, h, true, "k6-seed-0")
	if repeat.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("repeated key: status = %d, want %d", repeat.StatusCode, http.StatusUnprocessableEntity)
	}
	if got := decode(t, repeat)["code"]; got != "validation_error" {
		t.Errorf("code = %v, want validation_error", got)
	}
}

// Two stubs must not share a seen-set: one run's keys cannot make the next run fail.
func TestSeenKeysAreScopedToOneHandler(t *testing.T) {
	t.Parallel()

	cfg := stub.Config{Idempotency: stub.IdempotencyRequireUnique}
	first := postIdem(t, stub.NewHandler(cfg), true, "k6-seed-0")
	_ = first.Body.Close()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", first.StatusCode, http.StatusAccepted)
	}

	second := postIdem(t, stub.NewHandler(cfg), true, "k6-seed-0")
	_ = second.Body.Close()
	if second.StatusCode != http.StatusAccepted {
		t.Fatalf("fresh handler, same key: status = %d, want %d", second.StatusCode, http.StatusAccepted)
	}
}

// The load client is concurrent, so the check-and-insert must be atomic: N racing requests with the
// same key must yield exactly one acceptance, not N.
func TestRequireUniqueIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()

	const racers = 32

	h := stub.NewHandler(stub.Config{Idempotency: stub.IdempotencyRequireUnique})

	var (
		mu       sync.Mutex
		accepted int
		start    = make(chan struct{})
		wg       sync.WaitGroup
	)
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp := postIdem(t, h, true, "k6-contended")
			_ = resp.Body.Close()
			mu.Lock()
			defer mu.Unlock()
			if resp.StatusCode == http.StatusAccepted {
				accepted++
			}
		}()
	}
	close(start)
	wg.Wait()

	if accepted != 1 {
		t.Fatalf("accepted = %d, want exactly 1 out of %d racing requests on the same key", accepted, racers)
	}
}

func TestForbidRejectsAnyPresenceOfTheHeader(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		present bool
		key     string
		want    int
	}{
		// The positive run of the smoke script: IDEMPOTENCY unset must emit nothing at all.
		"absent": {false, "", http.StatusAccepted},
		// The negative run: emitting the header must be detected...
		"present": {true, "k6-abc-1", http.StatusUnprocessableEntity},
		// ...including the silent failure mode D10 names — a header present and empty, which the real
		// gateway treats as "no idempotency" without a word.
		"present but empty": {true, "", http.StatusUnprocessableEntity},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := stub.NewHandler(stub.Config{Idempotency: stub.IdempotencyForbid})
			resp := postIdem(t, h, tc.present, tc.key)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			if tc.want != http.StatusUnprocessableEntity {
				_ = resp.Body.Close()
				return
			}
			if got := decode(t, resp)["code"]; got != "validation_error" {
				t.Errorf("code = %v, want validation_error", got)
			}
		})
	}
}

// Authentication stays first, as on the real surface: an unauthenticated request is a 401 whatever
// the header says. Otherwise the stub would answer 422 where the gateway answers 401.
func TestIdempotencyChecksRunAfterAuthentication(t *testing.T) {
	t.Parallel()

	for name, mode := range map[string]stub.IdempotencyMode{
		"require-unique": stub.IdempotencyRequireUnique,
		"forbid":         stub.IdempotencyForbid,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := stub.NewHandler(stub.Config{Idempotency: mode})
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(validBody()))
			req.Header.Set(idempotencyHeader, "k6-abc-1")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
			}
		})
	}
}

// A rejection on this path must be delayed like every other response: an instant 422 would let a run
// against the slowed stub clear the latency budget.
func TestIdempotencyRejectionIsDelayed(t *testing.T) {
	t.Parallel()

	const delay = 150 * time.Millisecond

	h := stub.NewHandler(stub.Config{Delay: delay, Idempotency: stub.IdempotencyForbid})
	start := time.Now()
	resp := postIdem(t, h, true, "k6-abc-1")
	elapsed := time.Since(start)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnprocessableEntity)
	}
	if elapsed < delay {
		t.Fatalf("rejected in %v, want at least the configured delay %v", elapsed, delay)
	}
}

func TestParseIdempotencyMode(t *testing.T) {
	t.Parallel()

	ok := map[string]stub.IdempotencyMode{
		"ignore":         stub.IdempotencyIgnore,
		"":               stub.IdempotencyIgnore,
		"require-unique": stub.IdempotencyRequireUnique,
		"forbid":         stub.IdempotencyForbid,
	}
	for in, want := range ok {
		got, err := stub.ParseIdempotencyMode(in)
		if err != nil {
			t.Errorf("ParseIdempotencyMode(%q) errored: %v", in, err)

			continue
		}
		if got != want {
			t.Errorf("ParseIdempotencyMode(%q) = %q, want %q", in, got, want)
		}
	}

	// A typo must stop the run, not silently downgrade the observer to "ignore" — the same doctrine
	// the k6 script applies to PROFILE.
	for _, in := range []string{"requireunique", "require_unique", "Forbid", "off", "on", "IGNORE"} {
		if got, err := stub.ParseIdempotencyMode(in); err == nil {
			t.Errorf("ParseIdempotencyMode(%q) = %q, want an error", in, got)
		}
	}
}

// A mode that never went through ParseIdempotencyMode must not be able to disable the observer in
// silence: NewHandler refuses it outright.
func TestNewHandlerRejectsAnUnknownMode(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("NewHandler accepted an unknown idempotency mode; it must not")
		}
	}()
	stub.NewHandler(stub.Config{Idempotency: stub.IdempotencyMode("require_unique")})
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

	// A FRESH request: reusing the one above would fail on its drained body
	// ("ContentLength=N with Body length 0") before ever touching the network, and the assertion
	// would hold even against a Shutdown that closes nothing.
	after, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+srv.Addr()+"/v1/messages", strings.NewReader(validBody()))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	after.Header.Set("Authorization", "Bearer "+testKey)
	after.Header.Set("Content-Type", "application/json")

	resp2, err := http.DefaultClient.Do(after)
	if err == nil {
		_ = resp2.Body.Close()
		t.Fatal("server still answers after Shutdown")
	}
	// The listener must be gone, not merely slow: anything else means we proved the wrong thing.
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("want a refused connection after Shutdown, got: %v", err)
	}
}
