package restapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/idempotency"
	"github.com/martialanouman/go-gateway/internal/restapi"
)

// fakeIdemStore is an in-memory restapi.IdempotencyStore for handler tests, so they exercise the
// two-phase orchestration without a real Redis. Reserve is mutex-guarded, so the concurrency test sees
// a single winner.
type fakeIdemStore struct {
	mu         sync.Mutex
	entries    map[string]*idemEntry
	reserveErr error
}

type idemEntry struct {
	bodyHash string
	response []byte
	done     bool
}

func newFakeIdemStore() *fakeIdemStore {
	return &fakeIdemStore{entries: map[string]*idemEntry{}}
}

func (f *fakeIdemStore) Reserve(_ context.Context, accountID, idemKey, bodyHash string, response []byte) (idempotency.Result, error) {
	if f.reserveErr != nil {
		return idempotency.Result{}, f.reserveErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := accountID + "|" + idemKey
	e, ok := f.entries[k]
	if !ok {
		f.entries[k] = &idemEntry{bodyHash: bodyHash, response: response}
		return idempotency.Result{Outcome: idempotency.Reserved}, nil
	}
	if e.bodyHash != bodyHash {
		return idempotency.Result{Outcome: idempotency.Conflict}, nil
	}
	if e.done {
		return idempotency.Result{Outcome: idempotency.Replay, Response: e.response}, nil
	}
	return idempotency.Result{Outcome: idempotency.Pending, Response: e.response}, nil
}

func (f *fakeIdemStore) Finalize(_ context.Context, accountID, idemKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.entries[accountID+"|"+idemKey]; ok {
		e.done = true
	}
	return nil
}

func (f *fakeIdemStore) Release(_ context.Context, accountID, idemKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries, accountID+"|"+idemKey)
	return nil
}

func (f *fakeIdemStore) Await(ctx context.Context, accountID, idemKey string, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		e := f.entries[accountID+"|"+idemKey]
		if e != nil && e.done {
			resp := e.response
			f.mu.Unlock()
			return resp, nil
		}
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
	return nil, idempotency.ErrAwaitTimeout
}

// postWithKey submits a message carrying an Idempotency-Key header.
func postWithKey(t *testing.T, h *harness, auth, key string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, h.server.URL+"/v1/messages", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+auth)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := h.server.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestSubmitIdempotentReplaysAndPublishesOnce(t *testing.T) {
	h := buildHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, &fakeCDRReader{}, newFakeIdemStore())
	body := map[string]any{"to": "+2250700000000", "from": "ACME", "text": "Your OTP is 123456"}

	first := postWithKey(t, h, "sgw_key", "key-abc", body)
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first submit: got %d want 202", first.StatusCode)
	}
	var firstBody restapi.AcceptedMessage
	decode(t, first, &firstBody)

	// A retry with the same key + body replays the original result and does NOT publish again.
	second := postWithKey(t, h, "sgw_key", "key-abc", body)
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != http.StatusAccepted {
		t.Fatalf("replay submit: got %d want 202", second.StatusCode)
	}
	var secondBody restapi.AcceptedMessage
	decode(t, second, &secondBody)

	if secondBody.ID != firstBody.ID {
		t.Errorf("replay id = %q, want the original %q", secondBody.ID, firstBody.ID)
	}
	if h.producer.count() != 1 {
		t.Fatalf("expected exactly 1 publish across the original + replay, got %d", h.producer.count())
	}
}

func TestSubmitIdempotentConflictOnChangedBody(t *testing.T) {
	h := buildHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, &fakeCDRReader{}, newFakeIdemStore())

	first := postWithKey(t, h, "sgw_key", "key-xyz", map[string]any{"to": "+2250700000000", "from": "ACME", "text": "one"})
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first submit: got %d want 202", first.StatusCode)
	}

	// Same key, different body → 409 idempotency_conflict, and no second publish.
	second := postWithKey(t, h, "sgw_key", "key-xyz", map[string]any{"to": "+2250700000000", "from": "ACME", "text": "two"})
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != http.StatusConflict {
		t.Fatalf("conflicting submit: got %d want 409", second.StatusCode)
	}
	var errBody struct {
		Code string `json:"code"`
	}
	decode(t, second, &errBody)
	if errBody.Code != "idempotency_conflict" {
		t.Errorf("error code = %q, want idempotency_conflict", errBody.Code)
	}
	if h.producer.count() != 1 {
		t.Fatalf("conflict must not publish: got %d publishes, want 1", h.producer.count())
	}
}

func TestSubmitWithoutKeyIgnoresStore(t *testing.T) {
	store := newFakeIdemStore()
	h := buildHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, &fakeCDRReader{}, store)

	resp := postWithKey(t, h, "sgw_key", "", map[string]any{"to": "+2250700000000", "from": "ACME", "text": "hi"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit: got %d want 202", resp.StatusCode)
	}
	store.mu.Lock()
	n := len(store.entries)
	store.mu.Unlock()
	if n != 0 {
		t.Fatalf("no key means the store is untouched, got %d entries", n)
	}
}

// TestSubmitIdempotentConcurrentSinglePublish drives N concurrent submits of the same key + body and
// asserts exactly one publish reaches mt.inbound and every caller gets the same 202 id — the step's
// end-to-end invariant. Run under -race.
func TestSubmitIdempotentConcurrentSinglePublish(t *testing.T) {
	h := buildHarness(t, fakePrincipals{principal: activePrincipal(), found: true}, &fakeCDRReader{}, newFakeIdemStore())
	body := map[string]any{"to": "+2250700000000", "from": "ACME", "text": "concurrent"}

	const n = 16
	var (
		wg     sync.WaitGroup
		got2xx atomic.Int64
		ids    sync.Map
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp := postWithKey(t, h, "sgw_key", "race-key", body)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode == http.StatusAccepted {
				got2xx.Add(1)
				var b restapi.AcceptedMessage
				decode(t, resp, &b)
				ids.Store(b.ID, struct{}{})
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := got2xx.Load(); got != n {
		t.Fatalf("all %d concurrent submits should get 202, got %d", n, got)
	}
	if h.producer.count() != 1 {
		t.Fatalf("exactly 1 publish under concurrency, got %d", h.producer.count())
	}
	distinct := 0
	ids.Range(func(any, any) bool { distinct++; return true })
	if distinct != 1 {
		t.Fatalf("all callers must see the same message id, got %d distinct", distinct)
	}
}
