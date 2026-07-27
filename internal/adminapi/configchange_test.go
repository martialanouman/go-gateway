package adminapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/martialanouman/go-gateway/internal/adminapi"
)

// fakeChangePublisher records Publish calls (and can fail) for the config-change middleware tests.
type fakeChangePublisher struct {
	mu       sync.Mutex
	channels []string
	err      error
}

func (p *fakeChangePublisher) Publish(_ context.Context, channel string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.channels = append(p.channels, channel)
	return nil
}

func (p *fakeChangePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.channels)
}

// handlerReturning is a stub whose response status the test controls.
func handlerReturning(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
}

func TestPublishConfigChangesOnSuccessfulMutation(t *testing.T) {
	pub := &fakeChangePublisher{}
	h := adminapi.PublishConfigChanges(handlerReturning(http.StatusCreated), pub, "config:changed", nil)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/exact-routes", nil))
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", w.Code)
	}
	if pub.count() != 1 || pub.channels[0] != "config:changed" {
		t.Errorf("published %v, want one event on config:changed", pub.channels)
	}
}

func TestNoPublishOnRead(t *testing.T) {
	pub := &fakeChangePublisher{}
	h := adminapi.PublishConfigChanges(handlerReturning(http.StatusOK), pub, "config:changed", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/admin/exact-routes", nil))
	if pub.count() != 0 {
		t.Errorf("a GET published %d events, want 0", pub.count())
	}
}

func TestNoPublishOnFailedMutation(t *testing.T) {
	pub := &fakeChangePublisher{}
	h := adminapi.PublishConfigChanges(handlerReturning(http.StatusUnprocessableEntity), pub, "config:changed", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/admin/exact-routes", nil))
	if pub.count() != 0 {
		t.Errorf("a 422 published %d events, want 0 (only a successful write announces)", pub.count())
	}
}

func TestPublishErrorDoesNotFailRequest(t *testing.T) {
	pub := &fakeChangePublisher{err: errors.New("redis down")}
	h := adminapi.PublishConfigChanges(handlerReturning(http.StatusNoContent), pub, "config:changed", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/admin/exact-routes/2250700000001", nil))
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204 — a publish failure must not change the response", w.Code)
	}
}
