package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// newContentEraseAPI builds the Admin API with a verifier granting the operator token the content:erase scope.
func newContentEraseAPI(t *testing.T, deps adminapi.Deps) http.Handler {
	t.Helper()
	v, err := auth.NewStaticVerifier([]string{operatorToken + ":content:erase"})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	deps.Verifier = v
	mux, _ := adminapi.New(deps)
	return mux
}

type fakeContentKeyRotator struct {
	view   adminapi.ContentKeyView
	err    error
	gotID  uuid.UUID
	called bool
}

func (r *fakeContentKeyRotator) Rotate(_ context.Context, customerID uuid.UUID) (adminapi.ContentKeyView, error) {
	r.called = true
	r.gotID = customerID
	if r.err != nil {
		return adminapi.ContentKeyView{}, r.err
	}
	return r.view, nil
}

// TestRotateContentKeyReturnsMetadata: a rotate returns 200 with the new key's metadata (id, status), never
// any key material, and forwards the customer id to billing-svc.
func TestRotateContentKeyReturnsMetadata(t *testing.T) {
	cust := uuid.New()
	keyID := uuid.New()
	rotator := &fakeContentKeyRotator{view: adminapi.ContentKeyView{
		ID: keyID.String(), CustomerID: cust.String(), KMSKeyRef: "local/v1", Status: "active", CreatedAt: "2026-07-31T10:00:00Z",
	}}
	api := newTestAPIWith(t, adminapi.Deps{ContentKeys: rotator})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+cust.String()+"/content/rotate-key", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if !rotator.called || rotator.gotID != cust {
		t.Fatalf("rotator got id %s, want %s (called=%v)", rotator.gotID, cust, rotator.called)
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["id"] != keyID.String() || got["status"] != "active" || got["kms_key_ref"] != "local/v1" {
		t.Errorf("body = %v, want the rotated key metadata", got)
	}
	if _, leaked := got["wrapped_key"]; leaked {
		t.Error("response exposes wrapped_key — key material must never leave billing-svc")
	}
}

// TestRotateContentKeyUnknownCustomerIs404: billing-svc reporting the customer absent surfaces as 404.
func TestRotateContentKeyUnknownCustomerIs404(t *testing.T) {
	rotator := &fakeContentKeyRotator{err: errs.ErrNotFound}
	api := newTestAPIWith(t, adminapi.Deps{ContentKeys: rotator})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+uuid.NewString()+"/content/rotate-key", ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
}

type fakeContentKeyEraser struct {
	count  int
	err    error
	gotID  uuid.UUID
	called bool
}

func (e *fakeContentKeyEraser) Erase(_ context.Context, customerID uuid.UUID) (int, error) {
	e.called = true
	e.gotID = customerID
	return e.count, e.err
}

// TestEraseCustomerContentCryptoShreds: erase-customer-content delegates the shred and returns a 202 with a
// completed async job reporting the destroyed-key count.
func TestEraseCustomerContentCryptoShreds(t *testing.T) {
	cust := uuid.New()
	eraser := &fakeContentKeyEraser{count: 2}
	api := newContentEraseAPI(t, adminapi.Deps{ContentKeyEraser: eraser})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+cust.String()+"/content/erase", ""))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	if !eraser.called || eraser.gotID != cust {
		t.Fatalf("eraser got id %s, want %s (called=%v)", eraser.gotID, cust, eraser.called)
	}
	var job map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job["status"] != "completed" || job["finished_at"] == nil {
		t.Errorf("job = %v, want a completed job", job)
	}
}

// TestEraseCustomerContentServiceDownIs503: billing-svc unreachable is a retryable 503.
func TestEraseCustomerContentServiceDownIs503(t *testing.T) {
	eraser := &fakeContentKeyEraser{err: errs.ErrServiceUnavailable}
	api := newContentEraseAPI(t, adminapi.Deps{ContentKeyEraser: eraser})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+uuid.NewString()+"/content/erase", ""))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
	}
}

// TestEraseCustomerContentRequiresContentEraseScope: a token without content:erase is refused (403).
func TestEraseCustomerContentRequiresContentEraseScope(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{ContentKeyEraser: &fakeContentKeyEraser{}}) // admin:read|admin:write, not content:erase
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+uuid.NewString()+"/content/erase", ""))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (missing content:erase); body=%s", w.Code, w.Body.String())
	}
}

// TestRotateContentKeyServiceDownIs503: billing-svc unreachable is a retryable 503.
func TestRotateContentKeyServiceDownIs503(t *testing.T) {
	rotator := &fakeContentKeyRotator{err: errs.ErrServiceUnavailable}
	api := newTestAPIWith(t, adminapi.Deps{ContentKeys: rotator})
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+uuid.NewString()+"/content/rotate-key", ""))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", w.Code, w.Body.String())
	}
}
