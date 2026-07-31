package adminapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

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
