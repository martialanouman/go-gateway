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
	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// --- fakes ---

type fakeMessageReader struct {
	row   clickhouse.CDRRow
	found bool
}

func (f fakeMessageReader) ByMessageID(context.Context, uuid.UUID) (clickhouse.CDRRow, bool, error) {
	return f.row, f.found, nil
}

type fakeContentKeyReader struct {
	dek       []byte
	destroyed bool
	err       error
}

func (f fakeContentKeyReader) Fetch(context.Context, uuid.UUID) ([]byte, bool, error) {
	return f.dek, f.destroyed, f.err
}

type fakeAuditor struct{ recorded []cp.ContentAccess }

func (f *fakeAuditor) Record(_ context.Context, a cp.ContentAccess) error {
	f.recorded = append(f.recorded, a)
	return nil
}

// newContentReadAPI builds the Admin API with a verifier that grants the operator token the content:read
// scope, so the content-read endpoint's scope check passes.
func newContentReadAPI(t *testing.T, deps adminapi.Deps) http.Handler {
	t.Helper()
	v, err := auth.NewStaticVerifier([]string{operatorToken + ":content:read"})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	deps.Verifier = v
	mux, _ := adminapi.New(deps)
	return mux
}

func getContent(t *testing.T, api http.Handler, messageID uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/messages/"+messageID.String()+"/content", ""))
	return w
}

// TestGetMessageContentDecryptsAndAudits: an encrypted body is decrypted and returned under content:read, and
// a granted audit row is written. The audit records the fact of access, never the plaintext.
func TestGetMessageContentDecryptsAndAudits(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	cust, keyID, msgID := uuid.New(), uuid.New(), uuid.New()
	const secret = "the confidential OTP is 8842"
	env, _ := content.SealBody(dek, cust, keyID, msgID, []byte(secret))
	cipher := string(env)

	reader := fakeMessageReader{found: true, row: clickhouse.CDRRow{
		MessageID: msgID, CustomerID: cust, ContentCiphertext: &cipher, ContentKeyID: &keyID, Encoding: clickhouse.EncodingGSM7,
	}}
	audit := &fakeAuditor{}
	api := newContentReadAPI(t, adminapi.Deps{Messages: reader, ContentKeyReader: fakeContentKeyReader{dek: dek}, ContentAudit: audit})

	w := getContent(t, api, msgID)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["text"] != secret || got["encoding"] != "gsm7" {
		t.Errorf("body = %v, want the decrypted text + gsm7", got)
	}
	if len(audit.recorded) != 1 || audit.recorded[0].Outcome != cp.ContentAccessGranted {
		t.Fatalf("audit = %+v, want one granted row", audit.recorded)
	}
	if audit.recorded[0].MessageID != msgID || audit.recorded[0].Operator != operatorToken {
		t.Errorf("audit row = %+v, want operator+message recorded", audit.recorded[0])
	}
}

// TestGetMessageContentPlaintextMode: a stored_plaintext message (no key id) returns the column as-is.
func TestGetMessageContentPlaintextMode(t *testing.T) {
	msgID := uuid.New()
	clear := "plain body"
	reader := fakeMessageReader{found: true, row: clickhouse.CDRRow{MessageID: msgID, CustomerID: uuid.New(), ContentCiphertext: &clear}}
	audit := &fakeAuditor{}
	api := newContentReadAPI(t, adminapi.Deps{Messages: reader, ContentKeyReader: fakeContentKeyReader{}, ContentAudit: audit})

	w := getContent(t, api, msgID)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["text"] != clear {
		t.Errorf("text = %v, want %q", got["text"], clear)
	}
	if len(audit.recorded) != 1 || audit.recorded[0].Outcome != cp.ContentAccessGranted {
		t.Errorf("audit = %+v, want granted", audit.recorded)
	}
}

// TestGetMessageContentDestroyedKeyIsUnreadable: a crypto-shredded key yields 404 and an 'unreadable' audit
// row — a clean response, not a raw server error.
func TestGetMessageContentDestroyedKeyIsUnreadable(t *testing.T) {
	cipher := "irrelevant-ciphertext"
	keyID := uuid.New()
	msgID := uuid.New()
	reader := fakeMessageReader{found: true, row: clickhouse.CDRRow{MessageID: msgID, CustomerID: uuid.New(), ContentCiphertext: &cipher, ContentKeyID: &keyID}}
	audit := &fakeAuditor{}
	api := newContentReadAPI(t, adminapi.Deps{Messages: reader, ContentKeyReader: fakeContentKeyReader{destroyed: true}, ContentAudit: audit})

	w := getContent(t, api, msgID)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
	if len(audit.recorded) != 1 || audit.recorded[0].Outcome != cp.ContentAccessUnreadable {
		t.Errorf("audit = %+v, want unreadable", audit.recorded)
	}
}

// TestGetMessageContentNotFound: an unknown message (or one with no stored content) is 404 with a not_found
// audit row.
func TestGetMessageContentNotFound(t *testing.T) {
	audit := &fakeAuditor{}
	api := newContentReadAPI(t, adminapi.Deps{Messages: fakeMessageReader{found: false}, ContentKeyReader: fakeContentKeyReader{}, ContentAudit: audit})
	w := getContent(t, api, uuid.New())
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
	if len(audit.recorded) != 1 || audit.recorded[0].Outcome != cp.ContentAccessNotFound {
		t.Errorf("audit = %+v, want not_found", audit.recorded)
	}
}

// TestGetMessageContentRequiresContentReadScope: a token without content:read is refused (403) — the guard.
func TestGetMessageContentRequiresContentReadScope(t *testing.T) {
	// newTestAPIWith grants admin:read|admin:write, NOT content:read.
	api := newTestAPIWith(t, adminapi.Deps{Messages: fakeMessageReader{}, ContentKeyReader: fakeContentKeyReader{}, ContentAudit: &fakeAuditor{}})
	w := getContent(t, api, uuid.New())
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (missing content:read); body=%s", w.Code, w.Body.String())
	}
}

// TestGetMessageContentDecryptFailureIsUnreadable: a ciphertext that does not decrypt (wrong/corrupt) yields
// 404 unreadable, never a raw crypto error to the client.
func TestGetMessageContentDecryptFailureIsUnreadable(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	keyID, msgID := uuid.New(), uuid.New()
	garbage := "not-a-valid-envelope"
	reader := fakeMessageReader{found: true, row: clickhouse.CDRRow{MessageID: msgID, CustomerID: uuid.New(), ContentCiphertext: &garbage, ContentKeyID: &keyID}}
	audit := &fakeAuditor{}
	api := newContentReadAPI(t, adminapi.Deps{Messages: reader, ContentKeyReader: fakeContentKeyReader{dek: dek}, ContentAudit: audit})

	w := getContent(t, api, msgID)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
	if len(audit.recorded) != 1 || audit.recorded[0].Outcome != cp.ContentAccessUnreadable {
		t.Errorf("audit = %+v, want unreadable", audit.recorded)
	}
}
