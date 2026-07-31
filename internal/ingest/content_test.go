package ingest_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// --- fakes ---

type fakeCDR struct {
	mu   sync.Mutex
	rows []clickhouse.CDRRow
}

func (f *fakeCDR) InsertBatch(_ context.Context, rows []clickhouse.CDRRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, rows...)
	return nil
}
func (f *fakeCDR) captured() []clickhouse.CDRRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clickhouse.CDRRow(nil), f.rows...)
}

type fakePolicy struct{ p cp.ContentStorage }

func (f fakePolicy) For(uuid.UUID) cp.ContentStorage { return f.p }

type fakeKeys struct {
	dk  content.DataKey
	err error
}

func (f fakeKeys) Get(context.Context, uuid.UUID) (content.DataKey, error) { return f.dk, f.err }

// writeOne runs the accepted row+body through the writer and returns the single captured CDR row.
func writeOne(t *testing.T, sealer *ingest.ContentSealer, row clickhouse.CDRRow, body msg.Body) clickhouse.CDRRow {
	t.Helper()
	cdr := &fakeCDR{}
	w := ingest.NewAcceptedWriter(cdr, sealer, 1, 16, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	w.Enqueue(row, body)
	cancel() // force the worker to drain the queued item and flush
	<-done
	got := cdr.captured()
	if len(got) != 1 {
		t.Fatalf("captured %d rows, want 1", len(got))
	}
	return got[0]
}

func baseRow() (clickhouse.CDRRow, uuid.UUID, uuid.UUID) {
	cust, msgID := uuid.New(), uuid.New()
	return clickhouse.CDRRow{
		MessageID: msgID, CustomerID: cust, Direction: clickhouse.DirectionMT,
		SourceAddr: "SENDER", DestAddr: "33612345678", Status: clickhouse.StatusAccepted,
	}, cust, msgID
}

const secretBody = "PIN is 4731 — do not share"

// TestOffStoresNoContent: with policy off, the CDR carries no body at all.
func TestOffStoresNoContent(t *testing.T) {
	sealer := ingest.NewContentSealer(fakePolicy{cp.ContentOff}, fakeKeys{}, nil, nil)
	row, _, _ := baseRow()
	got := writeOne(t, sealer, row, msg.NewBodyString(secretBody))
	if got.ContentCiphertext != nil || got.ContentKeyID != nil {
		t.Errorf("off policy stored content: ciphertext=%v keyID=%v", got.ContentCiphertext, got.ContentKeyID)
	}
}

// TestPlaintextStoresClearInColumn: with policy stored_plaintext, the body lands in content_ciphertext as
// clear text, with no key id.
func TestPlaintextStoresClearInColumn(t *testing.T) {
	sealer := ingest.NewContentSealer(fakePolicy{cp.ContentStoredPlaintext}, fakeKeys{}, nil, nil)
	row, _, _ := baseRow()
	got := writeOne(t, sealer, row, msg.NewBodyString(secretBody))
	if got.ContentCiphertext == nil || *got.ContentCiphertext != secretBody {
		t.Errorf("plaintext content = %v, want %q", got.ContentCiphertext, secretBody)
	}
	if got.ContentKeyID != nil {
		t.Errorf("plaintext must not set a key id, got %v", got.ContentKeyID)
	}
}

// TestEncryptedStoresCiphertextAndRoundTrips: with policy stored_encrypted, content_ciphertext is an
// envelope (not the clear body) plus the key id, and it decrypts back to the original under the DEK.
func TestEncryptedStoresCiphertextAndRoundTrips(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	keyID := uuid.New()
	sealer := ingest.NewContentSealer(fakePolicy{cp.ContentStoredEncrypted}, fakeKeys{dk: content.DataKey{KeyID: keyID, DEK: dek}}, nil, nil)
	row, cust, msgID := baseRow()

	got := writeOne(t, sealer, row, msg.NewBodyString(secretBody))
	if got.ContentCiphertext == nil {
		t.Fatal("encrypted policy stored no content")
	}
	if strings.Contains(*got.ContentCiphertext, secretBody) {
		t.Fatal("ciphertext exposes the clear body")
	}
	if got.ContentKeyID == nil || *got.ContentKeyID != keyID {
		t.Fatalf("content_key_id = %v, want %s", got.ContentKeyID, keyID)
	}
	plain, err := content.OpenBody(dek, cust, keyID, msgID, []byte(*got.ContentCiphertext))
	if err != nil {
		t.Fatalf("OpenBody: %v", err)
	}
	if string(plain) != secretBody {
		t.Errorf("round-trip = %q, want %q", plain, secretBody)
	}
}

// TestEncryptedDegradesWhenKeyUnavailable: a DEK-fetch failure drops the content (counter++) but still writes
// the row — the message and its CDR survive a billing-svc blip.
func TestEncryptedDegradesWhenKeyUnavailable(t *testing.T) {
	dropped := prometheus.NewCounter(prometheus.CounterOpts{Name: "content_dropped_total"})
	sealer := ingest.NewContentSealer(fakePolicy{cp.ContentStoredEncrypted}, fakeKeys{err: context.DeadlineExceeded}, dropped, nil)
	row, _, _ := baseRow()

	got := writeOne(t, sealer, row, msg.NewBodyString(secretBody))
	if got.ContentCiphertext != nil || got.ContentKeyID != nil {
		t.Errorf("degraded write should store no content, got ciphertext=%v", got.ContentCiphertext)
	}
	if got.MessageID != row.MessageID {
		t.Error("the row itself must still be written")
	}
	if n := testutil.ToFloat64(dropped); n != 1 {
		t.Errorf("dropped counter = %v, want 1", n)
	}
}

// TestInvariantANoBodyLeakUnderEveryMode: under off, plaintext and encrypted, the clear body appears in NO CDR
// field other than content_ciphertext (and there only for plaintext; encrypted stores a ciphertext; off
// stores nothing). This is invariant (a) re-verified per storage policy.
func TestInvariantANoBodyLeakUnderEveryMode(t *testing.T) {
	dek, _ := content.GenerateDataKey()
	modes := []struct {
		name   string
		sealer *ingest.ContentSealer
	}{
		{"off", ingest.NewContentSealer(fakePolicy{cp.ContentOff}, fakeKeys{}, nil, nil)},
		{"plaintext", ingest.NewContentSealer(fakePolicy{cp.ContentStoredPlaintext}, fakeKeys{}, nil, nil)},
		{"encrypted", ingest.NewContentSealer(fakePolicy{cp.ContentStoredEncrypted}, fakeKeys{dk: content.DataKey{KeyID: uuid.New(), DEK: dek}}, nil, nil)},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			row, _, _ := baseRow()
			got := writeOne(t, m.sealer, row, msg.NewBodyString(secretBody))
			// No non-content field may carry the clear body.
			for name, v := range map[string]string{
				"source_addr": got.SourceAddr, "dest_addr": got.DestAddr, "status": string(got.Status),
			} {
				if strings.Contains(v, secretBody) {
					t.Errorf("clear body leaked into %s: %q", name, v)
				}
			}
			// The clear body may appear ONLY in content_ciphertext, and only under plaintext.
			if got.ContentCiphertext != nil && strings.Contains(*got.ContentCiphertext, secretBody) && m.name != "plaintext" {
				t.Errorf("%s: clear body found in content_ciphertext", m.name)
			}
		})
	}
}
