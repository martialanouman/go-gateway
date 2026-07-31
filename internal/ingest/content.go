package ingest

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// ContentPolicyResolver resolves a customer's effective content-storage policy (off/stored_plaintext/
// stored_encrypted). *content.PolicySnapshot satisfies it. Declared consumer-side.
type ContentPolicyResolver interface {
	For(customerID uuid.UUID) cp.ContentStorage
}

// DataKeyProvider fetches (cached) a customer's active content data key. *content.DataKeyCache satisfies it.
type DataKeyProvider interface {
	Get(ctx context.Context, customerID uuid.UUID) (content.DataKey, error)
}

// ContentSealer fills a CDR row's content columns from the message body per the customer's effective storage
// policy, at the async accepted-row write. It is the ONE place a body is written to the CDR (§6.14) — into the
// dedicated content_ciphertext column and nowhere else, so invariant (a) still holds (nothing logs the body).
//
// It never fails a row: when an encrypted customer's data key cannot be fetched (billing-svc down) it stores
// the CDR WITHOUT content and counts the drop, so a blip costs the body of one CDR, not the CDR itself and
// never the message (which is already durable in Kafka).
type ContentSealer struct {
	policy  ContentPolicyResolver
	keys    DataKeyProvider
	dropped prometheus.Counter
	logger  *slog.Logger
}

// NewContentSealer wires the sealer. dropped counts bodies dropped because the data key was unavailable.
func NewContentSealer(policy ContentPolicyResolver, keys DataKeyProvider, dropped prometheus.Counter, logger *slog.Logger) *ContentSealer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ContentSealer{policy: policy, keys: keys, dropped: dropped, logger: logger}
}

// Policy resolves the effective content-storage policy for a customer. It is read on the request path (a
// lock-free map lookup) to decide whether the accepted row must carry the body to the async sealer at all.
func (s *ContentSealer) Policy(customerID uuid.UUID) cp.ContentStorage {
	return s.policy.For(customerID)
}

// seal mutates the row's content columns for policy, using body. off → nothing; stored_plaintext → the clear
// body in content_ciphertext (no key id); stored_encrypted → SealBody under the customer's DEK plus its key
// id. A DEK-fetch or seal failure drops the content (counter++) and leaves the row otherwise intact.
func (s *ContentSealer) seal(ctx context.Context, row *clickhouse.CDRRow, body msg.Body, policy cp.ContentStorage) {
	switch policy {
	case cp.ContentStoredPlaintext:
		plaintext := string(body.Reveal())
		row.ContentCiphertext = &plaintext
	case cp.ContentStoredEncrypted:
		dk, err := s.keys.Get(ctx, row.CustomerID)
		if err != nil {
			s.dropContent(ctx, row.MessageID, "content data key unavailable", err)
			return
		}
		env, err := content.SealBody(dk.DEK, row.CustomerID, dk.KeyID, row.MessageID, body.Reveal())
		if err != nil {
			s.dropContent(ctx, row.MessageID, "content seal failed", err)
			return
		}
		sealed := string(env)
		keyID := dk.KeyID
		row.ContentCiphertext = &sealed
		row.ContentKeyID = &keyID
	default:
		// ContentOff (or already-resolved unknown): store no body.
	}
}

func (s *ContentSealer) dropContent(ctx context.Context, messageID uuid.UUID, reason string, err error) {
	if s.dropped != nil {
		s.dropped.Inc()
	}
	// message_id only — never the body or the error's key material.
	s.logger.WarnContext(ctx, "storing CDR without body: "+reason, "message_id", messageID, "err", err)
}
