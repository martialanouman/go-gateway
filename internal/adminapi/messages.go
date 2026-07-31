package adminapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// MessageContentReader reads the aggregated CDR row for a message by its id alone (cross-tenant admin lookup).
// *clickhouse.CDRReader satisfies it; declared consumer-side.
type MessageContentReader interface {
	ByMessageID(ctx context.Context, messageID uuid.UUID) (clickhouse.CDRRow, bool, error)
}

// ContentAuditor appends the content-access audit trail. *postgres.ContentAccessAuditRepo satisfies it.
type ContentAuditor interface {
	Record(ctx context.Context, a cp.ContentAccess) error
}

type messageHandlers struct {
	reader  MessageContentReader
	keys    ContentKeyReader
	auditor ContentAuditor
	logger  *slog.Logger
}

// registerMessages wires get-message-content: the guarded, audited read of a decrypted message body (§14,
// step-163). Every access under content:read leaves an audit row; the plaintext is returned in the HTTP
// response only, never logged (invariant a).
func registerMessages(api huma.API, reader MessageContentReader, keys ContentKeyReader, auditor ContentAuditor, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	h := &messageHandlers{reader: reader, keys: keys, auditor: auditor, logger: logger}
	register(api, huma.Operation{
		OperationID: "get-message-content", Method: http.MethodGet, Path: "/admin/messages/{id}/content",
		Summary: "Decrypt and return a message body (scope content:read, audited)", Tags: []string{"Content & RGPD"},
		Security: scopeSecurity(auth.ScopeContentRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.getContent)
}

// messageContentDTO conforms to api/openapi-admin.yaml MessageContent. encoding is optional.
type messageContentDTO struct {
	MessageID   string  `json:"message_id" format:"uuid"`
	Text        string  `json:"text"`
	Encoding    *string `json:"encoding,omitempty" enum:"gsm7,ucs2,binary"`
	DecryptedAt string  `json:"decrypted_at" format:"date-time"`
}

type messageContentInput struct {
	ID string `path:"id" format:"uuid"`
}
type messageContentOutput struct{ Body messageContentDTO }

func (h *messageHandlers) getContent(ctx context.Context, in *messageContentInput) (*messageContentOutput, error) {
	operator := operatorSubject(ctx)
	messageID, err := uuid.Parse(in.ID)
	if err != nil {
		_ = h.record(ctx, operator, uuid.Nil, nil, cp.ContentAccessNotFound)
		return nil, notFound("message")
	}

	row, found, err := h.reader.ByMessageID(ctx, messageID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		_ = h.record(ctx, operator, messageID, nil, cp.ContentAccessNotFound)
		return nil, notFound("message content")
	}
	cust := row.CustomerID
	// The message exists but its content was never stored (content_storage off) — nothing to return, but the
	// customer is known, so record it on the audit row.
	if row.ContentCiphertext == nil {
		_ = h.record(ctx, operator, messageID, &cust, cp.ContentAccessNotFound)
		return nil, notFound("message content")
	}

	// stored_plaintext: no key id, the content column holds the cleartext.
	if row.ContentKeyID == nil {
		if err := h.recordGranted(ctx, operator, messageID, &cust); err != nil {
			return nil, err
		}
		return h.ok(messageID, *row.ContentCiphertext, row.Encoding), nil
	}

	// stored_encrypted: fetch the DEK for the key the row was sealed under (may be a retired key).
	dek, destroyed, err := h.keys.Fetch(ctx, *row.ContentKeyID)
	if err != nil {
		// A TERMINAL fetch failure (the key row is gone) means the content is unreadable — audit it, mirroring
		// the destroyed path. A transient failure (billing unreachable) is a retryable 503, not an unreadable
		// outcome, so it is not audited as such.
		if !errors.Is(err, errs.ErrServiceUnavailable) {
			_ = h.record(ctx, operator, messageID, &cust, cp.ContentAccessUnreadable)
		}
		return nil, humaerr.FromError(err)
	}
	if destroyed {
		// The key was crypto-shredded: the body is permanently unreadable. Not a raw server error — a clean
		// "no content" to the operator, and an audit row recording the unreadable outcome.
		_ = h.record(ctx, operator, messageID, &cust, cp.ContentAccessUnreadable)
		return nil, notFound("message content")
	}
	plain, derr := content.OpenBody(dek, cust, *row.ContentKeyID, messageID, []byte(*row.ContentCiphertext))
	if derr != nil {
		// A decrypt failure on a live key is a data/integrity fault: log it for ops (never the plaintext or the
		// key), audit it as unreadable, and return a clean "no content" rather than leaking a crypto error.
		h.logger.ErrorContext(ctx, "decrypt message content failed", "message_id", messageID, "err", derr)
		_ = h.record(ctx, operator, messageID, &cust, cp.ContentAccessUnreadable)
		return nil, notFound("message content")
	}
	if err := h.recordGranted(ctx, operator, messageID, &cust); err != nil {
		return nil, err
	}
	return h.ok(messageID, string(plain), row.Encoding), nil
}

// recordGranted writes the granted audit row and REFUSES to return the plaintext if it cannot: there is no
// unaudited content access (§14). A best-effort audit would defeat the audit's purpose.
func (h *messageHandlers) recordGranted(ctx context.Context, operator string, messageID uuid.UUID, cust *uuid.UUID) error {
	if err := h.record(ctx, operator, messageID, cust, cp.ContentAccessGranted); err != nil {
		return humaerr.FromError(fmt.Errorf("audit content access: %w", errs.ErrServiceUnavailable))
	}
	return nil
}

// record appends one audit row, logging (never the plaintext) on failure and returning the error so the
// granted path can refuse to serve unaudited content.
func (h *messageHandlers) record(ctx context.Context, operator string, messageID uuid.UUID, cust *uuid.UUID, outcome cp.ContentAccessOutcome) error {
	if err := h.auditor.Record(ctx, cp.ContentAccess{Operator: operator, MessageID: messageID, CustomerID: cust, Outcome: outcome}); err != nil {
		h.logger.ErrorContext(ctx, "content access audit write failed", "message_id", messageID, "outcome", outcome, "err", err)
		return err
	}
	return nil
}

func (h *messageHandlers) ok(messageID uuid.UUID, text string, enc clickhouse.Encoding) *messageContentOutput {
	dto := messageContentDTO{MessageID: messageID.String(), Text: text, DecryptedAt: time.Now().UTC().Format(timeRFC3339)}
	switch enc {
	case clickhouse.EncodingGSM7, clickhouse.EncodingUCS2, clickhouse.EncodingBinary:
		e := string(enc)
		dto.Encoding = &e
	}
	return &messageContentOutput{Body: dto}
}

// operatorSubject is the audited operator identity — the authenticated principal's subject.
func operatorSubject(ctx context.Context) string {
	if p, ok := auth.PrincipalFrom(ctx); ok && p.Subject != "" {
		return p.Subject
	}
	return "unknown"
}
