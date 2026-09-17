package postgres

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// AuditLogRepo appends the consolidated operator audit trail (control_plane.audit_log, step-290c).
type AuditLogRepo struct {
	q *sqlcgen.Queries
}

// NewAuditLogRepo returns the audit-log repository backed by pool.
func NewAuditLogRepo(pool *pgxpool.Pool) *AuditLogRepo {
	return &AuditLogRepo{q: sqlcgen.New(pool)}
}

// maxRequestIDLen bounds the stored request id. chi echoes a client's X-Request-Id header verbatim, so an
// audited operator would otherwise choose how much of this immutable table their own row occupies.
const maxRequestIDLen = 64

// Begin records a request before its handler runs and returns the row to complete with Finish.
func (r *AuditLogRepo) Begin(ctx context.Context, in cp.AuditIntent) (uuid.UUID, error) {
	var reqID *string
	if in.RequestID != "" {
		// Bounded and made valid UTF-8: a header carrying a raw byte would otherwise make the INSERT fail,
		// and a failed intent refuses the request (503).
		id := strings.ToValidUTF8(in.RequestID, "")
		// Bound the bytes before converting to runes: a 1 MiB header would otherwise allocate 4 MiB per
		// request. Cutting mid-rune is harmless — the conversion below replaces the remnant.
		if len(id) > maxRequestIDLen*utf8.UTFMax {
			id = id[:maxRequestIDLen*utf8.UTFMax]
		}
		if runes := []rune(id); len(runes) > maxRequestIDLen {
			id = string(runes[:maxRequestIDLen])
		}
		if id != "" { // a header made only of invalid bytes sanitises to nothing: that is no id at all
			reqID = &id
		}
	}
	id, err := r.q.InsertAuditIntent(ctx, sqlcgen.InsertAuditIntentParams{
		Operator:    in.Operator,
		OperationID: in.OperationID,
		Method:      in.Method,
		Target:      in.Target,
		RequestID:   reqID,
	})
	if err != nil {
		return uuid.Nil, translate("record audit intent", err)
	}
	return id, nil
}

// Finish records the HTTP status of an audited request. It writes once: a row that already has an outcome
// keeps it, and this reports no error for that case — the row simply keeps the first outcome. A value that
// is not an HTTP status is refused rather than written, so a row reads as NULL ("outcome not recorded") or
// as a status, never as 0; the column CHECK refuses it too, for any writer.
func (r *AuditLogRepo) Finish(ctx context.Context, id uuid.UUID, status int) error {
	if status < 100 || status > 599 {
		return fmt.Errorf("record audit outcome: %d is not an HTTP status", status)
	}
	s := int16(status) // bounded to 100..599 on the line above
	if err := r.q.FinishAudit(ctx, sqlcgen.FinishAuditParams{ID: id, Status: &s}); err != nil {
		return translate("record audit outcome", err)
	}
	return nil
}
