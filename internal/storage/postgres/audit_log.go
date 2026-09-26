package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// AuditLogRepo appends the consolidated operator audit trail (control_plane.audit_log, step-290c).
type AuditLogRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// NewAuditLogRepo returns the audit-log repository backed by pool.
func NewAuditLogRepo(pool *pgxpool.Pool) *AuditLogRepo {
	return &AuditLogRepo{pool: pool, q: sqlcgen.New(pool)}
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

// List reads the trail newest first, filtered by f, after the keyset position when one is given.
func (r *AuditLogRepo) List(ctx context.Context, f cp.AuditLogFilter, limit int, after *cp.AuditLogKey) ([]cp.AuditEntry, error) {
	params := sqlcgen.ListAuditLogParams{
		FromAt: tsFromPtr(f.From),
		ToAt:   tsFromPtr(f.To),
		Lim:    int32(limit), //nolint:gosec // limit is a small bounded page size
	}
	if f.Operator != "" {
		params.Operator = &f.Operator
	}
	if after != nil {
		params.AfterAt = tsFrom(after.At)
		params.AfterID = &after.ID
	}
	rows, err := r.q.ListAuditLog(ctx, params)
	if err != nil {
		return nil, translate("list audit log", err)
	}
	out := make([]cp.AuditEntry, 0, len(rows))
	for _, row := range rows {
		e := cp.AuditEntry{
			ID: row.ID, Operator: row.Operator, OperationID: row.OperationID, Method: row.Method,
			Target: row.Target, RequestID: row.RequestID, At: tsVal(row.At), FinishedAt: tsPtr(row.FinishedAt),
		}
		if row.Status != nil {
			s := int(*row.Status)
			e.Status = &s
		}
		out = append(out, e)
	}
	return out, nil
}

// Purge deletes the rows older than retention and reports how many. SET LOCAL acts only inside a transaction,
// hence BeginFunc; under the trigger's floor, the whole DELETE is refused (ADR-0018).
func (r *AuditLogRepo) Purge(ctx context.Context, retention time.Duration) (int64, error) {
	var purged int64
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL audit_log.purge = 'on'`); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM control_plane.audit_log WHERE at < now() - make_interval(secs => $1)`,
			retention.Seconds())
		purged = tag.RowsAffected()
		return err
	})
	if err != nil {
		return 0, translate("purge audit log", err)
	}
	return purged, nil
}

// RunRetention purges the rows older than retention now, then every interval, until ctx ends. 0 disables it.
// A failed pass is logged, never returned: a retention fault must not take the service down.
func (r *AuditLogRepo) RunRetention(ctx context.Context, every, retention time.Duration, logger *slog.Logger) error {
	if every <= 0 {
		<-ctx.Done()
		return nil
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		purged, err := r.Purge(ctx, retention)
		switch {
		case err != nil && ctx.Err() == nil:
			logger.ErrorContext(ctx, "audit log retention pass failed", "err", err)
		case purged > 0:
			logger.InfoContext(ctx, "audit log retention pass", "purged", purged)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
