package adminapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// AuditLogStore records the consolidated operator audit trail (step-290c). *postgres.AuditLogRepo
// satisfies it; the interface lives here, consumer-side.
type AuditLogStore interface {
	Begin(ctx context.Context, in cp.AuditIntent) (uuid.UUID, error)
	Finish(ctx context.Context, id uuid.UUID, status int) error
}

// auditTimeout bounds each audit write, the way publishTimeout bounds the config-change publish.
const auditTimeout = 5 * time.Second

// revealReads are the reads that show subscriber numbers in clear when the caller holds msisdn:reveal.
// They change nothing, but the act of seeing is what the trail must hold. get-message-content is NOT here:
// content_access_audit already records it, with an outcome this table has no column for.
var revealReads = map[string]bool{
	"search-messages":   true,
	"get-message-trace": true,
	"list-suppressions": true,
	"list-unrouted-mo":  true,
}

// unconditionalReads are reads recorded whatever the caller's scopes, because what they hand over is not
// masked by one: get-message-export returns the download URL of an export whose artefact may itself be
// unmasked, and cdr:export_bulk alone opens it.
var unconditionalReads = map[string]bool{
	"get-message-export": true,
	// The MNP override table is a list of subscriber numbers, returned in clear under admin:read alone —
	// the widest subscriber-data read the Admin API serves, and no scope marks it as such.
	"list-exact-routes": true,
}

// auditMiddleware writes the audit row of every audited request before its handler runs, and its HTTP
// status after. It must be the LAST middleware registered: it reads the status from the huma context it
// passed on, and a middleware added after it that derived a new context (huma.WithValue does) would leave
// this one reading 0. It must also run AFTER auth.Middleware: the principal it records is the one that middleware put
// on the context, and a request that middleware refused never reaches here — an unauthenticated caller
// cannot make Postgres write.
//
// No row, no action: when the intent cannot be written the request is answered 503 and the handler never
// runs — the rule recordGranted already applies to content reads. That 503 is not enumerated per operation,
// like every infrastructure fault (see humaspec.Prune). The outcome is best-effort: the action already
// happened, so a lost outcome is logged rather than surfaced, and reads as NULL — never as success.
func auditMiddleware(api huma.API, store AuditLogStore, logger *slog.Logger) func(huma.Context, func(huma.Context)) {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx huma.Context, next func(huma.Context)) {
		operationID := ctx.Operation().OperationID
		method := ctx.Method()
		target := ctx.URL().Path
		if !audited(ctx.Context(), operationID, method, target) {
			next(ctx)
			return
		}

		beginCtx, cancel := context.WithTimeout(ctx.Context(), auditTimeout)
		id, err := store.Begin(beginCtx, cp.AuditIntent{
			Operator:    operatorSubject(ctx.Context()),
			OperationID: operationID,
			Method:      method,
			Target:      target,
			RequestID:   chimiddleware.GetReqID(ctx.Context()),
		})
		cancel()
		if err != nil {
			level := slog.LevelError
			if errors.Is(err, context.Canceled) {
				level = slog.LevelDebug // the client hung up; nothing is wrong with the trail
			}
			logger.Log(ctx.Context(), level, "audit intent not recorded; request refused",
				// The target is deliberately absent: an exact-route path IS a subscriber number, and no
				// access log carries it today.
				"operation", operationID, "operator", operatorSubject(ctx.Context()), "err", err)
			status, ok := errs.HTTPStatus(errs.ErrServiceUnavailable)
			if !ok {
				status = http.StatusServiceUnavailable
			}
			_ = huma.WriteErr(api, ctx, status, "service unavailable", errs.ErrServiceUnavailable)
			return
		}

		defer func() {
			status := ctx.Status()
			if status == 0 {
				// Nothing was written: the handler panicked, and the recoverer upstream answers 500. Reading
				// the status rather than a "did it panic" flag keeps the row honest when a response was
				// already sent before the panic. A streaming handler also leaves 0 (huma never sets a status
				// on that path) — none is audited today, and one entering the trail would need its own rule.
				status = http.StatusInternalServerError
			}
			finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx.Context()), auditTimeout)
			defer cancel()
			if err := store.Finish(finishCtx, id, status); err != nil {
				logger.WarnContext(ctx.Context(), "audit outcome not recorded",
					"operation", operationID, "audit_id", id, "err", err)
			}
		}()
		next(ctx)
	}
}

// audited reports whether a request enters the trail: every write, and a read that reveals numbers.
func audited(ctx context.Context, operationID, method, path string) bool {
	if !readOnlyRequest(method, path) {
		return true
	}
	if unconditionalReads[operationID] {
		return true
	}
	return revealReads[operationID] && mayRevealMSISDN(ctx)
}
