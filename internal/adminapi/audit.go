package adminapi

import (
	"context"
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
}

// auditMiddleware writes the audit row of every audited request before its handler runs, and its HTTP
// status after. It must run AFTER auth.Middleware: the principal it records is the one that middleware put
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
			logger.ErrorContext(ctx.Context(), "audit intent not recorded; request refused",
				"operation", operationID, "err", err)
			status, ok := errs.HTTPStatus(errs.ErrServiceUnavailable)
			if !ok {
				status = http.StatusServiceUnavailable
			}
			_ = huma.WriteErr(api, ctx, status, "service unavailable", errs.ErrServiceUnavailable)
			return
		}

		completed := false
		defer func() {
			status := ctx.Status()
			if !completed {
				// The handler panicked: the recoverer upstream answers 500, and the panic keeps unwinding
				// through this deferred write.
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
		completed = true
	}
}

// audited reports whether a request enters the trail: every write, and a read that reveals numbers.
func audited(ctx context.Context, operationID, method, path string) bool {
	if !readOnlyRequest(method, path) {
		return true
	}
	return revealReads[operationID] && mayRevealMSISDN(ctx)
}
