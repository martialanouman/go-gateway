// Package observability wires the three telemetry surfaces every service shares: structured
// logging, tracing, and the ops HTTP server carrying metrics and health probes.
//
// One rule governs all three: identifiers and decisions are observable, message content is not.
// Nothing here ever renders a message body — that is invariant (a), enforced by the redacting
// type in internal/platform/msg (guide de codage §11/§12).
package observability

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/config"
)

// Log attribute keys shared across services. Using constants keeps one spelling in the log
// index: "trace_id" and "traceID" would silently become two fields.
const (
	KeyTraceID   = "trace_id"
	KeySpanID    = "span_id"
	KeyService   = "service"
	KeyMessageID = "message_id"
	KeyAccountID = "account_id"
)

// NewLogger builds the JSON logger every service uses (guide de codage §12). It tags every record
// with the service name so records from several services land in one index without ambiguity.
//
// The logger writes to w; pass os.Stdout in a service and a buffer in a test.
func NewLogger(w io.Writer, cfg config.Config) (*slog.Logger, error) {
	level, err := config.ParseLogLevel(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(&traceHandler{Handler: handler}).With(
		slog.String(KeyService, cfg.ServiceName),
		slog.String("environment", string(cfg.Environment)),
	), nil
}

// traceHandler stamps every record with the trace and span id of the context it was logged from,
// so a log line can be pivoted to its trace and back. It adds nothing when the context carries no
// recording span, keeping records from startup and background loops clean.
//
// Constraint: do NOT call WithGroup on this logger. The stamp is applied to the record, so an
// open group captures it and trace_id lands at "<group>.trace_id" instead of the top level the
// log index keys on. Stamping at the root instead would mean rebuilding the handler chain on
// every record — an allocation on a path that runs at 8 000 msg/s — to serve a grouping nothing
// in this codebase needs. Attributes (With) are unaffected and are the idiomatic choice here.
type traceHandler struct {
	slog.Handler
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String(KeyTraceID, sc.TraceID().String()),
			slog.String(KeySpanID, sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(name)}
}
