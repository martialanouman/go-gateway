package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
)

func loggerConfig() config.Config {
	return config.Config{
		ServiceName: "router-svc",
		Environment: config.EnvDevelopment,
		LogLevel:    "info",
	}
}

// decode parses one JSON log record, failing the test if the output is not valid JSON — a log
// pipeline that cannot parse a record drops it.
func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(b), &rec); err != nil {
		t.Fatalf("log record is not valid JSON: %v\n%s", err, b)
	}
	return rec
}

func TestNewLoggerEmitsJSONWithServiceTag(t *testing.T) {
	var buf bytes.Buffer

	logger, err := observability.NewLogger(&buf, loggerConfig())
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}
	logger.Info("accepted", observability.KeyMessageID, "0199a1b2")

	rec := decode(t, buf.Bytes())
	if rec[observability.KeyService] != "router-svc" {
		t.Errorf("service = %v, want router-svc", rec[observability.KeyService])
	}
	if rec["environment"] != "development" {
		t.Errorf("environment = %v, want development", rec["environment"])
	}
	if rec["msg"] != "accepted" {
		t.Errorf("msg = %v, want accepted", rec["msg"])
	}
	if rec[observability.KeyMessageID] != "0199a1b2" {
		t.Errorf("message_id = %v, want 0199a1b2", rec[observability.KeyMessageID])
	}
}

func TestNewLoggerHonoursLevel(t *testing.T) {
	tests := []struct {
		level       string
		wantVisible map[string]bool // level name -> should appear
	}{
		{"debug", map[string]bool{"debug": true, "info": true, "warn": true, "error": true}},
		{"info", map[string]bool{"debug": false, "info": true, "warn": true, "error": true}},
		{"warn", map[string]bool{"debug": false, "info": false, "warn": true, "error": true}},
		{"error", map[string]bool{"debug": false, "info": false, "warn": false, "error": true}},
	}

	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			cfg := loggerConfig()
			cfg.LogLevel = tc.level

			var buf bytes.Buffer
			logger, err := observability.NewLogger(&buf, cfg)
			if err != nil {
				t.Fatalf("NewLogger() error = %v", err)
			}

			logger.Debug("dbg")
			logger.Info("inf")
			logger.Warn("wrn")
			logger.Error("err")

			out := buf.String()
			for name, want := range map[string]string{"debug": "dbg", "info": "inf", "warn": "wrn", "error": "err"} {
				if got := strings.Contains(out, `"`+want+`"`); got != tc.wantVisible[name] {
					t.Errorf("at level %s, %s record visible = %v, want %v", tc.level, name, got, tc.wantVisible[name])
				}
			}
		})
	}
}

func TestNewLoggerRejectsBadLevel(t *testing.T) {
	cfg := loggerConfig()
	cfg.LogLevel = "verbose"

	if _, err := observability.NewLogger(&bytes.Buffer{}, cfg); err == nil {
		t.Error("NewLogger() accepted an unknown level, want an error")
	}
}

// TestLoggerStampsTraceContext: a log line must be pivotable to its trace. Without this, a
// correlated log and trace cannot be joined at all.
func TestLoggerStampsTraceContext(t *testing.T) {
	var buf bytes.Buffer
	logger, err := observability.NewLogger(&buf, loggerConfig())
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("test").Start(context.Background(), "pipeline.submit")
	defer span.End()

	logger.InfoContext(ctx, "routed")

	rec := decode(t, buf.Bytes())
	wantTrace := span.SpanContext().TraceID().String()
	if rec[observability.KeyTraceID] != wantTrace {
		t.Errorf("trace_id = %v, want %s", rec[observability.KeyTraceID], wantTrace)
	}
	if rec[observability.KeySpanID] != span.SpanContext().SpanID().String() {
		t.Errorf("span_id = %v, want %s", rec[observability.KeySpanID], span.SpanContext().SpanID())
	}
}

// TestLoggerOmitsTraceContextWithoutSpan: startup and background loops have no span, and stamping
// them with a zero trace id would only add noise that looks like a real id.
func TestLoggerOmitsTraceContextWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	logger, err := observability.NewLogger(&buf, loggerConfig())
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	logger.InfoContext(context.Background(), "starting")

	rec := decode(t, buf.Bytes())
	if _, present := rec[observability.KeyTraceID]; present {
		t.Errorf("trace_id present without a span: %v", rec[observability.KeyTraceID])
	}
}

// TestLoggerPreservesAttrsAndTraceThroughWith guards the custom handler's WithAttrs: a wrapper
// that forgets to delegate silently drops attributes, and the trace stamp must survive it. With
// is the idiomatic way to bind context to a logger here, so this is the case that matters.
func TestLoggerPreservesAttrsAndTraceThroughWith(t *testing.T) {
	var buf bytes.Buffer
	logger, err := observability.NewLogger(&buf, loggerConfig())
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "pipeline.submit")
	defer span.End()

	logger.With(observability.KeyAccountID, "acct-42").
		InfoContext(ctx, "routed", "connector", "orange-ci")

	rec := decode(t, buf.Bytes())
	if rec[observability.KeyAccountID] != "acct-42" {
		t.Errorf("account_id = %v, want acct-42 (dropped by With)", rec[observability.KeyAccountID])
	}
	if rec["connector"] != "orange-ci" {
		t.Errorf("connector = %v, want orange-ci", rec["connector"])
	}
	if rec[observability.KeyTraceID] != span.SpanContext().TraceID().String() {
		t.Errorf("trace_id lost through With: %v", rec[observability.KeyTraceID])
	}
	if rec[observability.KeyService] != "router-svc" {
		t.Errorf("service = %v, want router-svc (dropped by With)", rec[observability.KeyService])
	}
}

// TestLoggerWithGroupNestsTheTraceStamp pins the documented constraint rather than leaving it to
// be discovered in production: an open group captures the record-level trace stamp, so trace_id
// moves under the group instead of staying at the top level the log index keys on. If this ever
// needs to change, the handler must stamp at the root — see traceHandler's doc comment.
func TestLoggerWithGroupNestsTheTraceStamp(t *testing.T) {
	var buf bytes.Buffer
	logger, err := observability.NewLogger(&buf, loggerConfig())
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "pipeline.submit")
	defer span.End()

	logger.WithGroup("mt").InfoContext(ctx, "routed", "connector", "orange-ci")

	rec := decode(t, buf.Bytes())
	if _, atTopLevel := rec[observability.KeyTraceID]; atTopLevel {
		t.Error("trace_id is now at the top level under WithGroup — the constraint documented on " +
			"traceHandler no longer holds; update the doc comment")
	}
	group, ok := rec["mt"].(map[string]any)
	if !ok {
		t.Fatalf("group mt missing or not an object: %v", rec["mt"])
	}
	if group[observability.KeyTraceID] != span.SpanContext().TraceID().String() {
		t.Errorf("mt.trace_id = %v, want the trace id", group[observability.KeyTraceID])
	}
}

// TestInvariantA_LoggerNeverLeaksBody is invariant (a) at the logger the services actually use:
// the redaction must hold through this handler, not just a bare slog handler.
func TestInvariantA_LoggerNeverLeaksBody(t *testing.T) {
	const plaintext = "PLAINTEXT-CANARY-Votre code est 123456"

	cfg := loggerConfig()
	cfg.LogLevel = "debug"

	var buf bytes.Buffer
	logger, err := observability.NewLogger(&buf, cfg)
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	body := msg.NewBodyString(plaintext)
	logger.Info("submit", "body", body)
	logger.Debug("submit", "body", body)
	logger.With("body", body).Info("submit")
	logger.WithGroup("mt").Info("submit", "body", body)

	if strings.Contains(buf.String(), plaintext) {
		t.Fatalf("INVARIANT (a) VIOLATED: body plaintext leaked through the service logger:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), msg.Redacted) {
		t.Errorf("body did not render as %q:\n%s", msg.Redacted, buf.String())
	}
}
