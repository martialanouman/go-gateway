package msg_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/platform/msg"
)

// plaintext is a distinctive marker: any occurrence of it in a serialized surface is a leak.
const plaintext = "PLAINTEXT-CANARY-Votre code est 123456"

// message mirrors the shape of a real pipeline message: identifiers that MUST be observable
// alongside a body that MUST NOT be.
type message struct {
	MessageID string   `json:"message_id"`
	AccountID string   `json:"account_id"`
	Body      msg.Body `json:"body"`
}

func (m message) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("message_id", m.MessageID),
		slog.String("account_id", m.AccountID),
		slog.Any("body", m.Body),
	)
}

func newMessage() message {
	return message{
		MessageID: "0199a1b2-c3d4-7000-8000-000000000001",
		AccountID: "acct-42",
		Body:      msg.NewBodyString(plaintext),
	}
}

// TestInvariantA_BodyNeverLeaksInSlogJSON is invariant (a) on the logging surface: no slog
// handler, at any level, through any attribute shape, may render the plaintext. Blocking test.
func TestInvariantA_BodyNeverLeaksInSlogJSON(t *testing.T) {
	m := newMessage()

	tests := []struct {
		name string
		log  func(*slog.Logger)
	}{
		{"body as attr", func(l *slog.Logger) { l.Info("submit", "body", m.Body) }},
		{"body via slog.Any", func(l *slog.Logger) { l.Info("submit", slog.Any("body", m.Body)) }},
		{"whole message via LogValuer", func(l *slog.Logger) { l.Info("submit", "msg", m) }},
		{"whole message via slog.Any", func(l *slog.Logger) { l.Info("submit", slog.Any("msg", m)) }},
		{"body pointer", func(l *slog.Logger) { l.Info("submit", "body", &m.Body) }},
		{"struct without LogValuer", func(l *slog.Logger) {
			l.Info("submit", "raw", struct{ B msg.Body }{B: m.Body})
		}},
		{"debug level", func(l *slog.Logger) { l.Debug("submit", "body", m.Body) }},
		{"in group", func(l *slog.Logger) { l.WithGroup("mt").Info("submit", "body", m.Body) }},
		{"in With", func(l *slog.Logger) { l.With("body", m.Body).Info("submit") }},
		{"format verb %v", func(l *slog.Logger) { l.Info(fmt.Sprintf("submit %v", m.Body)) }},
		{"format verb %s", func(l *slog.Logger) { l.Info(fmt.Sprintf("submit %s", m.Body)) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			tc.log(logger)

			out := buf.String()
			if strings.Contains(out, plaintext) {
				t.Fatalf("INVARIANT (a) VIOLATED: body plaintext leaked into a slog JSON record:\n%s", out)
			}
			if !strings.Contains(out, msg.Redacted) {
				t.Errorf("body did not render as %q; got:\n%s", msg.Redacted, out)
			}
		})
	}
}

// TestInvariantA_BodyNeverLeaksInSpanAttribute is invariant (a) on the tracing surface, checked
// against a real in-memory OTel exporter so it covers what an exporter would actually ship.
func TestInvariantA_BodyNeverLeaksInSpanAttribute(t *testing.T) {
	m := newMessage()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})

	ctx, span := tp.Tracer("test").Start(context.Background(), "pipeline.submit")
	span.SetAttributes(
		attribute.String("message_id", m.MessageID),
		attribute.Stringer("body", m.Body),
		attribute.String("body_fmt", fmt.Sprintf("%v", m.Body)),
		attribute.Int("body_len", m.Body.Len()),
	)
	span.AddEvent("encoded", bodyEvent(m))
	span.End()
	_ = ctx

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d exported spans, want 1", len(spans))
	}

	dump, err := json.Marshal(spans)
	if err != nil {
		t.Fatalf("marshal exported spans: %v", err)
	}
	if bytes.Contains(dump, []byte(plaintext)) {
		t.Fatalf("INVARIANT (a) VIOLATED: body plaintext leaked into a span attribute:\n%s", dump)
	}
	if !bytes.Contains(dump, []byte(msg.Redacted)) {
		t.Errorf("body did not render as %q in span attributes:\n%s", msg.Redacted, dump)
	}
}

// bodyEvent builds span event attributes the way pipeline stages will, exercising the same
// stringification path a careless caller would hit.
func bodyEvent(m message) oteltrace.EventOption {
	return oteltrace.WithAttributes(attribute.Stringer("body", m.Body))
}

// TestInvariantA_BodyNeverLeaksInJSON covers encoding/json directly: a struct holding a Body
// must serialize redacted, including when reached through a pointer or a nested struct.
func TestInvariantA_BodyNeverLeaksInJSON(t *testing.T) {
	m := newMessage()

	tests := []struct {
		name string
		v    any
	}{
		{"struct value", m},
		{"struct pointer", &m},
		{"body alone", m.Body},
		{"body pointer", &m.Body},
		{"nested", struct {
			Inner message `json:"inner"`
		}{Inner: m}},
		{"slice", []message{m}},
		{"map", map[string]msg.Body{"body": m.Body}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := json.Marshal(tc.v)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if bytes.Contains(out, []byte(plaintext)) {
				t.Fatalf("INVARIANT (a) VIOLATED: body plaintext leaked into JSON:\n%s", out)
			}
			if !bytes.Contains(out, []byte(msg.Redacted)) {
				t.Errorf("body did not render as %q; got: %s", msg.Redacted, out)
			}
		})
	}
}

// TestRevealReturnsPlaintext pins the escape hatch: Reveal is the one way to the content, and it
// must return it intact — a Body that redacted its own Reveal would be useless.
func TestRevealReturnsPlaintext(t *testing.T) {
	b := msg.NewBodyString(plaintext)

	if got := string(b.Reveal()); got != plaintext {
		t.Errorf("Reveal() = %q, want %q", got, plaintext)
	}
	if got := b.Len(); got != len(plaintext) {
		t.Errorf("Len() = %d, want %d", got, len(plaintext))
	}
}

func TestZeroBodyIsEmptyAndRedacted(t *testing.T) {
	var b msg.Body

	if !b.IsEmpty() {
		t.Error("zero Body should be empty")
	}
	if b.Len() != 0 {
		t.Errorf("zero Body Len() = %d, want 0", b.Len())
	}
	if b.String() != msg.Redacted {
		t.Errorf("zero Body String() = %q, want %q", b.String(), msg.Redacted)
	}
	if b.Reveal() != nil {
		t.Errorf("zero Body Reveal() = %v, want nil", b.Reveal())
	}
}

func TestNewBodyRoundTrip(t *testing.T) {
	b := msg.NewBody([]byte("héllo"))

	if got := string(b.Reveal()); got != "héllo" {
		t.Errorf("Reveal() = %q, want %q", got, "héllo")
	}
	if b.IsEmpty() {
		t.Error("body with content should not be empty")
	}
}
