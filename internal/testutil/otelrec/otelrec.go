// Package otelrec is an in-memory OpenTelemetry span recorder for tests. It gives a test its own
// tracer provider — never the global one — so several services wired into one test process each
// emit into the same recorder, and the test can assert on the spans they produced: that every
// pipeline stage opened its span (plan §6 M2), and that invariant (a) holds — no message body ever
// reaches a span.
package otelrec

import (
	"context"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Recorder captures ended spans in memory.
type Recorder struct {
	sr *tracetest.SpanRecorder
	tp *sdktrace.TracerProvider
}

// New builds a Recorder backed by its own tracer provider. Register it for shutdown with t so
// buffered spans flush at the end of the test.
func New(t testing.TB) *Recorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return &Recorder{sr: sr, tp: tp}
}

// Provider is the recorder's tracer provider, to pass into a service's Deps.
func (r *Recorder) Provider() trace.TracerProvider { return r.tp }

// Tracer returns a named tracer from the recorder's provider.
func (r *Recorder) Tracer(name string) trace.Tracer { return r.tp.Tracer(name) }

// Names returns the names of every ended span, in end order.
func (r *Recorder) Names() []string {
	ended := r.sr.Ended()
	out := make([]string, 0, len(ended))
	for _, s := range ended {
		out = append(out, s.Name())
	}
	return out
}

// Ended returns the recorded spans, in end order, for assertions the named helpers do not cover —
// status codes and descriptions, in particular.
func (r *Recorder) Ended() []sdktrace.ReadOnlySpan { return r.sr.Ended() }

// Recorded reports whether a span with the given name has ended.
func (r *Recorder) Recorded(name string) bool {
	for _, s := range r.sr.Ended() {
		if s.Name() == name {
			return true
		}
	}
	return false
}

// Leaks returns the names of ended spans in which secret appears anywhere — the span name, the status
// description, an attribute key or value, or an event's name, attribute keys or values — which would be a
// body leak (invariant a). It is empty when nothing leaked. AssertNoBody wraps it; it is exported so the
// detection itself can be unit-tested.
func (r *Recorder) Leaks(secret string) []string {
	if secret == "" {
		return nil
	}
	var out []string
	for _, s := range r.sr.Ended() {
		if spanContains(s, secret) {
			out = append(out, s.Name())
		}
	}
	return out
}

// AssertNoBody fails the test if secret appears in any recorded span. Pass the plaintext message
// body: the assertion proves it never escaped into a span through a stray Reveal().
func (r *Recorder) AssertNoBody(t testing.TB, secret string) {
	t.Helper()
	if leaked := r.Leaks(secret); len(leaked) > 0 {
		t.Fatalf("message body leaked into spans %v (invariant a)", leaked)
	}
}

func spanContains(s sdktrace.ReadOnlySpan, secret string) bool {
	if strings.Contains(s.Name(), secret) {
		return true
	}
	// The status description is the easiest place for a body to escape: SetStatus(codes.Error,
	// err.Error()) copies whatever text an error carries into an exported span. It was the one field this
	// detector did not read, so the invariant-(a) assertion could not have caught that vector.
	if strings.Contains(s.Status().Description, secret) {
		return true
	}
	for _, a := range s.Attributes() {
		if strings.Contains(string(a.Key), secret) || strings.Contains(a.Value.String(), secret) {
			return true
		}
	}
	for _, e := range s.Events() {
		if strings.Contains(e.Name, secret) {
			return true
		}
		for _, a := range e.Attributes {
			if strings.Contains(string(a.Key), secret) || strings.Contains(a.Value.String(), secret) {
				return true
			}
		}
	}
	return false
}
