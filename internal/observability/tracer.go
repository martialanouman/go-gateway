package observability

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns a named tracer from provider. Passing the provider explicitly, rather than reading
// the global one, is what lets several services share a single process — the in-process E2E test —
// each with its own tracer, without any of them mutating global state. A service takes the returned
// trace.Tracer through its Deps and opens spans with tracer.Start; it never calls otel.Tracer
// itself, so the wiring stays testable.
//
// A nil provider falls back to the global tracer provider, which is what a real binary uses after
// InitTracing has installed it.
func Tracer(provider trace.TracerProvider, name string) trace.Tracer {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return provider.Tracer(name)
}
