package observability_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

func tracingConfig() config.Config {
	return config.Config{
		ServiceName: "router-svc",
		Environment: config.EnvDevelopment,
		OTel: config.OTel{
			Disabled:    true,
			Endpoint:    "localhost:4317",
			Insecure:    true,
			SampleRatio: 1.0,
		},
	}
}

// TestInitTracingDisabled: with the SDK off, no exporter connection is opened and shutdown is a
// no-op — a service must not need a collector to boot.
func TestInitTracingDisabled(t *testing.T) {
	cfg := tracingConfig()

	shutdown, err := observability.InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("InitTracing() error = %v", err)
	}
	if shutdown == nil {
		t.Fatal("InitTracing() returned a nil shutdown")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown() = %v, want nil for a disabled SDK", err)
	}
}

// TestInitTracingInstallsPropagatorsWhenDisabled: a disabled SDK must still forward the trace
// context it receives, or it would sever traces that merely pass through this service.
func TestInitTracingInstallsPropagatorsWhenDisabled(t *testing.T) {
	if _, err := observability.InitTracing(context.Background(), tracingConfig()); err != nil {
		t.Fatalf("InitTracing() error = %v", err)
	}

	prop := otel.GetTextMapPropagator()
	if prop == nil {
		t.Fatal("no propagator installed")
	}

	fields := prop.Fields()
	wantFields := map[string]bool{"traceparent": false, "baggage": false}
	for _, f := range fields {
		if _, ok := wantFields[f]; ok {
			wantFields[f] = true
		}
	}
	for f, found := range wantFields {
		if !found {
			t.Errorf("propagator does not carry %q; fields = %v", f, fields)
		}
	}
}

// TestPropagatorRoundTrip proves the installed propagator actually moves a trace context across
// a carrier, which is what keeps a trace whole across services.
func TestPropagatorRoundTrip(t *testing.T) {
	if _, err := observability.InitTracing(context.Background(), tracingConfig()); err != nil {
		t.Fatalf("InitTracing() error = %v", err)
	}

	carrier := propagation.MapCarrier{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	prop := otel.GetTextMapPropagator()

	ctx := prop.Extract(context.Background(), carrier)
	sc := trace(ctx)
	if !sc.IsValid() {
		t.Fatal("extracted span context is invalid; incoming traces would be severed")
	}
	if got, want := sc.TraceID().String(), "4bf92f3577b34da6a3ce929d0e0e4736"; got != want {
		t.Errorf("trace id = %s, want %s", got, want)
	}

	out := propagation.MapCarrier{}
	prop.Inject(ctx, out)
	if out["traceparent"] == "" {
		t.Error("propagator did not inject traceparent; downstream services would start a new trace")
	}
}

// TestInitTracingRejectsUnreachableEndpointLazily documents real OTLP/gRPC behaviour: the
// exporter connects lazily, so InitTracing succeeds even with no collector listening. That is
// deliberate — a collector outage must not stop a service from booting.
func TestInitTracingSucceedsWithoutACollector(t *testing.T) {
	cfg := tracingConfig()
	cfg.OTel.Disabled = false
	cfg.OTel.Endpoint = "127.0.0.1:1" // nothing listens here

	shutdown, err := observability.InitTracing(context.Background(), cfg)
	if err != nil {
		t.Fatalf("InitTracing() error = %v; a missing collector must not block a boot", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Shutdown may report the failed flush; it must not hang or panic.
	_ = shutdown(ctx)
}

// trace extracts the span context from ctx. It is a tiny helper kept local to the test so the
// oteltrace import does not shadow the package's own naming.
func trace(ctx context.Context) oteltrace.SpanContext {
	return oteltrace.SpanContextFromContext(ctx)
}
