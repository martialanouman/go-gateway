package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"

	"github.com/martialanouman/go-gateway/internal/config"
)

// ShutdownFunc releases a telemetry pipeline, flushing what it still holds. It is called during
// the graceful drain, with a bounded context: a collector that has gone away must not keep a pod
// from terminating.
type ShutdownFunc func(context.Context) error

// InitTracing installs the global tracer provider and the W3C propagators, and returns the
// function that drains it.
//
// Spans carry identifiers and decisions, never a message body nor a secret (guide de codage §12).
// Sampling here is head-based: the ratio is decided when the trace starts, before its outcome is
// known. The "100% of errors" rule of spec §6.11 is therefore a tail-sampling policy configured
// on the collector, not something this SDK can decide — the ratio below only thins successful
// traces.
//
// When the SDK is disabled, no exporter connection is opened and the returned shutdown is a
// no-op; instrumented code keeps working against the API's no-op tracer.
func InitTracing(ctx context.Context, cfg config.Config) (ShutdownFunc, error) {
	// Propagators are installed either way: a disabled SDK must still forward the trace context
	// it receives, or it would sever traces that merely pass through this service.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.OTel.Disabled {
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTel.Endpoint)}
	if cfg.OTel.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter for %s: %w", cfg.OTel.Endpoint, err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.DeploymentEnvironmentNameKey.String(string(cfg.Environment)),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// ParentBased keeps a trace whole: once an upstream service has decided to sample a
		// trace, every downstream span of it is kept, so a sampled trace is never full of holes.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.OTel.SampleRatio))),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
