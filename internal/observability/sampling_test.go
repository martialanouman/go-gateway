package observability_test

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/martialanouman/go-gateway/internal/observability"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// exported builds a provider that drops everything by head sampling (ratio 0) and returns the spans that
// nonetheless reached the exporter.
func exported(t *testing.T, emit func(tp *sdktrace.TracerProvider)) []string {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(observability.ErrorBiasedSampler(0)),
		sdktrace.WithSpanProcessor(observability.ErrorBiased(sr)),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	emit(tp)
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	ended := sr.Ended()
	names := make([]string, 0, len(ended))
	for _, s := range ended {
		names = append(names, s.Name())
	}
	return names
}

// TestErrorSpansSurviveAZeroRatio is the step's acceptance criterion, and the promise TRACES_SAMPLER_ARG has
// been documenting since M0: a failure is traced whatever the ratio. Head sampling decides before the outcome
// exists, so a plain ratio drops precisely the traces worth keeping.
func TestErrorSpansSurviveAZeroRatio(t *testing.T) {
	names := exported(t, func(tp *sdktrace.TracerProvider) {
		_, span := tp.Tracer("test").Start(context.Background(), "pipeline.opt_out")
		observability.RecordSpanError(span, errs.ErrRecipientOptedOut)
		span.End()
	})

	if len(names) != 1 || names[0] != "pipeline.opt_out" {
		t.Fatalf("exported %v, want the failed span to survive a 0 ratio", names)
	}
}

// TestSuccessfulSpansStillObeyTheRatio: the other half. AlwaysRecord makes every span reach the processors,
// so without the filter the ratio would silently mean nothing and every span would ship.
func TestSuccessfulSpansStillObeyTheRatio(t *testing.T) {
	names := exported(t, func(tp *sdktrace.TracerProvider) {
		_, span := tp.Tracer("test").Start(context.Background(), "pipeline.e164")
		span.End()
	})

	if len(names) != 0 {
		t.Fatalf("exported %v at ratio 0, want nothing: the ratio is not being honoured", names)
	}
}

// TestEverythingShipsAtRatioOne: the default configuration must be unchanged by all of this.
func TestEverythingShipsAtRatioOne(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(observability.ErrorBiasedSampler(1)),
		sdktrace.WithSpanProcessor(observability.ErrorBiased(sr)),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	_, span := tp.Tracer("test").Start(context.Background(), "pipeline.e164")
	span.End()
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	if got := len(sr.Ended()); got != 1 {
		t.Errorf("exported %d spans at ratio 1, want 1", got)
	}
}

// TestErrorSpansReachTheRealBatchProcessor exercises what tracing.go actually wires. The other tests wrap a
// SpanRecorder, which accepts everything; the whole design instead rests on BatchSpanProcessor no longer
// filtering on the sampled flag — a behaviour that HAS changed across SDK versions. If it comes back, error
// traces stop being exported and every other test here stays green.
func TestErrorSpansReachTheRealBatchProcessor(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(observability.ErrorBiasedSampler(0)),
		sdktrace.WithSpanProcessor(observability.ErrorBiased(sdktrace.NewBatchSpanProcessor(exp))),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	_, failed := tp.Tracer("test").Start(context.Background(), "connector.submit")
	observability.RecordSpanError(failed, errs.ErrSubmitFailed)
	failed.End()
	_, ok := tp.Tracer("test").Start(context.Background(), "pipeline.e164")
	ok.End()

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want only the failed one", len(spans))
	}
	if spans[0].Name != "connector.submit" {
		t.Errorf("exported %q, want connector.submit", spans[0].Name)
	}
}

// TestUnsampledSpansAreStillRecorded pins the mechanism the whole design rests on: the sampler must return
// RecordOnly, not Drop, or the processor never sees a span to judge. If a future SDK changes that, this fails
// here rather than as silently missing error traces in production.
func TestUnsampledSpansAreStillRecorded(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(observability.ErrorBiasedSampler(0)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	_, span := tp.Tracer("test").Start(context.Background(), "pipeline.e164")
	defer span.End()

	if !span.IsRecording() {
		t.Fatal("an unsampled span is not recording; the error-biased processor would never see it")
	}
	if span.SpanContext().IsSampled() {
		t.Error("the ratio was ignored: the span is marked sampled")
	}
}
