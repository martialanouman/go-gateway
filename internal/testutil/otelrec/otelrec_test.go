package otelrec_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"

	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

func TestRecorderCapturesSpans(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "test")

	_, span := tracer.Start(context.Background(), "pipeline.e164")
	span.End()

	if !rec.Recorded("pipeline.e164") {
		t.Fatalf("span not recorded; got %v", rec.Names())
	}
}

func TestAssertNoBodyPassesWhenBodyMasked(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "test")

	const secret = "the secret message text"
	body := msg.NewBodyString(secret)

	_, span := tracer.Start(context.Background(), "ingest")
	// Attributes carry identifiers and the MASKED body — never the plaintext. msg.Body renders as
	// [REDACTED] through String(), so this is safe and AssertNoBody must pass.
	span.SetAttributes(attribute.String("body", body.String()))
	span.End()

	rec.AssertNoBody(t, secret)
}

func TestAssertNoBodyDetectsLeak(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "test")

	const secret = "leaked plaintext"
	_, span := tracer.Start(context.Background(), "ingest")
	// Simulate a bug: someone put the plaintext body into a span attribute.
	span.SetAttributes(attribute.String("body", secret))
	span.End()

	if leaks := rec.Leaks(secret); len(leaks) == 0 {
		t.Fatal("Leaks should have detected the plaintext in a span attribute")
	}
}
