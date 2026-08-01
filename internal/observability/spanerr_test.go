package observability_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"

	"github.com/martialanouman/go-gateway/internal/observability"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// TestRecordSpanErrorKeepsAnUnknownErrorsTextInTheProcess is the invariant-(a) barrier for traces. An error
// nobody classified may carry anything — including a message body it only had to wrap — and a span is
// exported, stored and indexed outside this process.
func TestRecordSpanErrorKeepsAnUnknownErrorsTextInTheProcess(t *testing.T) {
	const body = "MEET ME AT MIDNIGHT"
	rec := otelrec.New(t)

	_, span := rec.Tracer("test").Start(context.Background(), "pipeline.probe")
	observability.RecordSpanError(span, fmt.Errorf("encode message %q: %w", body, errors.New("bad rune")))
	span.End()

	rec.AssertNoBody(t, body)
	if leaked := rec.Leaks("bad rune"); len(leaked) > 0 {
		t.Errorf("the unclassified error's text reached spans %v", leaked)
	}
}

// TestRecordSpanErrorRedactsAClassifiedErrorsWrapping is the regression test for the hole this barrier had
// on its first attempt: "carries a known code" was taken to mean "the text is ours", which is false.
// postgres.translate wraps ANY driver failure as errors.Join(err, ErrInternal) — a classified error whose
// text is pgconn's, complete with host=, user= and whatever value the failing statement carried. The reserve
// stage's suppression lookup reaches it, so this was live.
func TestRecordSpanErrorRedactsAClassifiedErrorsWrapping(t *testing.T) {
	const body = "MEET ME AT MIDNIGHT"
	rec := otelrec.New(t)

	// The exact shape internal/storage/postgres/pgerr.go produces.
	driverFault := errors.New(`failed to connect to "host=db.internal user=gw": ` + body)
	classified := fmt.Errorf("is suppressed: %w", errors.Join(driverFault, errs.ErrInternal))

	_, span := rec.Tracer("test").Start(context.Background(), "pipeline.opt_out")
	observability.RecordSpanError(span, classified)
	span.End()

	rec.AssertNoBody(t, body)
	if leaked := rec.Leaks("db.internal"); len(leaked) > 0 {
		t.Errorf("the deployment topology leaked into spans %v", leaked)
	}
	if leaked := rec.Leaks("is suppressed"); len(leaked) > 0 {
		t.Errorf("the wrapping context leaked into spans %v; only the flat code may be published", leaked)
	}
}

// TestRecordSpanErrorPublishesTheFlatCode: a reader must still be able to act on the failure, and the flat
// code is the vocabulary shared with HTTP, SMPP and the CDR (§11.3).
func TestRecordSpanErrorPublishesTheFlatCode(t *testing.T) {
	rec := otelrec.New(t)

	_, span := rec.Tracer("test").Start(context.Background(), "pipeline.opt_out")
	observability.RecordSpanError(span, fmt.Errorf("check suppression: %w", errs.ErrRecipientOptedOut))
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("got %d spans, want 1", len(ended))
	}
	status := ended[0].Status()
	if status.Code != codes.Error {
		t.Errorf("status code = %v, want Error", status.Code)
	}
	if status.Description != string(errs.ErrRecipientOptedOut) {
		t.Errorf("description = %q, want the flat code %q", status.Description, errs.ErrRecipientOptedOut)
	}
	// The exception event carries the code and nothing else — not the context we wrapped it with. That
	// context is diagnostic gold in a log, which stays in the process; a span does not.
	var recorded bool
	for _, e := range ended[0].Events() {
		if e.Name != "exception" {
			continue
		}
		recorded = true
		for _, a := range e.Attributes {
			if strings.Contains(a.Value.String(), "check suppression") {
				t.Errorf("the wrapping context reached the exception event: %s", a.Value.String())
			}
		}
	}
	if !recorded {
		t.Error("a failure should still be recorded as an exception event")
	}
}

// TestRecordSpanErrorHidesTheTextOfAnUnknownError: the status must not degrade to silence either — an
// unclassified failure still reads as an internal error.
func TestRecordSpanErrorHidesTheTextOfAnUnknownError(t *testing.T) {
	rec := otelrec.New(t)

	_, span := rec.Tracer("test").Start(context.Background(), "pipeline.probe")
	observability.RecordSpanError(span, errors.New("dial secret-host:5432: connection refused"))
	span.End()

	ended := rec.Ended()
	if got := ended[0].Status().Description; got != string(errs.ErrInternal) {
		t.Errorf("description = %q, want the generic %q", got, errs.ErrInternal)
	}
	if leaked := rec.Leaks("secret-host"); len(leaked) > 0 {
		t.Errorf("internals leaked into spans %v", leaked)
	}
}

// TestRecordSpanErrorIgnoresSuccess: a stage that worked says nothing.
func TestRecordSpanErrorIgnoresSuccess(t *testing.T) {
	rec := otelrec.New(t)

	_, span := rec.Tracer("test").Start(context.Background(), "pipeline.probe")
	observability.RecordSpanError(span, nil)
	span.End()

	if got := rec.Ended()[0].Status().Code; got == codes.Error {
		t.Error("a nil error marked the span failed")
	}
}
