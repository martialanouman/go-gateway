package observability

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// RecordSpanError marks a span as failed without letting an error's text escape into the trace.
//
// A span leaves the process: it is exported to a collector, stored, indexed and displayed. Copying an
// arbitrary err.Error() into it is the same mistake grpcerr exists to prevent on the wire, and it is the one
// place a message body can reach a trace without anyone writing the body down — an error only has to wrap a
// value that happens to contain it.
//
// So ONLY the flat error code (§11.3) is published — as the status description and as the exception event.
// Never the error's own text, not even when it carries a known code: carrying a code says nothing about who
// wrote the text. internal/storage/postgres/pgerr.go is the proof — it classifies any driver failure as
// errors.Join(err, ErrInternal), so a "classified" error routinely holds pgconn's message, complete with
// host=, user= and whatever value the failing statement carried. The full error still reaches the structured
// log, which does not leave the process.
//
// Pass a nil error to leave the span untouched — a stage that succeeded says nothing.
func RecordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	code, known := errs.CodeOf(err)
	if !known || !code.Valid() {
		code = errs.ErrInternal
	}
	span.SetStatus(codes.Error, string(code))
	span.RecordError(code)
}
