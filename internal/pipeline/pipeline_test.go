package pipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

type stubResolver struct {
	route pipeline.Route
	err   error
}

func (s stubResolver) Resolve(context.Context, string) (pipeline.Route, error) {
	return s.route, s.err
}

// stubAuthorizer authorizes a source address, or rejects it with a fixed error. The zero value
// allows everything.
type stubAuthorizer struct{ err error }

func (s stubAuthorizer) Authorize(context.Context, uuid.UUID, uuid.UUID, string) error { return s.err }

// stubOptOut answers the opt-out check with fixed values. The zero value passes every message.
type stubOptOut struct {
	optedOut bool
	err      error
}

func (s stubOptOut) IsOptedOut(context.Context, uuid.UUID, uuid.UUID, string, string) (bool, error) {
	return s.optedOut, s.err
}

// capturingOptOut records the arguments it was called with, so a test can assert the pipeline
// forwards them in the right positions (accountID vs customerID must not be swapped).
type capturingOptOut struct {
	accountID, customerID uuid.UUID
	from, dest            string
}

func (c *capturingOptOut) IsOptedOut(_ context.Context, accountID, customerID uuid.UUID, from, dest string) (bool, error) {
	c.accountID, c.customerID, c.from, c.dest = accountID, customerID, from, dest
	return false, nil
}

// allStages is the frozen ordered set of spans the pipeline must emit, STUBs included (plan §6).
var allStages = []string{
	"pipeline.e164",
	"pipeline.sender_id",
	"pipeline.opt_out",
	"pipeline.anti_spam",
	"pipeline.route",
	"pipeline.encoding",
	"pipeline.rate_limit",
	"pipeline.credit",
}

func inbound(to string) pipeline.InboundMT {
	return pipeline.InboundMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		From: "GATEWAY", To: to, Body: msg.NewBodyString("topsecretbody"),
		Encoding: "auto", SubmittedAt: time.Now().UTC(),
	}
}

func TestPipelineHappyPathEmitsEveryStageSpan(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	connector := uuid.New()
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: connector}}, stubAuthorizer{}, stubOptOut{})

	out, err := p.Process(context.Background(), inbound("+2250700000000"))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.To != "2250700000000" {
		t.Errorf("dest not normalized to digits-only form: %q", out.To)
	}
	if out.ConnectorID != connector {
		t.Errorf("connector: got %s want %s", out.ConnectorID, connector)
	}
	if out.Encoding != "gsm7" {
		t.Errorf("encoding: got %q want gsm7 (auto resolves to gsm7 in M2)", out.Encoding)
	}
	if out.SegmentCount != 1 {
		t.Errorf("segment_count: got %d want 1", out.SegmentCount)
	}

	for _, name := range allStages {
		if !rec.Recorded(name) {
			t.Errorf("stage span %q not emitted; got %v", name, rec.Names())
		}
	}
	// invariant (a): the body never reaches a span.
	rec.AssertNoBody(t, "topsecretbody")
}

func TestPipelineRejectsInvalidDestination(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: uuid.New()}}, stubAuthorizer{}, stubOptOut{})

	_, err := p.Process(context.Background(), inbound("not-a-number"))
	if code, _ := errs.CodeOf(err); code != errs.ErrInvalidDestination {
		t.Fatalf("code: got %q want invalid_destination", code)
	}
	// E.164 ran and failed; route resolution was never reached.
	if !rec.Recorded("pipeline.e164") {
		t.Error("e164 span should have been emitted")
	}
	if rec.Recorded("pipeline.route") {
		t.Error("route span must NOT be emitted after an E.164 rejection")
	}
}

// TestPipelineRejectsUnauthorizedSenderID: the sender-ID stage rejects an unauthorized source with
// sender_id_not_authorized, before route resolution, and never leaks the body into a span.
func TestPipelineRejectsUnauthorizedSenderID(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: uuid.New()}},
		stubAuthorizer{err: errs.ErrSenderIDNotAuthorized}, stubOptOut{})

	_, err := p.Process(context.Background(), inbound("+2250700000000"))
	if code, _ := errs.CodeOf(err); code != errs.ErrSenderIDNotAuthorized {
		t.Fatalf("code: got %q want sender_id_not_authorized", code)
	}
	if !rec.Recorded("pipeline.sender_id") {
		t.Error("sender_id span should have been emitted")
	}
	if rec.Recorded("pipeline.route") {
		t.Error("route span must NOT be emitted after a sender-ID rejection (frozen order, invariant b)")
	}
	rec.AssertNoBody(t, "topsecretbody")
}

// TestPipelineRejectsOptedOutRecipient: the opt-out stage blocks a suppressed destination with
// recipient_opted_out, after sender-ID but before route resolution, and never leaks the body.
func TestPipelineRejectsOptedOutRecipient(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: uuid.New()}},
		stubAuthorizer{}, stubOptOut{optedOut: true})

	_, err := p.Process(context.Background(), inbound("+2250700000000"))
	if code, _ := errs.CodeOf(err); code != errs.ErrRecipientOptedOut {
		t.Fatalf("code: got %q want recipient_opted_out", code)
	}
	if !rec.Recorded("pipeline.opt_out") {
		t.Error("opt_out span should have been emitted")
	}
	if rec.Recorded("pipeline.route") {
		t.Error("route span must NOT be emitted after an opt-out rejection (frozen order, invariant b)")
	}
	rec.AssertNoBody(t, "topsecretbody")
}

// TestPipelineOptOutTransientErrorIsNotACode: a store fault in the opt-out check surfaces as a
// non-code error (the router retries), never a rejection code — a message must not be dropped as
// opted-out because the database blinked.
func TestPipelineOptOutTransientErrorIsNotACode(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: uuid.New()}},
		stubAuthorizer{}, stubOptOut{err: errors.New("suppressions store down")})

	_, err := p.Process(context.Background(), inbound("+2250700000000"))
	if err == nil {
		t.Fatal("expected an error from the opt-out store fault")
	}
	if code, ok := errs.CodeOf(err); ok {
		t.Fatalf("transient fault must not carry a rejection code, got %q", code)
	}
}

// TestPipelineForwardsOptOutIdentifiers locks the call-site wiring: the pipeline must pass the
// account id and customer id to the opt-out check in the positions its interface declares, and the
// NORMALIZED destination. A swap would check the customer scope under the account's id — a silent
// regulatory false negative.
func TestPipelineForwardsOptOutIdentifiers(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	spy := &capturingOptOut{}
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: uuid.New()}}, stubAuthorizer{}, spy)

	in := inbound("+2250700000000")
	if _, err := p.Process(context.Background(), in); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if spy.accountID != in.AccountID {
		t.Errorf("accountID forwarded = %s, want %s", spy.accountID, in.AccountID)
	}
	if spy.customerID != in.CustomerID {
		t.Errorf("customerID forwarded = %s, want %s", spy.customerID, in.CustomerID)
	}
	if spy.from != in.From {
		t.Errorf("from forwarded = %q, want %q", spy.from, in.From)
	}
	if spy.dest != "2250700000000" {
		t.Errorf("dest forwarded = %q, want the normalized destination", spy.dest)
	}
}

func TestPipelineRejectsNoRoute(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(tracer, stubResolver{err: errs.ErrNoRoute}, stubAuthorizer{}, stubOptOut{})

	_, err := p.Process(context.Background(), inbound("+2250700000000"))
	if code, _ := errs.CodeOf(err); code != errs.ErrNoRoute {
		t.Fatalf("code: got %q want no_route", code)
	}
	if !rec.Recorded("pipeline.route") {
		t.Error("route span should have been emitted")
	}
}
