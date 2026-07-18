package pipeline_test

import (
	"context"
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
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: connector}}, "")

	out, err := p.Process(context.Background(), inbound("+2250700000000"))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.To != "+2250700000000" {
		t.Errorf("dest not normalized: %q", out.To)
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
	p := pipeline.New(tracer, stubResolver{route: pipeline.Route{ConnectorID: uuid.New()}}, "")

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

func TestPipelineRejectsNoRoute(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(tracer, stubResolver{err: errs.ErrNoRoute}, "")

	_, err := p.Process(context.Background(), inbound("+2250700000000"))
	if code, _ := errs.CodeOf(err); code != errs.ErrNoRoute {
		t.Fatalf("code: got %q want no_route", code)
	}
	if !rec.Recorded("pipeline.route") {
		t.Error("route span should have been emitted")
	}
}
