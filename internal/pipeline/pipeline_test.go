package pipeline_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

type stubResolver struct {
	route pipeline.Route
	err   error
}

func (s stubResolver) Resolve(context.Context, pipeline.RouteRequest) (pipeline.Route, error) {
	return s.route, s.err
}

// stubAuthorizer authorizes a source address under a fixed category, or rejects it with a fixed error. The
// zero value allows everything.
type stubAuthorizer struct {
	category cp.TrafficCategory
	err      error
}

func (s stubAuthorizer) Authorize(context.Context, uuid.UUID, string) (cp.TrafficCategory, error) {
	return s.category, s.err
}

// stubOptOut answers the opt-out check with fixed values. The zero value passes every message.
type stubOptOut struct {
	optedOut bool
	err      error
}

func (s stubOptOut) IsOptedOut(context.Context, uuid.UUID, uuid.UUID, string, string) (bool, error) {
	return s.optedOut, s.err
}

// stubAntispam returns a fixed anti-spam action and records the category it was asked to check against.
// The zero value passes every message (empty action).
type stubAntispam struct {
	action   cp.AntispamAction
	err      error
	category *cp.TrafficCategory
}

func (s stubAntispam) Evaluate(_ context.Context, _, _, _ uuid.UUID, _ string, category cp.TrafficCategory, _ string, _ []byte) (cp.AntispamAction, error) {
	if s.category != nil {
		*s.category = category
	}
	return s.action, s.err
}

// stubReserver returns a fixed credit-stage verdict and counts its calls. The zero value reserves nothing
// (billing disabled), so a test that wires it proves the stage ran without a real billing client.
type stubReserver struct {
	reserved  bool
	ownerType string
	err       error
	calls     int
}

func (s *stubReserver) Reserve(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int) (bool, string, error) {
	s.calls++
	return s.reserved, s.ownerType, s.err
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

// allStages is the frozen ordered set of spans the pipeline must emit (plan §6).
var allStages = []string{
	"pipeline.e164",
	"pipeline.sender_id",
	"pipeline.opt_out",
	"pipeline.anti_spam",
	"pipeline.route",
	"pipeline.encoding",
	"pipeline.segment",
	"pipeline.credit",
}

// testDeps is the pipeline every test starts from: stubs that pass every message and no credit
// reserver. A test then names ONLY the collaborator it varies, so what it exercises is
// readable at its call site rather than buried in argument position.
func testDeps(tracer trace.Tracer) pipeline.Deps {
	return pipeline.Deps{
		Tracer:    tracer,
		Resolver:  stubResolver{route: pipeline.Route{ConnectorID: uuid.New()}},
		SenderIDs: stubAuthorizer{},
		OptOut:    stubOptOut{},
		Antispam:  stubAntispam{},
	}
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
	deps := testDeps(tracer)
	deps.Resolver = stubResolver{route: pipeline.Route{ConnectorID: connector}}
	p := pipeline.New(deps)

	out, segs, err := p.Process(context.Background(), inbound("+2250700000000"))
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
	// A short message is exactly one segment, carrying the bare content with no UDH.
	if len(segs) != 1 {
		t.Fatalf("segments: got %d want 1", len(segs))
	}
	if segs[0].Seq != 1 || segs[0].Total != 1 || segs[0].HasUDH {
		t.Errorf("single segment = %+v, want seq 1 / total 1 / no UDH", segs[0])
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
	p := pipeline.New(testDeps(tracer))

	_, _, err := p.Process(context.Background(), inbound("not-a-number"))
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
	deps := testDeps(tracer)
	deps.SenderIDs = stubAuthorizer{err: errs.ErrSenderIDNotAuthorized}
	p := pipeline.New(deps)

	_, _, err := p.Process(context.Background(), inbound("+2250700000000"))
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
	deps := testDeps(tracer)
	deps.OptOut = stubOptOut{optedOut: true}
	p := pipeline.New(deps)

	_, _, err := p.Process(context.Background(), inbound("+2250700000000"))
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
	deps := testDeps(tracer)
	deps.OptOut = stubOptOut{err: errors.New("suppressions store down")}
	p := pipeline.New(deps)

	_, _, err := p.Process(context.Background(), inbound("+2250700000000"))
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
	deps := testDeps(tracer)
	deps.OptOut = spy
	p := pipeline.New(deps)

	in := inbound("+2250700000000")
	if _, _, err := p.Process(context.Background(), in); err != nil {
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

type capturingAntispam struct{ messageID uuid.UUID }

func (s *capturingAntispam) Evaluate(_ context.Context, messageID, _, _ uuid.UUID, _ string, _ cp.TrafficCategory, _ string, _ []byte) (cp.AntispamAction, error) {
	s.messageID = messageID
	return "", nil
}

// TestPipelineForwardsMessageIDToAntispam: the anti-spam state is keyed by message_id so a redelivered
// message is not its own duplicate (step-285c); that only holds if the pipeline hands over the real one.
func TestPipelineForwardsMessageIDToAntispam(t *testing.T) {
	spy := &capturingAntispam{}
	deps := testDeps(observability.Tracer(otelrec.New(t).Provider(), "router"))
	deps.Antispam = spy
	in := inbound("+2250700000000")
	if _, _, err := pipeline.New(deps).Process(context.Background(), in); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if spy.messageID != in.MessageID {
		t.Errorf("messageID forwarded = %s, want %s", spy.messageID, in.MessageID)
	}
}

// TestPipelineRejectsSpamContent: the anti-spam stage blocks a message with content_blocked, after
// opt-out but before route resolution, and never leaks the body.
func TestPipelineRejectsSpamContent(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	deps := testDeps(tracer)
	deps.Antispam = stubAntispam{action: cp.AntispamActionBlock}
	p := pipeline.New(deps)

	_, _, err := p.Process(context.Background(), inbound("+2250700000000"))
	if code, _ := errs.CodeOf(err); code != errs.ErrContentBlocked {
		t.Fatalf("code: got %q want content_blocked", code)
	}
	if !rec.Recorded("pipeline.anti_spam") {
		t.Error("anti_spam span should have been emitted")
	}
	if rec.Recorded("pipeline.route") {
		t.Error("route span must NOT be emitted after an anti-spam block (frozen order, invariant b)")
	}
	rec.AssertNoBody(t, "topsecretbody")
}

// TestPipelineSpamFlagDoesNotBlock: a flag/throttle action annotates but never stops the message —
// it routes normally.
func TestPipelineSpamFlagDoesNotBlock(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	deps := testDeps(tracer)
	deps.Antispam = stubAntispam{action: cp.AntispamActionFlag}
	p := pipeline.New(deps)

	if _, _, err := p.Process(context.Background(), inbound("+2250700000000")); err != nil {
		t.Fatalf("a flagged message must still route: %v", err)
	}
	if !rec.Recorded("pipeline.route") {
		t.Error("a flagged message must reach route resolution")
	}
	rec.AssertNoBody(t, "topsecretbody")
}

// TestPipelineChecksTheTrafficAgainstTheSendersCategory: anti-spam judges a message by the category its
// sender ID declares (ADR-0020 §5), read at the sender-ID stage that runs before it.
func TestPipelineChecksTheTrafficAgainstTheSendersCategory(t *testing.T) {
	deps := testDeps(observability.Tracer(otelrec.New(t).Provider(), "router"))
	deps.SenderIDs = stubAuthorizer{category: cp.TrafficOTP}
	var seen cp.TrafficCategory
	deps.Antispam = stubAntispam{category: &seen}

	if _, _, err := pipeline.New(deps).Process(context.Background(), inbound("+2250700000000")); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if seen != cp.TrafficOTP {
		t.Fatalf("anti-spam checked against %q, want the sender's otp", seen)
	}
}

// TestPipelineCarriesTheEffectivePriority: the requested priority survives the pipeline, bounded by the
// sender's category (ADR-0020 §2), and the category travels with it to mt.routed.
func TestPipelineCarriesTheEffectivePriority(t *testing.T) {
	for _, c := range []struct {
		category  cp.TrafficCategory
		requested int
		want      uint8
	}{
		// Above the transactional ceiling: neither the request nor the default may pass through.
		{cp.TrafficTransactional, 3, 2},
		{cp.TrafficMarketing, 3, 0},
	} {
		deps := testDeps(observability.Tracer(otelrec.New(t).Provider(), "router"))
		deps.SenderIDs = stubAuthorizer{category: c.category}
		in := inbound("+2250700000000")
		in.Priority = c.requested

		routed, _, err := pipeline.New(deps).Process(context.Background(), in)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if routed.TrafficCategory != c.category || routed.Priority != c.want {
			t.Errorf("routed category/priority = %q/%d, want %q/%d", routed.TrafficCategory, routed.Priority, c.category, c.want)
		}
	}
}

// TestPipelineRejectionCarriesTheCategoryOnceKnown: a rejection after the sender-ID stage returns the
// category and priority with its error, so the rejected CDR row can be filtered by category (step-293);
// one before it has nothing to return.
func TestPipelineRejectionCarriesTheCategoryOnceKnown(t *testing.T) {
	for name, c := range map[string]struct {
		reject func(*pipeline.Deps)
		code   errs.Code
	}{
		"opt-out":   {func(d *pipeline.Deps) { d.OptOut = stubOptOut{optedOut: true} }, errs.ErrRecipientOptedOut},
		"anti-spam": {func(d *pipeline.Deps) { d.Antispam = stubAntispam{action: cp.AntispamActionBlock} }, errs.ErrContentBlocked},
		"route":     {func(d *pipeline.Deps) { d.Resolver = stubResolver{err: errs.ErrNoRoute} }, errs.ErrNoRoute},
		"credit":    {func(d *pipeline.Deps) { d.Credit = &stubReserver{err: errs.ErrInsufficientCredit} }, errs.ErrInsufficientCredit},
	} {
		deps := testDeps(observability.Tracer(otelrec.New(t).Provider(), "router"))
		deps.SenderIDs = stubAuthorizer{category: cp.TrafficTransactional}
		c.reject(&deps)
		in := inbound("+2250700000000")
		in.Priority = 3

		routed, _, err := pipeline.New(deps).Process(context.Background(), in)
		if code, _ := errs.CodeOf(err); code != c.code {
			t.Fatalf("%s: err = %v, want %s", name, err, c.code)
		}
		if routed.TrafficCategory != cp.TrafficTransactional || routed.Priority != 2 {
			t.Errorf("%s: rejected category/priority = %q/%d, want transactional/2", name, routed.TrafficCategory, routed.Priority)
		}
	}

	deps := testDeps(observability.Tracer(otelrec.New(t).Provider(), "router"))
	deps.SenderIDs = stubAuthorizer{err: errs.ErrSenderIDNotAuthorized}
	routed, _, _ := pipeline.New(deps).Process(context.Background(), inbound("+2250700000000"))
	if routed.TrafficCategory != "" {
		t.Errorf("a sender-ID rejection carries category %q, want none", routed.TrafficCategory)
	}
}

func TestPipelineRejectsNoRoute(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	deps := testDeps(tracer)
	deps.Resolver = stubResolver{err: errs.ErrNoRoute}
	p := pipeline.New(deps)

	_, _, err := p.Process(context.Background(), inbound("+2250700000000"))
	if code, _ := errs.CodeOf(err); code != errs.ErrNoRoute {
		t.Fatalf("code: got %q want no_route", code)
	}
	if !rec.Recorded("pipeline.route") {
		t.Error("route span should have been emitted")
	}
}

// TestPipelineReserveSetsBillableAndOwner: a successful reserve pins Billable and the resolved owner onto the
// routed message so connector-pool can capture the identical balance key (step-146).
func TestPipelineReserveSetsBillableAndOwner(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	res := &stubReserver{reserved: true, ownerType: cp.OwnerTypeSMPPAccount}
	deps := testDeps(tracer)
	deps.Credit = res
	p := pipeline.New(deps)

	out, _, err := p.Process(context.Background(), inbound("+2250700000000"))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !out.Billable || out.OwnerType != cp.OwnerTypeSMPPAccount {
		t.Errorf("(Billable, OwnerType) = (%v, %q), want (true, smpp_account)", out.Billable, out.OwnerType)
	}
	if res.calls != 1 {
		t.Errorf("credit stage made %d reserve calls, want 1", res.calls)
	}
}

// TestPipelineReserveDisabledLeavesUnbilled: a customer with billing disabled (reserved=false) routes with
// nothing to settle — Billable false, no owner pinned — so connector-pool skips capture (step-146).
func TestPipelineReserveDisabledLeavesUnbilled(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	res := &stubReserver{reserved: false}
	deps := testDeps(tracer)
	deps.Credit = res
	p := pipeline.New(deps)

	out, _, err := p.Process(context.Background(), inbound("+2250700000000"))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Billable || out.OwnerType != "" {
		t.Errorf("(Billable, OwnerType) = (%v, %q), want (false, empty)", out.Billable, out.OwnerType)
	}
}

// TestPipelineCreditInsufficientRejects: a business denial rejects with insufficient_credit AFTER
// segmentation (frozen order), so the caller writes a rejected CDR and never sends; the span carries no body (invariant a).
func TestPipelineCreditInsufficientRejects(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	res := &stubReserver{err: errs.ErrInsufficientCredit}
	deps := testDeps(tracer)
	deps.Credit = res
	p := pipeline.New(deps)

	_, _, err := p.Process(context.Background(), inbound("+2250700000000"))
	if code, _ := errs.CodeOf(err); code != errs.ErrInsufficientCredit {
		t.Fatalf("code: got %q want insufficient_credit", code)
	}
	if !rec.Recorded("pipeline.credit") {
		t.Error("credit span should have been emitted")
	}
	if !rec.Recorded("pipeline.segment") {
		t.Error("credit must run AFTER segmentation (frozen order)")
	}
	rec.AssertNoBody(t, "topsecretbody")
}

// TestPipelineCreditTransientErrorIsNotACode: a billing transport fault surfaces as a non-code error (the
// router retries), never a rejection code — a billed message must not be dropped because billing blinked.
func TestPipelineCreditTransientErrorIsNotACode(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	res := &stubReserver{err: errors.New("billing-svc unavailable")}
	deps := testDeps(tracer)
	deps.Credit = res
	p := pipeline.New(deps)

	_, _, err := p.Process(context.Background(), inbound("+2250700000000"))
	if err == nil {
		t.Fatal("expected an error from the billing transport fault")
	}
	if code, ok := errs.CodeOf(err); ok {
		t.Fatalf("transient billing fault must not carry a rejection code, got %q", code)
	}
}

// TestPipelineSplitsLongMessageIntoSegments: a message past one segment is split into concatenated
// segments, each carrying a well-formed UDH that round-trips through ParseUDH under one shared
// reference, and the segment span carries the count but never the body (invariant a).
func TestPipelineSplitsLongMessageIntoSegments(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(testDeps(tracer))

	in := inbound("+2250700000000")
	in.Body = msg.NewBodyString(strings.Repeat("a", 161)) // 161 GSM-7 chars -> 2 segments (152 + 9)

	out, segs, err := p.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.SegmentCount != 2 || len(segs) != 2 {
		t.Fatalf("segment_count=%d, len(segs)=%d, want 2/2", out.SegmentCount, len(segs))
	}
	for i, s := range segs {
		if s.Seq != i+1 || s.Total != 2 || !s.HasUDH {
			t.Errorf("segment %d = {seq:%d total:%d udh:%v}, want seq %d / total 2 / udh", i+1, s.Seq, s.Total, s.HasUDH, i+1)
		}
		concat, _, hasConcat, perr := smpp.ParseUDH(s.Payload)
		if perr != nil || !hasConcat {
			t.Fatalf("segment %d UDH: parse err=%v hasConcat=%v", i+1, perr, hasConcat)
		}
		if concat.Reference != segs[0].Ref {
			t.Errorf("segment %d reference %d differs from %d", i+1, concat.Reference, segs[0].Ref)
		}
	}
	if !rec.Recorded("pipeline.segment") {
		t.Error("pipeline.segment span should have been emitted")
	}
	rec.AssertNoBody(t, "aaaa")
}

// TestPipelineDataCodingDrivesSegmentationCharset locks Q2 of the design: a client that drives the
// wire DCS (data_coding) fixes the charset the message is segmented in, so segment boundaries match
// the bytes on the wire. Plain ASCII would auto-detect as one GSM-7 segment; data_coding=UCS-2 makes
// it two UCS-2 segments (100 code units > the 70-unit single-segment limit).
func TestPipelineDataCodingDrivesSegmentationCharset(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(testDeps(tracer))

	ucs2 := int(smpp.DataCodingUCS2)
	in := inbound("+2250700000000")
	in.Encoding = "auto"
	in.DataCoding = &ucs2
	in.Body = msg.NewBodyString(strings.Repeat("a", 100))

	out, segs, err := p.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Encoding != "ucs2" {
		t.Errorf("encoding = %q, want ucs2 (data_coding drives the charset)", out.Encoding)
	}
	if len(segs) != 2 {
		t.Fatalf("segments = %d, want 2 (100 UCS-2 units past the 70 single limit)", len(segs))
	}
}

// TestPipelinePreSegmentedUDHIBypass: a client that already segmented its own SMPP submit (esm_class
// UDH indicator set) is passed through as a single record carrying its UDH verbatim — never re-split.
func TestPipelinePreSegmentedUDHIBypass(t *testing.T) {
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	p := pipeline.New(testDeps(tracer))

	raw := strings.Repeat("x", 200) // would be 2 segments if we re-split, but the client already did
	in := inbound("+2250700000000")
	in.ESMClass = smpp.ESMClassUDHIndicator
	in.Body = msg.NewBodyString(raw)

	out, segs, err := p.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.SegmentCount != 1 || len(segs) != 1 {
		t.Fatalf("segment_count=%d len(segs)=%d, want 1/1 (never re-split a pre-segmented submit)", out.SegmentCount, len(segs))
	}
	if !segs[0].HasUDH {
		t.Error("a pre-segmented submit must keep its UDH indicator")
	}
	if string(segs[0].Payload) != raw {
		t.Error("a pre-segmented body must pass through verbatim")
	}
}

// TestSegmentCountIsThePipelinesOwn: admission pays the segment count before the acknowledgement
// (step-283); it must be the count the pipeline then segments into, or a client pays one figure at the
// door and occupies another on the wire.
func TestSegmentCountIsThePipelinesOwn(t *testing.T) {
	ucs2 := 8
	cases := map[string]struct {
		mutate func(*pipeline.InboundMT)
		want   int
	}{
		"short gsm-7":              {func(in *pipeline.InboundMT) { in.Body = msg.NewBodyString("hello") }, 1},
		"long gsm-7":               {func(in *pipeline.InboundMT) { in.Body = msg.NewBodyString(strings.Repeat("a", 200)) }, 2},
		"160 chars that are ucs-2": {func(in *pipeline.InboundMT) { in.Body = msg.NewBodyString(strings.Repeat("ê", 160)) }, 3},
		"ucs-2 forced by data_coding": {func(in *pipeline.InboundMT) {
			in.Body = msg.NewBodyString(strings.Repeat("a", 100))
			in.DataCoding = &ucs2
		}, 2},
		"pre-segmented by the client": {func(in *pipeline.InboundMT) {
			in.Body = msg.NewBodyString(strings.Repeat("a", 150))
			in.ESMClass = smpp.ESMClassUDHIndicator
		}, 1},
	}
	p := pipeline.New(testDeps(observability.Tracer(otelrec.New(t).Provider(), "router")))
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			in := inbound("+2250700000000")
			c.mutate(&in)
			out, _, err := p.Process(context.Background(), in)
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if out.SegmentCount != c.want {
				t.Fatalf("the pipeline segmented into %d, want %d — the case does not test what it says", out.SegmentCount, c.want)
			}
			if got := pipeline.SegmentCount(in); got != out.SegmentCount {
				t.Errorf("admission counts %d segments, the pipeline sends %d", got, out.SegmentCount)
			}
		})
	}
}

type mismatchRules struct{}

func (mismatchRules) ListActive(context.Context) ([]cp.AntispamRule, error) {
	return []cp.AntispamRule{{
		ID: uuid.New(), RuleType: cp.AntispamCategoryMismatch, Scope: cp.AntispamScopeGlobal,
		ConfigJSON: []byte(`{"promo_markers":["jackpot"]}`), Action: cp.AntispamActionFlag, Status: cp.AntispamRuleActive,
	}}, nil
}

// TestPipelineCategoryMismatchLeaksNothingIntoTheSpans: invariant (a) through the real engine — a flagged
// transactional message leaves neither its body nor the marker it matched on any span.
func TestPipelineCategoryMismatchLeaksNothingIntoTheSpans(t *testing.T) {
	rec := otelrec.New(t)
	deps := testDeps(observability.Tracer(rec.Provider(), "router"))
	deps.SenderIDs = stubAuthorizer{category: cp.TrafficTransactional}
	engine, err := antispam.New(context.Background(), mismatchRules{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("antispam.New: %v", err)
	}
	deps.Antispam = engine
	in := inbound("+2250700000000")
	in.Body = msg.NewBodyString("topsecretbody jackpot")

	if _, _, err := pipeline.New(deps).Process(context.Background(), in); err != nil {
		t.Fatalf("a flagged message must still route: %v", err)
	}
	flagged := false
	for _, span := range rec.Ended() {
		for _, kv := range span.Attributes() {
			if kv.Key == "anti_spam.action" && kv.Value.AsString() == "flag" {
				flagged = true
			}
		}
	}
	if !flagged {
		t.Fatal("the message was not flagged — the leak check would be vacuous")
	}
	rec.AssertNoBody(t, "topsecretbody")
	rec.AssertNoBody(t, "jackpot")
}

// stageTimes counts the pipeline stages observed, by name.
type stageTimes map[string]int

func (s stageTimes) Observe(stage string, _ time.Duration) { s[stage]++ }

// TestPipelineTimesEachStage: the router spends ~226 ms a message (run 2 of step-287), and nothing said in
// which stage. Each one is timed, under the name its span carries (step-287e).
func TestPipelineTimesEachStage(t *testing.T) {
	deps := testDeps(observability.Tracer(otelrec.New(t).Provider(), "router"))
	deps.Credit = &stubReserver{}
	times := stageTimes{}
	deps.Stages = times

	if _, _, err := pipeline.New(deps).Process(context.Background(), inbound("+2250700000000")); err != nil {
		t.Fatalf("Process: %v", err)
	}
	for _, stage := range pipeline.StageNames() {
		if times[stage] != 1 {
			t.Errorf("stage %q observed %d times, want 1 (all: %v)", stage, times[stage], times)
		}
	}
	// The router seeds the histogram from StageNames: a stage timed under any other name would never be.
	if len(times) != len(pipeline.StageNames()) {
		t.Errorf("observed stages %v, want exactly StageNames() %v", times, pipeline.StageNames())
	}
}
