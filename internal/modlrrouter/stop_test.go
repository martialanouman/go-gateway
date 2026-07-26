package modlrrouter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

type fakeOptOutLister struct{ rows []cp.OptOutKeyword }

func (f fakeOptOutLister) ListActive(context.Context) ([]cp.OptOutKeyword, error) { return f.rows, nil }

// fakeSuppress records writes and dedups on the natural key, so a repeated STOP reports "not created"
// exactly as the real ON CONFLICT DO NOTHING does.
type fakeSuppress struct {
	created []cp.NewSuppression
	deleted []string // msisdns
	keys    map[string]bool
	err     error
}

func (f *fakeSuppress) Create(_ context.Context, in cp.NewSuppression) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	if f.keys == nil {
		f.keys = map[string]bool{}
	}
	k := string(in.Scope) + "|" + derefStr(in.ScopeID) + "|" + in.MSISDN
	f.created = append(f.created, in)
	if f.keys[k] {
		return false, nil
	}
	f.keys[k] = true
	return true, nil
}

func (f *fakeSuppress) DeleteByKey(_ context.Context, _ cp.SuppressionScope, _ *uuid.UUID, msisdn string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	f.deleted = append(f.deleted, msisdn)
	return true, nil
}

func derefStr(u *uuid.UUID) string {
	if u == nil {
		return ""
	}
	return u.String()
}

func strptr(s string) *string { return &s }

func detectorWith(t *testing.T, kws []cp.OptOutKeyword, sup *fakeSuppress, prod *fakeProducer) *modlrrouter.StopDetector {
	t.Helper()
	kw, err := modlrrouter.LoadOptOutKeywords(context.Background(), fakeOptOutLister{kws})
	if err != nil {
		t.Fatalf("LoadOptOutKeywords: %v", err)
	}
	return modlrrouter.NewStopDetector(modlrrouter.StopDeps{
		Keywords: kw, Suppress: sup, Producer: prod, Tracer: observability.Tracer(nil, "test"),
	})
}

func stopInput(inbNum, from string, inbID uuid.UUID, body string) modlrrouter.StopInput {
	return modlrrouter.StopInput{
		MO:              pipeline.MOInbound{ConnectorID: uuid.New(), From: from, To: inbNum, Body: msg.NewBodyString(body), ReceivedAt: time.Unix(1000, 0).UTC()},
		InboundNumber:   inbNum,
		From:            from,
		Body:            []byte(body),
		InboundNumberID: inbID,
		Country:         "CI",
		AccountID:       uuid.New(),
		CustomerID:      uuid.New(),
	}
}

func TestOptOutKeywordMatchPrecedence(t *testing.T) {
	kws := []cp.OptOutKeyword{
		{Keyword: "STOP", Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchExact, Status: cp.OptOutKeywordActive},
		{Keyword: "STOP", CountryCode: strptr("CI"), Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchPrefix, Status: cp.OptOutKeywordActive},
		{Keyword: "START", Action: cp.OptOutActionUnsuppress, MatchType: cp.OptOutMatchExact, Status: cp.OptOutKeywordActive},
	}
	k, err := modlrrouter.LoadOptOutKeywords(context.Background(), fakeOptOutLister{kws})
	if err != nil {
		t.Fatalf("LoadOptOutKeywords: %v", err)
	}

	// Case-insensitive, trimmed.
	if _, ok := k.Match("CI", "  stop  "); !ok {
		t.Error("STOP should match case-insensitively and trimmed")
	}
	// START matches unsuppress.
	m, ok := k.Match("CI", "START")
	if !ok || m.Action != cp.OptOutActionUnsuppress {
		t.Errorf("START match = %v/%s, want unsuppress", ok, m.Action)
	}
	// A non-keyword does not match.
	if _, ok := k.Match("CI", "hello world"); ok {
		t.Error("a non-keyword body must not match")
	}
	// Country mismatch falls back to the global rule (STOP exact still matches).
	if _, ok := k.Match("FR", "STOP"); !ok {
		t.Error("global STOP should match in any country")
	}
}

func TestStopSuppressesAndAutoReplies(t *testing.T) {
	inbID := uuid.New()
	sup := &fakeSuppress{}
	prod := &fakeProducer{}
	d := detectorWith(t, []cp.OptOutKeyword{
		{Keyword: "STOP", Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchExact,
			AutoReplyTemplate: strptr("You are unsubscribed"), Status: cp.OptOutKeywordActive},
	}, sup, prod)

	in := stopInput("36000", "2250700000001", inbID, "STOP")
	if err := d.Detect(context.Background(), in); err != nil {
		t.Fatalf("Detect: %v", err)
	}

	// A suppression scoped to the inbound number, keyed by the sender, sourced mo_stop.
	if len(sup.created) != 1 {
		t.Fatalf("expected 1 suppression, got %d", len(sup.created))
	}
	s := sup.created[0]
	if s.Scope != cp.SuppressionScopeInboundNumber || s.ScopeID == nil || *s.ScopeID != inbID ||
		s.MSISDN != "2250700000001" || s.Source != cp.SuppressionSourceMOStop {
		t.Errorf("suppression = %+v, want inbound_number/%s/2250700000001/mo_stop", s, inbID)
	}

	// An auto-reply MT to mt.routed, never billed, from the inbound number to the sender, body = template.
	if len(prod.recs) != 1 {
		t.Fatalf("expected 1 auto-reply, got %d", len(prod.recs))
	}
	if prod.recs[0].Topic != kafka.TopicMTRouted {
		t.Errorf("auto-reply topic = %q, want mt.routed", prod.recs[0].Topic)
	}
	reply, err := pipeline.DecodeRouted(prod.recs[0])
	if err != nil {
		t.Fatalf("decode auto-reply: %v", err)
	}
	if reply.Billable {
		t.Error("auto-reply must be billable=false")
	}
	if reply.From != "36000" || reply.To != "2250700000001" {
		t.Errorf("auto-reply addrs = %s -> %s, want 36000 -> 2250700000001", reply.From, reply.To)
	}
	if string(reply.Body.Reveal()) != "You are unsubscribed" {
		t.Errorf("auto-reply body = %q, want the template", reply.Body.Reveal())
	}
}

func TestStopIsIdempotentAndDeterministic(t *testing.T) {
	inbID := uuid.New()
	sup := &fakeSuppress{}
	prod := &fakeProducer{}
	d := detectorWith(t, []cp.OptOutKeyword{
		{Keyword: "STOP", Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchExact,
			AutoReplyTemplate: strptr("bye"), Status: cp.OptOutKeywordActive},
	}, sup, prod)

	in := stopInput("36000", "2250700000001", inbID, "STOP")
	if err := d.Detect(context.Background(), in); err != nil {
		t.Fatalf("Detect #1: %v", err)
	}
	if err := d.Detect(context.Background(), in); err != nil {
		t.Fatalf("Detect #2: %v", err)
	}

	// The same MO produces the SAME auto-reply message id both times (redelivery must not send a new
	// confirmation downstream — dedup collapses them by id).
	r1, _ := pipeline.DecodeRouted(prod.recs[0])
	r2, _ := pipeline.DecodeRouted(prod.recs[1])
	if r1.MessageID != r2.MessageID {
		t.Errorf("auto-reply ids differ across redelivery: %s vs %s", r1.MessageID, r2.MessageID)
	}
	// The second suppression write did not create a duplicate (idempotence).
	if len(sup.keys) != 1 {
		t.Errorf("expected a single distinct suppression key, got %d", len(sup.keys))
	}
}

func TestStartRemovesSuppression(t *testing.T) {
	sup := &fakeSuppress{}
	d := detectorWith(t, []cp.OptOutKeyword{
		{Keyword: "START", Action: cp.OptOutActionUnsuppress, MatchType: cp.OptOutMatchExact, Status: cp.OptOutKeywordActive},
	}, sup, &fakeProducer{})

	if err := d.Detect(context.Background(), stopInput("36000", "2250700000001", uuid.New(), "START")); err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if len(sup.deleted) != 1 || sup.deleted[0] != "2250700000001" {
		t.Errorf("expected a delete for 2250700000001, got %v", sup.deleted)
	}
	if len(sup.created) != 0 {
		t.Error("START must not create a suppression")
	}
}

func TestNoKeywordDoesNothing(t *testing.T) {
	sup := &fakeSuppress{}
	prod := &fakeProducer{}
	d := detectorWith(t, []cp.OptOutKeyword{
		{Keyword: "STOP", Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchExact, Status: cp.OptOutKeywordActive},
	}, sup, prod)

	if err := d.Detect(context.Background(), stopInput("36000", "2250700000001", uuid.New(), "hello there")); err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if len(sup.created) != 0 || len(prod.recs) != 0 {
		t.Errorf("a non-keyword MO must write nothing and emit no reply: created=%d replies=%d", len(sup.created), len(prod.recs))
	}
}

// TestMOStillDeliveredOnStop is the load-bearing criterion: a STOP MO writes a suppression and emits
// an auto-reply, but the MO is STILL routed to the account (delivery is never interrupted, §6.20).
func TestMOStillDeliveredOnStop(t *testing.T) {
	account, customer := uuid.New(), uuid.New()
	numID, connector := uuid.New(), uuid.New()
	snap := moSnapshot(t,
		[]cp.InboundNumber{{ID: numID, Address: "36000", CountryCode: "CI", Status: cp.InboundNumberActive, AccountID: &account}},
		nil, map[uuid.UUID]uuid.UUID{account: customer})

	sup := &fakeSuppress{}
	prod := &fakeProducer{}
	detector := detectorWith(t, []cp.OptOutKeyword{
		{Keyword: "STOP", Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchExact,
			AutoReplyTemplate: strptr("bye"), Status: cp.OptOutKeywordActive},
	}, sup, prod)

	_, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: &fakeUnrouted{}, Stop: detector},
		moRecord(t, connector, "2250700000001", "36000", "STOP"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The suppression was written.
	if len(sup.created) != 1 {
		t.Fatalf("expected the STOP to write a suppression, got %d", len(sup.created))
	}
	// Both an auto-reply (mt.routed) AND the delivered MO (mo.routed) were produced.
	var moRouted, autoReply int
	for _, r := range prod.recs {
		switch r.Topic {
		case kafka.TopicMORouted:
			moRouted++
		case kafka.TopicMTRouted:
			autoReply++
		}
	}
	if moRouted != 1 {
		t.Errorf("the MO must still be delivered on STOP: mo.routed count = %d, want 1", moRouted)
	}
	if autoReply != 1 {
		t.Errorf("auto-reply count = %d, want 1", autoReply)
	}
}

// TestStopFromNonCanonicalSourceSkipsButDelivers is the poison-message guard: a STOP whose source is
// not a canonical MSISDN (a national-format or alphanumeric source_addr the SMSC delivered) cannot key
// a suppression, so the effect is skipped WITHOUT an error — the MO is still delivered, never wedged.
func TestStopFromNonCanonicalSourceSkipsButDelivers(t *testing.T) {
	sup := &fakeSuppress{}
	prod := &fakeProducer{}
	d := detectorWith(t, []cp.OptOutKeyword{
		{Keyword: "STOP", Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchExact,
			AutoReplyTemplate: strptr("bye"), Status: cp.OptOutKeywordActive},
	}, sup, prod)

	// "0700000001" (national format, leading zero) does not normalize to E.164.
	err := d.Detect(context.Background(), stopInput("36000", "0700000001", uuid.New(), "STOP"))
	if err != nil {
		t.Fatalf("a non-canonical source must be skipped, not error: %v", err)
	}
	if len(sup.created) != 0 {
		t.Error("no suppression must be written for a non-canonical source")
	}
	if len(prod.recs) != 0 {
		t.Error("no auto-reply must be emitted to a non-canonical source")
	}
}

// fakeVelocity records the sources it was asked to count, and can fail to prove best-effort.
type fakeVelocity struct {
	sources []string
	err     error
}

func (f *fakeVelocity) RecordSource(_ context.Context, from string) error {
	f.sources = append(f.sources, from)
	return f.err
}

// TestMORecordsSourceVelocity: a routed MO counts its (normalized) source into the velocity, and a
// recording failure is best-effort — the MO is still delivered.
func TestMORecordsSourceVelocity(t *testing.T) {
	account, customer := uuid.New(), uuid.New()
	numID, connector := uuid.New(), uuid.New()
	snap := moSnapshot(t,
		[]cp.InboundNumber{{ID: numID, Address: "36000", CountryCode: "CI", Status: cp.InboundNumberActive, AccountID: &account}},
		nil, map[uuid.UUID]uuid.UUID{account: customer})

	vel := &fakeVelocity{err: errors.New("redis down")} // failure must not interrupt delivery
	prod := &fakeProducer{}
	_, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: &fakeUnrouted{}, Velocity: vel},
		moRecord(t, connector, "2250700000001", "36000", "hello"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(vel.sources) != 1 || vel.sources[0] != "2250700000001" {
		t.Errorf("recorded sources = %v, want [2250700000001]", vel.sources)
	}
	// The MO is still delivered despite the velocity recording failing.
	moRouted := 0
	for _, r := range prod.recs {
		if r.Topic == kafka.TopicMORouted {
			moRouted++
		}
	}
	if moRouted != 1 {
		t.Errorf("mo.routed count = %d, want 1 (velocity failure is best-effort)", moRouted)
	}
}

func TestStopWriteErrorIsRetryable(t *testing.T) {
	d := detectorWith(t, []cp.OptOutKeyword{
		{Keyword: "STOP", Action: cp.OptOutActionSuppress, MatchType: cp.OptOutMatchExact, Status: cp.OptOutKeywordActive},
	}, &fakeSuppress{err: errors.New("db down")}, &fakeProducer{})

	if err := d.Detect(context.Background(), stopInput("36000", "2250700000001", uuid.New(), "STOP")); err == nil {
		t.Fatal("a suppression-write failure must return an error so the MO is reprocessed")
	}
}
