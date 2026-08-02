package connectorpool_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// outcomeProducer captures every record the pool publishes and can be made to fail, so a test can drive
// the produce-failure path the CDR write used to occupy.
type outcomeProducer struct {
	mu   sync.Mutex
	recs []kafka.Record
	err  error
}

func (p *outcomeProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.recs = append(p.recs, rec)
	return nil
}

// records returns everything published, on any topic.
func (p *outcomeProducer) records() []kafka.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]kafka.Record(nil), p.recs...)
}

// outcomes decodes the mt.outcome records only — the pool shares one producer with the reroute and
// dead-letter paths, so filtering by topic is what keeps this from counting the wrong thing.
func (p *outcomeProducer) outcomes(t *testing.T) []pipeline.OutcomeMT {
	t.Helper()
	var out []pipeline.OutcomeMT
	for _, rec := range p.records() {
		if rec.Topic != kafka.TopicMTOutcome {
			continue
		}
		o, err := pipeline.DecodeOutcome(rec)
		if err != nil {
			t.Fatalf("decode mt.outcome: %v", err)
		}
		out = append(out, o)
	}
	return out
}

// only fails the test unless exactly one outcome was published, and returns it.
func (p *outcomeProducer) only(t *testing.T) pipeline.OutcomeMT {
	t.Helper()
	got := p.outcomes(t)
	if len(got) != 1 {
		t.Fatalf("mt.outcome records = %d (%+v), want 1", len(got), got)
	}
	return got[0]
}

// poolSink is everything one run records: the CDR rows the pool still writes DIRECTLY to ClickHouse
// (cancel, reroute, dead-letter — D6 leaves those alone) and the outcome events it now publishes for the
// projection. The send path writes the first and publishes the second, so a test that wants to know what
// a submit recorded must look at the outcomes.
type poolSink struct {
	cdr *fakeCDR
	out *outcomeProducer
}

func newPoolSink() *poolSink { return &poolSink{cdr: &fakeCDR{}, out: &outcomeProducer{}} }

// rows are the CDR rows written straight to ClickHouse (never a submit outcome any more).
func (s *poolSink) rows() []clickhouse.CDRRow {
	s.cdr.mu.Lock()
	defer s.cdr.mu.Unlock()
	return append([]clickhouse.CDRRow(nil), s.cdr.rows...)
}

// outcome fails the test unless exactly one outcome was published, and returns it.
func (s *poolSink) outcome(t *testing.T) pipeline.OutcomeMT {
	t.Helper()
	return s.out.only(t)
}

// outcomes are every published outcome, in publish order.
func (s *poolSink) outcomes(t *testing.T) []pipeline.OutcomeMT { return s.out.outcomes(t) }

// runOutcome drives one record through the connector with a recording producer wired, returning the
// producer, the direct-ClickHouse sink (which the send path must now leave untouched) and Run's error.
func runOutcome(t *testing.T, resp func(smpp.SubmitSM) fakesmsc.Resp, r pipeline.RoutedMT,
	prod *outcomeProducer, settler connectorpool.BillingSettler,
) (*fakeCDR, string, error) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: resp})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	cdr := &fakeCDR{}
	logBuf := &syncBuffer{}
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}},
		CDR:      cdr,
		Producer: prod,
		Billing:  settler,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
		Logger: slog.New(slog.NewTextHandler(logBuf, nil)),
	})
	runErr := svc.Run(context.Background())
	return cdr, logBuf.String(), runErr
}

// TestConnectorPublishesEnrouteOutcome is the step: an accepted submit_sm records its outcome by
// PUBLISHING it, and writes nothing to ClickHouse on the send path. The projection rebuilds the same row
// from what travels here, so every field the old row carried is asserted.
func TestConnectorPublishesEnrouteOutcome(t *testing.T) {
	r := routed()
	routeID := r.ConnectorID // any non-nil route id; the projection must carry it through
	r.RouteID = &routeID
	r.SegmentSeq, r.SegmentCount = 2, 3
	prod := &outcomeProducer{}

	cdr, _, err := runOutcome(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, r, prod, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 0 {
		t.Errorf("the send path must not write ClickHouse any more, got %+v", cdr.rows)
	}

	got := prod.only(t)
	if got.Status != "enroute" {
		t.Errorf("status = %q, want enroute", got.Status)
	}
	if got.ErrorCode != nil {
		t.Errorf("error_code = %v, want nil on an accepted submit", *got.ErrorCode)
	}
	if got.MessageID != r.MessageID || got.TraceID != r.TraceID ||
		got.AccountID != r.AccountID || got.CustomerID != r.CustomerID {
		t.Errorf("ids = %+v, want those of %+v", got, r)
	}
	if got.ConnectorID != r.ConnectorID || got.RouteID == nil || *got.RouteID != routeID {
		t.Errorf("routing = (%v, %v), want (%v, %v)", got.ConnectorID, got.RouteID, r.ConnectorID, routeID)
	}
	if got.From != r.From || got.To != r.To || got.Encoding != r.Encoding {
		t.Errorf("addressing = (%q, %q, %q), want (%q, %q, %q)",
			got.From, got.To, got.Encoding, r.From, r.To, r.Encoding)
	}
	if got.SegmentSeq != 2 || got.SegmentCount != 3 {
		t.Errorf("segments = %d/%d, want 2/3", got.SegmentSeq, got.SegmentCount)
	}
	if !got.SubmittedAt.Equal(r.SubmittedAt) {
		t.Errorf("submitted_at = %s, want the immutable accept time %s", got.SubmittedAt, r.SubmittedAt)
	}
}

// TestConnectorOutcomeClampsSegmentCoordinates: a connector outcome is always a DISPATCHED segment, and
// segment_seq 0 is reserved by the CDR sorting key for the pre-dispatch message-level row. A record that
// carries no segment coordinates (a single-segment message the router did not number) must therefore
// leave as 1 of 1 — publishing the raw 0 would make the projection supersede the accepted placeholder
// instead of writing the segment's own row.
func TestConnectorOutcomeClampsSegmentCoordinates(t *testing.T) {
	r := routed()
	r.SegmentSeq, r.SegmentCount = 0, 0
	prod := &outcomeProducer{}

	if _, _, err := runOutcome(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, r, prod, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := prod.only(t)
	if got.SegmentSeq != 1 || got.SegmentCount != 1 {
		t.Errorf("segments = %d/%d, want 1/1", got.SegmentSeq, got.SegmentCount)
	}
}

// TestConnectorPublishesFailedOutcome: a permanent SMSC rejection is a terminal outcome, so it is
// published with the contract error code and the record commits — the same contract the failed CDR row
// carried, minus the synchronous ClickHouse write.
func TestConnectorPublishesFailedOutcome(t *testing.T) {
	prod := &outcomeProducer{}
	cdr, _, err := runOutcome(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SubmitFailed() }, routed(), prod, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 0 {
		t.Errorf("the send path must not write ClickHouse any more, got %+v", cdr.rows)
	}
	got := prod.only(t)
	if got.Status != "failed" {
		t.Errorf("status = %q, want failed", got.Status)
	}
	if got.ErrorCode == nil || *got.ErrorCode != "submit_failed" {
		t.Errorf("error_code = %v, want submit_failed", got.ErrorCode)
	}
}

// TestConnectorPublishesNothingOnTransientRejection: a throttle is backpressure, not an outcome. It was
// never a CDR row and must not become an event either — publishing one would record a message as failed
// while it is still going to be sent.
func TestConnectorPublishesNothingOnTransientRejection(t *testing.T) {
	prod := &outcomeProducer{}
	_, _, err := runOutcome(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() }, routed(), prod, nil)
	if err == nil {
		t.Fatal("a throttled submit must redeliver (non-nil Run error)")
	}
	if got := prod.outcomes(t); len(got) != 0 {
		t.Errorf("mt.outcome records = %+v, want none on deliberate backpressure", got)
	}
}

// TestConnectorRedeliversWhenOutcomePublishFails pins the new fail-closed frontier. The produce is the
// last thing standing between a sent message and a committed offset, exactly where the ClickHouse write
// stood: it must be acked BEFORE the commit, so a failure leaves the offset uncommitted. That is a
// bounded duplicate submit on redelivery (ADR-0012), deliberately preferred to a lost CDR row — the
// billing reaper settles a reservation against the message's recorded outcome, and there is no reaper
// for a row that was never written.
func TestConnectorRedeliversWhenOutcomePublishFails(t *testing.T) {
	prod := &outcomeProducer{err: errors.New("kafka unavailable")}
	_, _, err := runOutcome(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, routed(), prod, nil)
	if err == nil {
		t.Fatal("a failed outcome publish must not commit the offset (want a non-nil Run error)")
	}
	if !strings.Contains(err.Error(), "kafka unavailable") {
		t.Errorf("Run error = %v, want it to carry the produce failure", err)
	}
}

// TestConnectorOutcomeCarriesTheCaptureResult: billed/credits_charged are known only to the connector
// (it holds the reservation), so they travel on the event rather than being recomputed downstream.
func TestConnectorOutcomeCarriesTheCaptureResult(t *testing.T) {
	charged := int32(3)
	spy := &spySettler{billed: true, charged: &charged}
	prod := &outcomeProducer{}
	_, _, err := runOutcome(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, billableRouted(), prod, spy)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spy.captureCalls != 1 {
		t.Fatalf("capture calls = %d, want 1 — the fixture never reached the settle site", spy.captureCalls)
	}
	got := prod.only(t)
	if !got.Billed || got.CreditsCharged == nil || *got.CreditsCharged != 3 {
		t.Errorf("billing = (billed %v, charged %v), want (true, &3)", got.Billed, got.CreditsCharged)
	}
}

// TestConnectorOutcomeNeverCarriesTheBody is invariant (a) at the new egress. mt.routed legitimately
// carries the plaintext (it is the wire payload); mt.outcome is consumed by a writer that stores no
// content, so the body must not ride along — nor reach the log on the way.
func TestConnectorOutcomeNeverCarriesTheBody(t *testing.T) {
	const secret = "confidential outcome body"
	r := routed()
	r.Body = msg.NewBodyString(secret)
	prod := &outcomeProducer{}

	_, logs, err := runOutcome(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, r, prod, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	recs := prod.records()
	if len(recs) != 1 {
		t.Fatalf("published %d records, want 1 — the fixture never reached the publish site", len(recs))
	}
	// Both the raw bytes and the base64 form the wire codec would use: a body smuggled through either
	// encoding fails here.
	needles := [][]byte{[]byte(secret), []byte(base64.StdEncoding.EncodeToString([]byte(secret)))}
	for _, rec := range recs {
		for _, needle := range needles {
			if bytes.Contains(rec.Value, needle) {
				t.Errorf("the message body leaked into the %s value (invariant a)", rec.Topic)
			}
			if bytes.Contains(rec.Key, needle) {
				t.Errorf("the message body leaked into the %s key (invariant a)", rec.Topic)
			}
			for _, h := range rec.Headers {
				if bytes.Contains(h.Value, needle) {
					t.Errorf("the message body leaked into the %s header %q (invariant a)", rec.Topic, h.Key)
				}
			}
		}
	}
	if strings.Contains(logs, secret) {
		t.Errorf("the message body leaked into the log (invariant a):\n%s", logs)
	}
}

// TestConnectorRecordsDLRMappingEvenWhenTheOutcomePublishFails pins the ordering the move settles: the
// SMS is already on the SMSC's wire, so the receipt it will send must be correlatable no matter what our
// own bookkeeping does next. Recording the mapping only after a fail-closed write meant an outage on the
// bookkeeping side ALSO orphaned the receipt of a message that really was delivered.
func TestConnectorRecordsDLRMappingEvenWhenTheOutcomePublishFails(t *testing.T) {
	dlr := &fakeDLRMap{}
	prod := &outcomeProducer{err: errors.New("kafka unavailable")}
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }})
	r := routed()
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}},
		CDR:      &fakeCDR{},
		Producer: prod,
		DLRMap:   dlr,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err == nil {
		t.Fatal("a failed outcome publish must redeliver — the fixture never reached the publish site")
	}
	if len(dlr.puts) != 1 {
		t.Fatalf("DLR mappings = %d, want 1: the submit succeeded, so its receipt must be correlatable", len(dlr.puts))
	}
	if dlr.puts[0].routed.MessageID != r.MessageID {
		t.Errorf("mapping message id = %s, want %s", dlr.puts[0].routed.MessageID, r.MessageID)
	}
}

// TestConnectorStillWritesCancelledRowDirectly is D6's boundary, made falsifiable: only the post-submit
// site moves to the projection. The cancel site precedes the irreversible effect, so its failure cannot
// duplicate an SMS — it keeps writing ClickHouse, and publishes no outcome.
func TestConnectorStillWritesCancelledRowDirectly(t *testing.T) {
	prod := &outcomeProducer{}
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }})
	rec, err := pipeline.EncodeRouted(routed())
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	cdr := &fakeCDR{}
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:    &fakeConsumer{records: []kafka.Record{rec}},
		CDR:         cdr,
		Producer:    prod,
		CancelFlags: &fakeFlags{cancelled: true},
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != "cancelled" {
		t.Errorf("cancelled row = %+v, want one written straight to ClickHouse (D6)", cdr.rows)
	}
	if got := prod.outcomes(t); len(got) != 0 {
		t.Errorf("mt.outcome records = %+v, want none: a cancelled message was never submitted", got)
	}
}
