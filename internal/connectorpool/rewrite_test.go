package connectorpool_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// fakeRewriter answers a fixed address and records what it was asked.
type fakeRewriter struct {
	mu   sync.Mutex
	to   string
	args []any
}

func (f *fakeRewriter) Rewrite(connectorID, accountID, customerID uuid.UUID, from, to string, messageID uuid.UUID) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.args = []any{connectorID, accountID, customerID, from, to, messageID}
	if f.to == "" {
		return from
	}
	return f.to
}

type rewriteRun struct {
	wire     smpp.SubmitSM
	out      *outcomeProducer
	dlr      *fakeDLRMap
	rewriter *fakeRewriter
}

func runRewrite(t *testing.T, to string, r pipeline.RoutedMT, resp fakesmsc.Resp) rewriteRun {
	t.Helper()
	run := rewriteRun{out: &outcomeProducer{}, dlr: &fakeDLRMap{}, rewriter: &fakeRewriter{to: to}}
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(sm smpp.SubmitSM) fakesmsc.Resp {
		run.wire = sm
		return resp
	}})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}},
		CDR:      &fakeCDR{},
		Producer: run.out,
		DLRMap:   run.dlr,
		// ConnectorID left unset, as on a pool that filters nothing: the rules must still key on the
		// message's connector.
		Rewriter: run.rewriter,
		Bind:     poolBind(smsc.Addr(), 1),
		Tracer:   observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return run
}

// The rewritten sender is what the SMSC receives and what the CDR records as sent; the client's own
// travels beside it — on mt.outcome for the CDR, in the DLR mapping for the receipt it gets back.
func TestRewrittenSenderGoesOnTheWireAndTheOriginalIsKept(t *testing.T) {
	r := routed()
	r.From = "+22507000001"
	run := runRewrite(t, "INFO", r, fakesmsc.OK())

	if run.wire.SourceAddr != "INFO" || run.wire.SourceAddrTON != smpp.TONAlphanumeric {
		t.Errorf("wire source = %q TON %#x, want INFO alphanumeric", run.wire.SourceAddr, run.wire.SourceAddrTON)
	}
	out := run.out.only(t)
	if out.From != "INFO" || out.OriginalFrom != "+22507000001" {
		t.Errorf("outcome from = %q, original = %q; want INFO and +22507000001", out.From, out.OriginalFrom)
	}
	if len(run.dlr.puts) != 1 || run.dlr.puts[0].routed.From != "INFO" || run.dlr.puts[0].originalFrom != "+22507000001" {
		t.Errorf("dlr puts = %+v, want the sent INFO and the original +22507000001", run.dlr.puts)
	}
	want := []any{r.ConnectorID, r.AccountID, r.CustomerID, r.From, r.To, r.MessageID}
	if len(run.rewriter.args) != len(want) {
		t.Fatalf("rewriter called with %v, want %v", run.rewriter.args, want)
	}
	for i, a := range run.rewriter.args {
		if a != want[i] {
			t.Errorf("rewriter argument %d = %v, want %v", i, a, want[i])
		}
	}
}

// A rewrite to a numeric sender is typed by the same rule as a client's numeric sender.
func TestRewrittenNumericSenderIsTypedInternational(t *testing.T) {
	run := runRewrite(t, "+2250700999", routed(), fakesmsc.OK())
	if run.wire.SourceAddr != "2250700999" || run.wire.SourceAddrTON != smpp.TONInternational {
		t.Errorf("wire source = %q TON %#x, want 2250700999 international", run.wire.SourceAddr, run.wire.SourceAddrTON)
	}
}

// No rule changed the address: nothing claims a rewrite, so the CDR keeps original_source_addr NULL.
func TestUnchangedSenderCarriesNoOriginal(t *testing.T) {
	r := routed()
	run := runRewrite(t, "", r, fakesmsc.OK())
	if out := run.out.only(t); out.From != r.From || out.OriginalFrom != "" {
		t.Errorf("outcome from = %q, original = %q; want %q and nothing", out.From, out.OriginalFrom, r.From)
	}
	if run.dlr.puts[0].originalFrom != "" {
		t.Errorf("dlr original = %q, want nothing", run.dlr.puts[0].originalFrom)
	}
}

// A rewritten submit the SMSC refuses reroutes the ORIGINAL message: the next connector applies its own
// rules to what the client sent, never to this connector's rewrite.
func TestRerouteAfterARewrittenSubmitCarriesTheOriginal(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	r := routed()
	r.ConnectorID, r.FallbackChain = a, []uuid.UUID{a, b}
	run := runRewrite(t, "INFO", r, fakesmsc.SysErr())
	if run.wire.SourceAddr != "INFO" {
		t.Fatalf("wire source = %q, want the rewrite INFO", run.wire.SourceAddr)
	}
	var rerouted []pipeline.RoutedMT
	for _, rec := range run.out.records() {
		if rec.Topic == kafka.TopicMTRouted {
			got, err := pipeline.DecodeRouted(rec)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			rerouted = append(rerouted, got)
		}
	}
	if len(rerouted) != 1 || rerouted[0].From != r.From || rerouted[0].ConnectorID != b {
		t.Fatalf("rerouted = %+v, want one record to %s carrying %q", rerouted, b, r.From)
	}
}

// A permanent SMSC refusal is a failed CDR row for the address sent; the client's own must be on it too.
func TestRefusedRewrittenSubmitKeepsTheOriginal(t *testing.T) {
	r := routed()
	run := runRewrite(t, "INFO", r, fakesmsc.SubmitFailed())
	if out := run.out.only(t); out.Status != "failed" || out.From != "INFO" || out.OriginalFrom != r.From {
		t.Errorf("outcome = %s from %q original %q, want failed from INFO original %q", out.Status, out.From, out.OriginalFrom, r.From)
	}
}

// fakePins is an in-memory SenderPins; getErr drives the Redis-down path.
type fakePins struct {
	mu     sync.Mutex
	pins   map[uuid.UUID]string
	gets   int
	getErr error
}

func (f *fakePins) Get(_ context.Context, messageID uuid.UUID) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil {
		return "", false, f.getErr
	}
	s, ok := f.pins[messageID]
	return s, ok, nil
}

func (f *fakePins) Pin(_ context.Context, messageID uuid.UUID, sender string, _ *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.pins[messageID]; !ok {
		f.pins[messageID] = sender
	}
	return nil
}

func runPinned(t *testing.T, to string, pins *fakePins, r pipeline.RoutedMT, resp fakesmsc.Resp) (smpp.SubmitSM, *fakeRewriter) {
	t.Helper()
	var wire smpp.SubmitSM
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(sm smpp.SubmitSM) fakesmsc.Resp {
		wire = sm
		return resp
	}})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	rw := &fakeRewriter{to: to}
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:   &fakeConsumer{records: []kafka.Record{rec}},
		CDR:        &fakeCDR{},
		Producer:   &outcomeProducer{},
		Rewriter:   rw,
		SenderPins: pins,
		Bind:       poolBind(smsc.Addr(), 1),
		Tracer:     observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return wire, rw
}

func multipart(seq int) pipeline.RoutedMT {
	r := routed()
	r.SegmentSeq, r.SegmentCount = seq, 2
	return r
}

// A segment whose message already has a sender on the wire takes it, whatever this connector's rules
// say: a handset reassembles a concatenated SMS only under one originator.
func TestLaterSegmentTakesTheSenderAlreadyOnTheWire(t *testing.T) {
	r := multipart(2)
	pins := &fakePins{pins: map[uuid.UUID]string{r.MessageID: "PINNED"}}
	wire, rw := runPinned(t, "LOCAL", pins, r, fakesmsc.OK())
	if wire.SourceAddr != "PINNED" {
		t.Errorf("wire source = %q, want the pinned PINNED", wire.SourceAddr)
	}
	if rw.args != nil {
		t.Error("the rules were evaluated for a message whose sender is already pinned")
	}
}

// The first segment accepted by the SMSC pins the sender it went out under — rewritten or not.
func TestAcceptedSegmentPinsItsSender(t *testing.T) {
	for _, to := range []string{"LOCAL", ""} {
		r := multipart(1)
		pins := &fakePins{pins: map[uuid.UUID]string{}}
		wire, _ := runPinned(t, to, pins, r, fakesmsc.OK())
		if got := pins.pins[r.MessageID]; got != wire.SourceAddr || got == "" {
			t.Errorf("rewrite %q: pinned %q, want the sender on the wire %q", to, got, wire.SourceAddr)
		}
	}
}

// A refused segment pins nothing: the message may reroute whole, and the next connector's rules apply.
func TestRefusedSegmentPinsNothing(t *testing.T) {
	r := multipart(1)
	pins := &fakePins{pins: map[uuid.UUID]string{}}
	runPinned(t, "LOCAL", pins, r, fakesmsc.SubmitFailed())
	if len(pins.pins) != 0 {
		t.Errorf("a refused segment pinned %v", pins.pins)
	}
}

// A single-segment message has nothing to reassemble: no Redis round trip on the bulk of the traffic.
func TestSingleSegmentMessageSkipsThePins(t *testing.T) {
	pins := &fakePins{pins: map[uuid.UUID]string{}}
	runPinned(t, "LOCAL", pins, routed(), fakesmsc.OK())
	if pins.gets != 0 || len(pins.pins) != 0 {
		t.Errorf("single segment: %d reads, %d pins; want none", pins.gets, len(pins.pins))
	}
}

// Redis down: the segment still leaves, under this connector's rules — never blocked, never doubled.
func TestUnreadablePinFallsBackToTheRules(t *testing.T) {
	pins := &fakePins{pins: map[uuid.UUID]string{}, getErr: errors.New("redis down")}
	wire, _ := runPinned(t, "LOCAL", pins, multipart(2), fakesmsc.OK())
	if wire.SourceAddr != "LOCAL" {
		t.Errorf("wire source = %q, want the rules' LOCAL", wire.SourceAddr)
	}
}
