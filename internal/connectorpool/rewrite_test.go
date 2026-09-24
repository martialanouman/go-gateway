package connectorpool_test

import (
	"context"
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
