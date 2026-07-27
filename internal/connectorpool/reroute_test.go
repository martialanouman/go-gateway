package connectorpool_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// chainRouted builds a routed record for connector `target` carrying the given fallback chain.
func chainRouted(t *testing.T, target uuid.UUID, chain []uuid.UUID) kafka.Record {
	t.Helper()
	r := routed()
	r.ConnectorID = target
	r.FallbackChain = chain
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return rec
}

// fakeBreakerState reports a fixed open-set for reroute candidate skipping.
type fakeBreakerState struct{ open map[uuid.UUID]bool }

func (f fakeBreakerState) IsOpen(_ context.Context, id uuid.UUID) (bool, error) {
	return f.open[id], nil
}

// rerouteService wires a pool for connector `self` with a recording producer and the given breaker-state.
func rerouteService(t *testing.T, self uuid.UUID, rec kafka.Record, resp func(smpp.SubmitSM) fakesmsc.Resp, bs connectorpool.BreakerState) (*recordingProducer, *fakeCDR) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: resp})
	prod := &recordingProducer{got: make(chan struct{}, 4)}
	cdr := &fakeCDR{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:     &fakeConsumer{records: []kafka.Record{rec}},
		CDR:          cdr,
		Producer:     prod,
		BreakerState: bs,
		ConnectorID:  self,
		Bind:         poolBind(smsc.Addr(), 1),
		Tracer:       observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return prod, cdr
}

// TestRerouteToNextConnector: a connector-health rejection with a fallback chain republishes the message
// to the next connector (advanced chain) and records a rerouted CDR row — no failed row, original committed.
func TestRerouteToNextConnector(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	rec := chainRouted(t, a, []uuid.UUID{a, b})
	prod, cdr := rerouteService(t, a, rec, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SysErr() }, nil)

	recs := prod.records()
	if len(recs) != 1 {
		t.Fatalf("produced %d records, want 1 reroute", len(recs))
	}
	got, err := pipeline.DecodeRouted(recs[0])
	if err != nil {
		t.Fatalf("decode reroute: %v", err)
	}
	if got.ConnectorID != b {
		t.Errorf("rerouted to %s, want next connector %s", got.ConnectorID, b)
	}
	if len(got.FallbackChain) != 0 {
		t.Errorf("advanced chain = %v, want empty (b was the last)", got.FallbackChain)
	}
	if recs[0].Topic != kafka.TopicMTRouted {
		t.Errorf("reroute topic = %s, want mt.routed", recs[0].Topic)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusRerouted {
		t.Fatalf("cdr rows = %+v, want one rerouted", cdr.rows)
	}
	if cdr.rows[0].ConnectorID == nil || *cdr.rows[0].ConnectorID != a {
		t.Errorf("rerouted row connector = %v, want the faulty connector %s", cdr.rows[0].ConnectorID, a)
	}
}

// TestRerouteExhaustedDeadLetters: when the chain has no next connector, the message is parked on
// mt.dead-letter with a failed CDR row (fallback_exhausted).
func TestRerouteExhaustedDeadLetters(t *testing.T) {
	a := uuid.New()
	rec := chainRouted(t, a, []uuid.UUID{a}) // only itself → no fallback
	prod, cdr := rerouteService(t, a, rec, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SysErr() }, nil)

	recs := prod.records()
	if len(recs) != 1 || recs[0].Topic != kafka.TopicMTDeadLetter {
		t.Fatalf("produced %+v, want one mt.dead-letter record", recs)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusFailed {
		t.Fatalf("cdr rows = %+v, want one failed", cdr.rows)
	}
	if cdr.rows[0].ErrorCode == nil || *cdr.rows[0].ErrorCode != "fallback_exhausted" {
		t.Errorf("failed error_code = %v, want fallback_exhausted", cdr.rows[0].ErrorCode)
	}
}

// TestRerouteSkipsOpenCandidate: a candidate whose breaker is open is skipped in one hop.
func TestRerouteSkipsOpenCandidate(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	rec := chainRouted(t, a, []uuid.UUID{a, b, c})
	bs := fakeBreakerState{open: map[uuid.UUID]bool{b: true}} // b is open → skip to c
	prod, _ := rerouteService(t, a, rec, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SysErr() }, bs)

	got, err := pipeline.DecodeRouted(prod.records()[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ConnectorID != c {
		t.Errorf("rerouted to %s, want %s (b was open, skipped)", got.ConnectorID, c)
	}
}

// TestSkipsForeignConnector: a record for another connector is skipped-and-committed — not submitted,
// no CDR, no reroute (option B addressing).
func TestSkipsForeignConnector(t *testing.T) {
	mine, other := uuid.New(), uuid.New()
	rec := chainRouted(t, other, nil) // addressed to another connector
	var submitted bool
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp {
		submitted = true
		return fakesmsc.OK()
	}})
	prod := &recordingProducer{got: make(chan struct{}, 1)}
	cdr := &fakeCDR{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:    &fakeConsumer{records: []kafka.Record{rec}},
		CDR:         cdr,
		Producer:    prod,
		ConnectorID: mine,
		Bind:        poolBind(smsc.Addr(), 1),
		Tracer:      observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if submitted {
		t.Error("submitted a foreign connector's message")
	}
	if len(cdr.rows) != 0 || len(prod.records()) != 0 {
		t.Errorf("foreign record left side effects: cdr=%+v prod=%+v", cdr.rows, prod.records())
	}
}

// TestRerouteOnBreakerOpenSkipsSubmit: once this connector's local breaker has opened, a further chained
// message is rerouted WITHOUT being submitted (the reroute-before-submit path).
func TestRerouteOnBreakerOpenSkipsSubmit(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	var submits atomic.Int32
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp {
		submits.Add(1)
		return fakesmsc.SysErr() // connector-health failure → feeds the breaker
	}})
	recs := make([]kafka.Record, 3)
	for i := range recs {
		recs[i] = chainRouted(t, a, []uuid.UUID{a, b})
	}
	prod := &recordingProducer{got: make(chan struct{}, 8)}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:      &feedThenBlock{records: recs},
		CDR:           &fakeCDR{},
		Producer:      prod,
		ConnectorID:   a,
		Bind:          poolBind(smsc.Addr(), 1),
		Breaker:       &fakeAgg{},
		BreakerConfig: breaker.Config{MinRequests: 2, FailureRate: 0.5},
		Tracer:        observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	// All three are rerouted (to b); the breaker opens after 2 health failures, so the third never submits.
	if !waitFor(3*time.Second, func() bool { return len(prod.records()) >= 3 }) {
		cancel()
		<-done
		t.Fatalf("expected 3 reroutes, got %d (submits=%d)", len(prod.records()), submits.Load())
	}
	cancel()
	<-done
	if got := submits.Load(); got != 2 {
		t.Errorf("submits = %d, want 2 (the third message was rerouted on the open breaker, not submitted)", got)
	}
}
