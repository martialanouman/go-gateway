package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

const e2eMetric = "message_e2e_duration_seconds"

// e2eSample is one message_e2e_duration_seconds series read off a registry.
type e2eSample struct {
	connectorID string
	status      string
	count       uint64
	sum         float64
}

// gatherE2E reads every message_e2e_duration_seconds series from reg. Gathering (rather than reaching
// into the HistogramVec) is deliberate: WithLabelValues would CREATE the child it looks up, so a test
// asserting "nothing was observed" would silently manufacture the series it is checking for.
func gatherE2E(t *testing.T, reg prometheus.Gatherer) []e2eSample {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var out []e2eSample
	for _, fam := range families {
		if fam.GetName() != e2eMetric {
			continue
		}
		for _, m := range fam.GetMetric() {
			s := e2eSample{count: m.GetHistogram().GetSampleCount(), sum: m.GetHistogram().GetSampleSum()}
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "connector_id":
					s.connectorID = l.GetValue()
				case "status":
					s.status = l.GetValue()
				}
			}
			out = append(out, s)
		}
	}
	return out
}

// oneE2E fails unless exactly one series was observed, and returns it.
func oneE2E(t *testing.T, reg prometheus.Gatherer) e2eSample {
	t.Helper()
	got := gatherE2E(t, reg)
	if len(got) != 1 {
		t.Fatalf("%s series = %d (%+v), want 1", e2eMetric, len(got), got)
	}
	return got[0]
}

// meteredConnector is the connector id both the pool and its records carry in these tests, so the
// label read back is the one under test and not the nil UUID a default would give.
var meteredConnector = uuid.MustParse("11111111-1111-1111-1111-111111111111")

// runMetered drives records through the connector with the Prometheus catalogue wired, and returns the
// guarded registry it is registered on. Registering through metrics.Guard is part of the assertion: an
// emission the cardinality guard would drop at gather time must not read as a success here either.
func runMetered(t *testing.T, resp func(smpp.SubmitSM) fakesmsc.Resp, maxAge time.Duration, rs ...pipeline.RoutedMT) (prometheus.Gatherer, error) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: resp})

	recs := make([]kafka.Record, 0, len(rs))
	for _, r := range rs {
		rec, err := pipeline.EncodeRouted(r)
		if err != nil {
			t.Fatalf("encode routed: %v", err)
		}
		recs = append(recs, rec)
	}

	reg := metrics.Guard(prometheus.NewRegistry())
	cat := metrics.NewCatalog()
	reg.MustRegister(cat.Collectors()...)

	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: recs},
		CDR:      &fakeCDR{},
		Metrics:  cat,
		// The outcome producer is part of the send path since step-201c: without one the pool refuses
		// to drop the outcome of a message it really sent, so a latency test that submits must wire it.
		Producer:      newRecordingProducer(),
		ConnectorID:   meteredConnector,
		MaxMessageAge: maxAge,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
	})
	return reg, svc.Run(context.Background())
}

// meteredRouted is routed() pinned to the metered connector, with an accept time aged by age.
func meteredRouted(age time.Duration) pipeline.RoutedMT {
	r := routed()
	r.ConnectorID = meteredConnector
	r.SubmittedAt = time.Now().UTC().Add(-age)
	return r
}

// TestE2ELatencyObservedOnAcceptedSubmit is the whole point of the wiring: the histogram declared in the
// catalogue must actually carry a sample after a send, and that sample must be measured from the
// message's immutable accept time — not from the instant the connector happened to pick the record up,
// which is what a "time.Since(time.Now())" mistake would give and what makes a dead p99 look healthy.
func TestE2ELatencyObservedOnAcceptedSubmit(t *testing.T) {
	reg, err := runMetered(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, 0,
		meteredRouted(1500*time.Millisecond))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := oneE2E(t, reg)
	if got.connectorID != meteredConnector.String() {
		t.Errorf("connector_id = %v, want %v", got.connectorID, meteredConnector)
	}
	if got.status != "ok" {
		t.Errorf("status = %v, want %v", got.status, "ok")
	}
	if got.count != 1 {
		t.Errorf("sample count = %v, want %v", got.count, 1)
	}
	// One sample, so the sum IS the observation. It must sit above the age planted on SubmittedAt and
	// stay within a slack a fake in-process SMSC cannot plausibly exceed.
	if got.sum < 1.4 || got.sum > 30 {
		t.Errorf("observed latency = %vs, want the planted 1.5s age (within [1.4, 30])", got.sum)
	}
}

// TestE2ELatencyObservedOnPermanentReject: a permanent SMSC rejection is a terminal outcome and a
// latency worth seeing — a connector that answers fast and refuses everything must not read as a
// connector with no latency at all. The status label is the same closed vocabulary submits_total uses.
func TestE2ELatencyObservedOnPermanentReject(t *testing.T) {
	reg, err := runMetered(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SubmitFailed() }, 0,
		meteredRouted(200*time.Millisecond))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := oneE2E(t, reg)
	if got.status != "rejected" {
		t.Errorf("status = %v, want %v", got.status, "rejected")
	}
	if got.count != 1 {
		t.Errorf("sample count = %v, want %v", got.count, 1)
	}
}

// TestE2ELatencyNotObservedOnTransientReject: a throttle is deliberate backpressure, which the NFR
// (spec §1.2) explicitly excludes from the end-to-end budget. The record is not committed and comes
// back later; observing it here would count the same message once per redelivery AND inject the
// backpressure the budget does not cover.
func TestE2ELatencyNotObservedOnTransientReject(t *testing.T) {
	reg, err := runMetered(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() }, 0,
		meteredRouted(200*time.Millisecond))
	if err == nil {
		t.Fatal("Run = nil, want a redelivery error on a throttled submit")
	}
	if got := gatherE2E(t, reg); len(got) != 0 {
		t.Errorf("%s series = %+v, want none on deliberate backpressure", e2eMetric, got)
	}
}

// TestE2ELatencyNotObservedOnExpiredMessage: a message dead-lettered on the max-age SLA never reaches
// the SMSC, so there is no delivery attempt to time. The NFR excludes deliberate dead-lettering too,
// and an observation here would be the largest value in the histogram every time.
func TestE2ELatencyNotObservedOnExpiredMessage(t *testing.T) {
	reg, err := runMetered(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, time.Minute,
		meteredRouted(2*time.Hour))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := gatherE2E(t, reg); len(got) != 0 {
		t.Errorf("%s series = %+v, want none for a dead-lettered message", e2eMetric, got)
	}
}

// TestE2ELatencyOfAReplayRunsFromTheReplay: a replayed message keeps its original, immutable
// SubmittedAt, which for an operator drain is hours old. Timing from it would push every replayed
// message past the histogram's 5-minute ceiling and wreck the very percentile this metric exists to
// serve. The base is max(SubmittedAt, ReplayedAt) — the same base the max-age SLA already uses.
func TestE2ELatencyOfAReplayRunsFromTheReplay(t *testing.T) {
	r := meteredRouted(2 * time.Hour)
	now := time.Now().UTC()
	r.ReplayedAt = &now

	reg, err := runMetered(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, 0, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := oneE2E(t, reg)
	if got.count != 1 {
		t.Fatalf("sample count = %v, want %v", got.count, 1)
	}
	if got.sum > 60 {
		t.Errorf("observed latency = %vs, want the replay age, not the 2h accept age", got.sum)
	}
}

// TestE2ELatencyObservedPerSegment: the pool's unit of work is a segment, and submits_total already
// counts one per segment. The histogram is observed at the same site with the same labels, so
// message_e2e_duration_seconds_count and submits_total stay comparable; a multipart message therefore
// contributes one sample per segment, all timed from the accept time they share.
func TestE2ELatencyObservedPerSegment(t *testing.T) {
	first := meteredRouted(300 * time.Millisecond)
	first.SegmentSeq, first.SegmentCount, first.HasUDH = 1, 2, true
	second := first
	second.SegmentSeq = 2

	reg, err := runMetered(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, 0, first, second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := oneE2E(t, reg)
	if got.count != 2 {
		t.Errorf("sample count = %v, want %v (one per segment)", got.count, 2)
	}
}

// TestE2ELatencyExcludesTheBookkeepingAfterTheResponse pins the closing edge of the span. The NFR times
// "submission → SMSC delivery attempt", so the clock must stop on the submit_sm_resp: the billing
// settle and the CDR write that follow are our own bookkeeping and belong to no delivery latency.
//
// Both are stalled here, and both sit between the response and the observation site in the code, so
// reading the clock at the observation site instead of at the response would show up in the number.
// The billing settle is the one that matters in production — it is a network round-trip on every sent
// message, and charging it to a delivery budget would let a slow billing service fail an NFR the
// gateway is meeting.
func TestE2ELatencyExcludesTheBookkeepingAfterTheResponse(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }})
	r := meteredRouted(0)
	r.Billable, r.OwnerType = true, "customer"
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}

	reg := metrics.Guard(prometheus.NewRegistry())
	cat := metrics.NewCatalog()
	reg.MustRegister(cat.Collectors()...)

	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}},
		CDR:      slowCDR{delay: 600 * time.Millisecond},
		Billing:  slowSettler{delay: 600 * time.Millisecond},
		// The outcome produce is the bookkeeping step-201c ADDED after the response, and it is a real
		// network round-trip in production. Stalling it is what keeps this test meaningful now that the
		// send path no longer writes ClickHouse: slowCDR alone would be inert here.
		Producer:    slowProducer{delay: 600 * time.Millisecond},
		Metrics:     cat,
		ConnectorID: meteredConnector,
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

	got := oneE2E(t, reg)
	if got.sum >= 0.5 {
		t.Errorf("observed latency = %vs, want well under the 1.2s the settle and the CDR write cost", got.sum)
	}
}

// slowCDR stands in for a stalled ClickHouse writer.
type slowCDR struct{ delay time.Duration }

func (s slowCDR) Insert(context.Context, clickhouse.CDRRow) error {
	time.Sleep(s.delay)
	return nil
}

// slowSettler stands in for a billing service answering slowly. It settles nothing: only the delay is
// under test.
type slowSettler struct{ delay time.Duration }

func (s slowSettler) Capture(context.Context, pipeline.RoutedMT) (bool, *int32) {
	time.Sleep(s.delay)
	return true, nil
}

func (s slowSettler) Release(context.Context, pipeline.RoutedMT) { time.Sleep(s.delay) }

// TestE2ELatencyClampsAClockThatRanBackwards guards the direction that lies in our favour. The accept
// stamp is written by another pod and survives a JSON round trip, so it carries no monotonic reading
// and time.Since falls back to the wall clock. A connector-pool pod whose clock trails the ingest
// pod's yields a negative duration, which Prometheus files into the lowest bucket — so the p99 would
// read "under 10 ms" and any budget would pass trivially, on the one pod whose measurements can least
// be trusted.
func TestE2ELatencyClampsAClockThatRanBackwards(t *testing.T) {
	const skew = time.Hour // the accept stamp sits an hour in this pod's future

	reg, err := runMetered(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, 0,
		meteredRouted(-skew))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	samples := gatherE2E(t, reg)
	if len(samples) != 1 || samples[0].count != 1 {
		t.Fatalf("samples = %+v, want exactly one observation — the fixture never reached the observation site", samples)
	}
	sum := samples[0].sum
	if sum < 0 {
		t.Errorf("observed latency = %vs, want it clamped at or above zero: a negative sample lands in the "+
			"lowest bucket and makes a skewed pod report a p99 it never achieved", sum)
	}
	// An hour of skew must not become an hour of latency either: clamping is to zero, not to |value|.
	if sum > skew.Seconds()/2 {
		t.Errorf("observed latency = %vs, want it near zero rather than the %v of skew", sum, skew)
	}
}

// slowProducer stalls the outcome publish, the bookkeeping step-201c moved after the response.
type slowProducer struct{ delay time.Duration }

func (p slowProducer) Produce(ctx context.Context, _ kafka.Record) error {
	select {
	case <-time.After(p.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
