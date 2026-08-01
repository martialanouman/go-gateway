package metricstream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/metricstream"
)

// fakeSink captures published snapshots. It records every call so a test can assert on what the hot path
// actually put on the wire.
type fakeSink struct {
	mu      sync.Mutex
	values  [][]byte
	keys    [][]byte
	refuse  bool
	refused int
}

func (s *fakeSink) TryPublish(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refuse {
		s.refused++
		return
	}
	s.keys = append(s.keys, key)
	s.values = append(s.values, value)
}

func (s *fakeSink) refusals() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refused
}

func (s *fakeSink) snapshots(t *testing.T) []metricstream.Snapshot {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metricstream.Snapshot, 0, len(s.values))
	for _, v := range s.values {
		var snap metricstream.Snapshot
		if err := json.Unmarshal(v, &snap); err != nil {
			t.Fatalf("decode snapshot: %v (%s)", err, v)
		}
		out = append(out, snap)
	}
	return out
}

func newEmitter(t *testing.T, sink metricstream.Sink, opts ...metricstream.Option) *metricstream.Emitter {
	t.Helper()
	opts = append([]metricstream.Option{metricstream.WithInstance("router-svc-abc123")}, opts...)
	e, err := metricstream.New("router-svc", sink, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

// TestEmitterCapsDistinctSeries is the guard that matters most. The label-name guard of step-180 bounds
// NAMES; nothing bounds VALUES, and an emitter keyed by label values is a map that grows with them — a
// memory leak in the gateway AND a snapshot payload that grows without limit on the wire. The cap is what
// makes an unbounded label value a counted, harmless mistake instead of an outage.
func TestEmitterCapsDistinctSeries(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink, metricstream.WithMaxSeries(10))

	for i := range 100 {
		e.Add("submitted_total", metricstream.Labels{"status": fmt.Sprintf("made-up-%d", i)}, 1)
	}
	e.PublishNow()

	snaps := sink.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("got %d snapshots, want 1", len(snaps))
	}
	if got := len(snaps[0].Samples); got != 10 {
		t.Errorf("published %d series, want the cap of 10", got)
	}
	if got := e.Dropped(); got != 90 {
		t.Errorf("Dropped() = %d, want 90 — a dropped sample must be counted, never silent", got)
	}
}

// TestEmitterRefusesUnboundedLabelNames reuses the step-180 vocabulary rather than inventing a second one:
// one place decides what a bounded label is, for metrics and for the stream alike.
func TestEmitterRefusesUnboundedLabelNames(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)

	e.Add("submitted_total", metricstream.Labels{"msisdn": "33612345678"}, 1)
	e.Add("submitted_total", metricstream.Labels{"status": "accepted"}, 1)
	e.PublishNow()

	snaps := sink.snapshots(t)
	for _, s := range snaps[0].Samples {
		if _, bad := s.Labels["msisdn"]; bad {
			t.Fatal("an msisdn label reached metrics.stream (invariant a)")
		}
	}
	if got := len(snaps[0].Samples); got != 1 {
		t.Errorf("published %d samples, want only the bounded one", got)
	}
	if e.Dropped() != 1 {
		t.Errorf("Dropped() = %d, want the refused sample counted", e.Dropped())
	}
}

// TestEmitterNeverBlocksOnARefusingSink: the whole point is best-effort. A saturated or dead Kafka must cost
// the hot path nothing — the CDR stays the authority for what happened. Counting the loss belongs to the
// sink, which is the only party that learns of it (franz-go reports a full buffer on its own goroutine).
func TestEmitterNeverBlocksOnARefusingSink(t *testing.T) {
	sink := &fakeSink{refuse: true}
	e := newEmitter(t, sink)

	e.Add("submitted_total", metricstream.Labels{"status": "accepted"}, 1)

	done := make(chan struct{})
	go func() { defer close(done); e.PublishNow() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PublishNow blocked on a refusing sink")
	}
	if sink.refusals() != 1 {
		t.Errorf("sink saw %d publishes, want 1 — the emitter must still hand the snapshot over", sink.refusals())
	}
}

// TestCountersAndGaugesAggregate: a counter accumulates between ticks and resets; a gauge reports its last
// value and persists. Getting this backwards would make a dashboard show a rate as a total, or a queue depth
// that vanishes whenever nothing changed.
func TestCountersAndGaugesAggregate(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)

	e.Add("submitted_total", metricstream.Labels{"status": "accepted"}, 2)
	e.Add("submitted_total", metricstream.Labels{"status": "accepted"}, 3)
	e.Set("queue_depth_records", metricstream.Labels{"queue": "mt.inbound"}, 42)
	e.PublishNow()
	e.PublishNow() // a second tick with no activity in between

	snaps := sink.snapshots(t)
	if len(snaps) != 2 {
		t.Fatalf("got %d snapshots, want 2", len(snaps))
	}
	if got := sampleValue(t, snaps[0], "submitted_total"); got != 5 {
		t.Errorf("counter = %v, want 5 (2+3 accumulated)", got)
	}
	if got := sampleValue(t, snaps[1], "submitted_total"); got != 0 {
		t.Errorf("counter after a tick = %v, want 0: a counter reports the interval, not the total", got)
	}
	if got := sampleValue(t, snaps[1], "queue_depth_records"); got != 42 {
		t.Errorf("gauge after a tick = %v, want 42: a gauge is a level, it does not reset", got)
	}
}

// TestObserveEmitsCountAndSum: no quantile machinery on the hot path — the consumer derives rate and mean
// from the pair, which is what a live dashboard needs.
func TestObserveEmitsCountAndSum(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)

	e.Observe("ingest_duration_seconds", metricstream.Labels{"source": "rest"}, 0.010)
	e.Observe("ingest_duration_seconds", metricstream.Labels{"source": "rest"}, 0.030)
	e.PublishNow()

	snap := sink.snapshots(t)[0]
	if got := sampleValue(t, snap, "ingest_duration_seconds_count"); got != 2 {
		t.Errorf("count = %v, want 2", got)
	}
	if got := sampleValue(t, snap, "ingest_duration_seconds_sum"); got < 0.0399 || got > 0.0401 {
		t.Errorf("sum = %v, want ~0.04", got)
	}
}

// TestSnapshotCarriesItsVersionAndService: the payload is a contract with step-183 and with whatever reads
// the topic later. An unversioned schema cannot be changed without breaking consumers silently.
func TestSnapshotCarriesItsVersionAndService(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)
	e.Add("submitted_total", metricstream.Labels{"status": "accepted"}, 1)
	e.PublishNow()

	snap := sink.snapshots(t)[0]
	if snap.V != metricstream.SchemaVersion {
		t.Errorf("v = %d, want %d", snap.V, metricstream.SchemaVersion)
	}
	if snap.Service != "router-svc" {
		t.Errorf("service = %q, want router-svc", snap.Service)
	}
	if snap.EmittedAt.IsZero() {
		t.Error("emitted_at is zero; a consumer cannot judge freshness (< 5 s, step-183)")
	}
	// Without an instance a consumer cannot aggregate: every replica publishes the same kinds under the same
	// service name, and the right rule differs per kind (sum the counters, MAX the group-scoped gauges).
	if snap.Instance != "router-svc-abc123" {
		t.Errorf("instance = %q, want the replica identity", snap.Instance)
	}
	// The partition key must be bounded too: the instance, never anything per-message.
	if got := string(sink.keys[0]); got != "router-svc-abc123" {
		t.Errorf("partition key = %q, want the instance", got)
	}
}

// TestIdleCountersAreReclaimed is what keeps the cap a ceiling on what is LIVE rather than a lifetime quota.
// Without it, one burst of bad label values holds every slot for the life of the process — legitimate series
// stay locked out long after the bug is fixed — and every snapshot grows towards "every series ever seen".
func TestIdleCountersAreReclaimed(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink, metricstream.WithMaxSeries(3))

	for i := range 3 {
		e.Add("rejected_total", metricstream.Labels{"code": fmt.Sprintf("code-%d", i)}, 1)
	}
	e.PublishNow() // publishes the interval and zeroes the counters
	e.PublishNow() // a fully idle interval: now they are reclaimed
	if e.Dropped() != 0 {
		t.Fatalf("Dropped() = %d before the cap was reached", e.Dropped())
	}

	// The cap must now accept three NEW series: the previous ones went quiet and released their slots.
	for i := 3; i < 6; i++ {
		e.Add("rejected_total", metricstream.Labels{"code": fmt.Sprintf("code-%d", i)}, 1)
	}
	e.PublishNow()

	if got := e.Dropped(); got != 0 {
		t.Errorf("Dropped() = %d; the cap did not release the slots of idle series", got)
	}
	snaps := sink.snapshots(t)
	last := snaps[len(snaps)-1]
	if got := len(last.Samples); got != 3 {
		t.Errorf("last snapshot has %d samples, want the 3 new ones and none of the silent ones", got)
	}
}

// TestSetOneHotIsAtomic: as separate Set calls a drain can land between them and publish two states at 1 —
// a connector shown as open AND closed, which is worse than no metric. One lock for the whole enum.
func TestSetOneHotIsAtomic(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)
	states := []string{"closed", "open", "half_open"}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			e.SetOneHot("connector_breaker_state", metricstream.Labels{"connector_id": "c1"},
				"state", states, states[i%len(states)])
		}
	}()
	for range 200 {
		e.PublishNow()
	}
	close(stop)
	wg.Wait()

	for _, snap := range sink.snapshots(t) {
		var hot int
		for _, s := range snap.Samples {
			if s.Kind == "connector_breaker_state" && s.Value == 1 {
				hot++
			}
		}
		if hot > 1 {
			t.Fatalf("%d states at 1 in one snapshot: the enum was published mid-update", hot)
		}
	}
}

// TestHotPathNeverTouchesTheSink is the structural half of "best-effort". Recording must be isolated from
// Kafka by construction, not by the sink implementation behaving well — publishing happens on the ticker.
func TestHotPathNeverTouchesTheSink(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)

	e.Add("messages_total", metricstream.Labels{"status": "routed"}, 1)
	e.Set("queue_depth_records", metricstream.Labels{"queue": "mt.inbound"}, 3)
	e.Observe("pipeline_duration_seconds", nil, 0.01)

	if got := len(sink.snapshots(t)); got != 0 {
		t.Errorf("the sink saw %d publishes from the hot path; it must see none until a tick", got)
	}
}

// TestEmitterPublishesNothingWhenIdle: an empty snapshot every tick would be pure noise on the topic and in
// every connected browser.
func TestEmitterPublishesNothingWhenIdle(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)

	e.PublishNow()

	if len(sink.snapshots(t)) != 0 {
		t.Error("an idle emitter published a snapshot")
	}
}

// TestRunStopsWithItsContext: no goroutine without a stop condition (CLAUDE.md).
func TestRunStopsWithItsContext(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx, 10*time.Millisecond) }()

	e.Add("submitted_total", metricstream.Labels{"status": "accepted"}, 1)
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return when its context was cancelled")
	}
	if len(sink.snapshots(t)) == 0 {
		t.Error("Run never published on its ticker")
	}
}

// TestConcurrentHotPathIsSafe: the emitter is fed from every pipeline goroutine at once.
func TestConcurrentHotPathIsSafe(t *testing.T) {
	sink := &fakeSink{}
	e := newEmitter(t, sink)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				e.Add("submitted_total", metricstream.Labels{"status": "accepted"}, 1)
				e.Set("queue_depth_records", metricstream.Labels{"queue": "mt.inbound"}, 7)
				e.Observe("ingest_duration_seconds", metricstream.Labels{"source": "rest"}, 0.001)
			}
		}()
	}
	go e.PublishNow()
	wg.Wait()
	e.PublishNow()

	var total float64
	for _, s := range sink.snapshots(t) {
		for _, sam := range s.Samples {
			if sam.Kind == "submitted_total" {
				total += sam.Value
			}
		}
	}
	if total != 1600 {
		t.Errorf("counter total across snapshots = %v, want 1600 (no lost or double-counted increment)", total)
	}
}

func sampleValue(t *testing.T, snap metricstream.Snapshot, kind string) float64 {
	t.Helper()
	for _, s := range snap.Samples {
		if s.Kind == kind {
			return s.Value
		}
	}
	if strings.HasSuffix(kind, "_count") || strings.HasSuffix(kind, "_sum") {
		t.Fatalf("no sample %q in %+v", kind, snap.Samples)
	}
	return 0
}
