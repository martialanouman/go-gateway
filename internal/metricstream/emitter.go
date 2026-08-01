package metricstream

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

// DefaultMaxSeries caps the distinct label combinations one emitter tracks.
//
// This is the guard that actually holds. The registry guard of step-180 bounds label NAMES; nothing bounds
// their VALUES, and an aggregator keyed by label values is a map that grows with them — an unbounded label
// value would be a memory leak in the gateway and a snapshot payload growing without limit on the wire. The
// cap turns that mistake into a counted, harmless one.
//
// 1 000 is far above any legitimate use (a few kinds times a few dozen connectors) and far below anything
// that costs memory. Idle series are reclaimed (see [Emitter.PublishNow]), so this is a ceiling on what is
// LIVE, not a lifetime quota — one bad burst of label values must not lock the emitter out for the life of
// the process, long after the bug that caused it was fixed.
const DefaultMaxSeries = 1000

// gaugeIdleTicks is how many consecutive silent intervals retire a gauge.
//
// A gauge is a level, so it must survive an interval nobody touched — a queue depth nobody refreshed is still
// the queue depth. But it must not survive forever: a connector removed from the control plane would keep
// publishing its last known state for the life of the pod, and hold a series slot with it. Twelve ticks is a
// couple of minutes at the default cadence — far past any legitimate gap, short enough that a dashboard stops
// showing a decommissioned connector.
const gaugeIdleTicks = 12

// Emitter aggregates measurements on the hot path and publishes them as periodic snapshots.
//
// Every recording method is non-blocking and returns nothing: a caller on the message path must not be able
// to handle, log or branch on a stream failure. None of them touches the sink — publishing happens on the
// ticker goroutine — so the hot path is isolated from Kafka BY CONSTRUCTION, not by the discipline of the
// sink implementation.
//
// What is refused (an unbounded label name, a series past the cap) is counted and reported in the next
// snapshot, so a mistake is visible rather than silent.
type Emitter struct {
	service  string
	instance string
	sink     Sink

	maxSeries int
	now       func() time.Time

	mu       sync.Mutex
	counters map[string]*series
	gauges   map[string]*series
	// Scratch reused under mu so a steady-state recording allocates nothing at all: the map key bytes, the
	// sorted label names, and the kind_count/kind_sum names Observe would otherwise rebuild every call.
	keyBuf       []byte
	nameBuf      [maxInlineLabels]string
	observeNames map[string][2]string

	dropped atomic.Int64
}

// series is one (kind, labels) tuple and its accumulated value.
type series struct {
	kind      string
	labels    Labels
	value     float64
	idleTicks int // gauges only: consecutive intervals with no write
}

// Option configures an Emitter.
type Option func(*Emitter)

// WithMaxSeries overrides [DefaultMaxSeries].
func WithMaxSeries(n int) Option { return func(e *Emitter) { e.maxSeries = n } }

// WithClock injects a clock, for tests that assert on emitted_at.
func WithClock(now func() time.Time) Option { return func(e *Emitter) { e.now = now } }

// WithInstance overrides the instance identity, which defaults to the hostname. See [Snapshot.Instance].
func WithInstance(id string) Option { return func(e *Emitter) { e.instance = id } }

// New builds an Emitter publishing on behalf of service. It starts nothing; call [Emitter.Run].
func New(service string, sink Sink, opts ...Option) (*Emitter, error) {
	if service == "" {
		return nil, fmt.Errorf("metricstream: service name is required")
	}
	if sink == nil {
		return nil, fmt.Errorf("metricstream: sink is required")
	}
	e := &Emitter{
		service:   service,
		instance:  defaultInstance(),
		sink:      sink,
		maxSeries: DefaultMaxSeries,
		now:       time.Now,
		counters:  make(map[string]*series),
		gauges:    make(map[string]*series),
		keyBuf:    make([]byte, 0, 128),

		observeNames: make(map[string][2]string),
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.maxSeries <= 0 {
		return nil, fmt.Errorf("metricstream: max series must be positive, got %d", e.maxSeries)
	}
	return e, nil
}

// defaultInstance identifies this replica. The hostname is the pod name under Kubernetes, which is what an
// operator recognises; an unavailable hostname degrades to "unknown" rather than failing a service over a
// dashboard identity.
func defaultInstance() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "unknown"
	}
	return host
}

// Add accumulates a counter delta over the current interval. The snapshot reports the interval's total, then
// the counter resets — a consumer renders a rate without keeping state, and a browser reconnecting mid-day is
// never shown a total accumulated since boot.
func (e *Emitter) Add(kind string, labels Labels, delta float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s := e.locate(e.counters, kind, labels); s != nil {
		s.value += delta
	}
}

// Set records a gauge's current level. Unlike a counter it persists across ticks: a queue depth that nothing
// changed is still the queue depth, and must not read as zero.
func (e *Emitter) Set(kind string, labels Labels, value float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s := e.locate(e.gauges, kind, labels); s != nil {
		s.value = value
		s.idleTicks = 0
	}
}

// SetOneHot models an enum as one gauge per value, exactly one at 1, under a SINGLE lock acquisition.
//
// The single lock is the point: as N separate Set calls, a snapshot can land between them and publish two
// states at 1 — a connector shown as open AND closed, which is worse than no metric at all.
//
// An unrecognised current value leaves every gauge at 0 rather than inventing a series: the label stays
// bounded whatever a caller passes, and the anomaly is visible, since the values no longer sum to 1.
func (e *Emitter) SetOneHot(kind string, base Labels, dimension string, values []string, current string) {
	labels := make(Labels, len(base)+1)
	maps.Copy(labels, base)

	e.mu.Lock()
	defer e.mu.Unlock()
	for _, v := range values {
		labels[dimension] = v
		value := 0.0
		if v == current {
			value = 1
		}
		if s := e.locate(e.gauges, kind, labels); s != nil {
			s.value = value
			s.idleTicks = 0
		}
	}
}

// Observe records a duration as a count/sum pair, published as kind_count and kind_sum.
//
// No quantiles: a p95 needs a histogram on the hot path, and a live dashboard needs a rate and a mean, which
// the pair gives for two additions. Prometheus keeps the real distribution (step-180) for the questions that
// need it.
func (e *Emitter) Observe(kind string, labels Labels, seconds float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Derived once per kind, not per call: kind+"_count" is a string concatenation, and at 8 000 msg/s two
	// of those per message is pure garbage on the hottest path in the system.
	names, ok := e.observeNames[kind]
	if !ok {
		names = [2]string{kind + "_count", kind + "_sum"}
		e.observeNames[kind] = names
	}
	if s := e.locate(e.counters, names[0], labels); s != nil {
		s.value++
	}
	if s := e.locate(e.counters, names[1], labels); s != nil {
		s.value += seconds
	}
}

// Dropped is the number of samples this emitter refused since start: an unbounded label name, or a series
// past the cap. It does NOT count snapshots the sink dropped — the sink knows about those and counts them
// itself, on its own Prometheus counter.
func (e *Emitter) Dropped() int64 { return e.dropped.Load() }

// locate finds or creates the series for (kind, labels). The caller must hold e.mu. It returns nil when the
// sample is refused, having counted the drop.
//
// Label names are validated only on a MISS. That is safe because a forbidden name never creates a series and
// therefore always misses — and it keeps the steady-state hot path to a single map lookup with no allocation
// at all (the key is built into a reused buffer, and Go elides the copy in map[string(buf)]).
func (e *Emitter) locate(bucket map[string]*series, kind string, labels Labels) *series {
	e.keyBuf = appendKey(e.keyBuf[:0], kind, e.sortedNames(labels), labels)
	if s, ok := bucket[string(e.keyBuf)]; ok {
		return s
	}
	if err := metrics.ValidateLabelNames(kind, e.sortedNames(labels)); err != nil {
		e.dropped.Add(1)
		return nil
	}
	if len(e.counters)+len(e.gauges) >= e.maxSeries {
		e.dropped.Add(1)
		return nil
	}
	s := &series{kind: kind, labels: maps.Clone(labels)}
	bucket[string(e.keyBuf)] = s
	return s
}

// PublishNow builds a snapshot of the current interval and hands it to the sink. It is what the ticker calls;
// tests call it directly. An interval with nothing to say publishes nothing — an empty snapshot every second
// would be noise on the topic and in every connected browser.
func (e *Emitter) PublishNow() {
	snap, ok := e.drain()
	if !ok {
		return
	}
	value, err := json.Marshal(snap)
	if err != nil {
		// A snapshot is plain data; marshalling it cannot fail in practice. Count rather than crash: this
		// path must never be able to take a service down.
		e.dropped.Add(1)
		return
	}
	// The partition key is the INSTANCE — bounded by the replica count, never anything per-message — so one
	// replica's snapshots stay ordered among themselves, and the topic's partitioning cannot become a
	// cardinality problem of its own.
	e.sink.TryPublish([]byte(e.instance), value)
}

// drain snapshots the current state and reclaims what has gone quiet.
//
// A counter that recorded nothing this interval is DELETED rather than published as zero. Publishing it would
// grow every snapshot towards "every series ever seen" and, worse, hold its slot under the cap for the life
// of the process — one bad burst of label values would lock out legitimate series forever, long after the bug
// was fixed. Absence is the zero, and a consumer reads it as such.
//
// Reclaim happens one interval AFTER the last write, not immediately: a series is published, zeroed, and only
// deleted if the next interval is silent too. A busy counter would otherwise be destroyed and rebuilt every
// second, which is allocation churn on the hottest series for no benefit.
func (e *Emitter) drain() (Snapshot, bool) {
	e.mu.Lock()
	samples := make([]Sample, 0, len(e.counters)+len(e.gauges))
	for key, s := range e.counters {
		if s.value == 0 {
			delete(e.counters, key)
			continue
		}
		samples = append(samples, Sample{Kind: s.kind, Labels: maps.Clone(s.labels), Value: s.value})
		s.value = 0
	}
	for key, s := range e.gauges {
		s.idleTicks++
		if s.idleTicks > gaugeIdleTicks {
			delete(e.gauges, key)
			continue
		}
		samples = append(samples, Sample{Kind: s.kind, Labels: maps.Clone(s.labels), Value: s.value})
	}
	e.mu.Unlock()

	if len(samples) == 0 {
		return Snapshot{}, false
	}
	// Deterministic order so a diff of two snapshots is readable and a test is not flaky. The sort key is
	// built once per sample rather than inside the comparator, which would rebuild it O(n log n) times.
	keys := make([]string, len(samples))
	for i, s := range samples {
		var scratch [maxInlineLabels]string
		names := scratch[:0]
		for name := range s.Labels {
			names = append(names, name)
		}
		slices.Sort(names)
		keys[i] = s.Kind + string(appendKey(nil, "", names, s.Labels))
	}
	sort.Sort(&samplesByKey{samples: samples, keys: keys})
	return Snapshot{
		V:                 SchemaVersion,
		Feed:              FeedMetrics,
		Service:           e.service,
		Instance:          e.instance,
		EmittedAt:         e.now().UTC(),
		Samples:           samples,
		DroppedSinceStart: e.dropped.Load(),
	}, true
}

// Run publishes a snapshot every interval until ctx is cancelled, then publishes once more so the final
// interval is not lost at every redeployment. It returns on cancellation — no goroutine without a stop
// condition (CLAUDE.md).
func (e *Emitter) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			e.PublishNow()
			return
		case <-ticker.C:
			e.PublishNow()
		}
	}
}

// maxInlineLabels is the label count sortedNames handles out of the emitter's own scratch array. Nothing in
// the catalogue comes close; past it the heap path is still correct, just not free.
const maxInlineLabels = 8

// sortedNames returns the label names in a stable order, reusing the emitter's scratch array so the result
// costs nothing. The caller must hold e.mu, and must be done with the slice before the next call.
func (e *Emitter) sortedNames(labels Labels) []string {
	if len(labels) > maxInlineLabels {
		return slices.Sorted(maps.Keys(labels))
	}
	names := e.nameBuf[:0]
	for name := range labels {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// appendKey writes a stable key for (kind, labels) into dst, given the label names already sorted.
func appendKey(dst []byte, kind string, names []string, labels Labels) []byte {
	dst = append(dst, kind...)
	for _, name := range names {
		dst = append(dst, 0x1f) // a unit separator cannot appear in a label name or value
		dst = append(dst, name...)
		dst = append(dst, '=')
		dst = append(dst, labels[name]...)
	}
	return dst
}

// samplesByKey sorts samples alongside their precomputed keys.
type samplesByKey struct {
	samples []Sample
	keys    []string
}

func (s *samplesByKey) Len() int           { return len(s.samples) }
func (s *samplesByKey) Less(i, j int) bool { return s.keys[i] < s.keys[j] }
func (s *samplesByKey) Swap(i, j int) {
	s.samples[i], s.samples[j] = s.samples[j], s.samples[i]
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}
