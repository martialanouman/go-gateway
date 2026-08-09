// Package redpandametrics reads the test broker's own Prometheus exposition, so a run can say whether
// a throughput ceiling belongs to the broker or to the gateway.
//
// It is the third peer reader in the load harness, after smscmetrics and gatewaymetrics, and the one
// that made them share their HTTP half (test/load/promscrape). What lives here is what cannot be
// shared: Redpanda's metric contract.
//
// # Why this reader exists
//
// Every figure the harness had about the broker was a subtraction. The reference run measures the Go
// process's CPU with getrusage, which counts the process ONLY — Redpanda, Postgres and ClickHouse run
// in containers beside it — so "1.6 cores of 14" was a floor on the host's load and never a total, and
// the most likely suspect for a ceiling was precisely the thing not being measured (step-201e D2).
//
// # Which exposition, and why it matters more than it looks
//
// Redpanda serves two on the admin port (9644):
//
//   - /metrics — internal, vectorized_* prefixed, thousands of series. It carries per-handler detail
//     the curated set drops, including offset_commit.
//   - /public_metrics — curated, redpanda_* prefixed, aggregated labels. This is what NewClient reads.
//
// A reader pointed at the wrong one gets a successful 200 full of series it does not recognise, and
// reports a broker that appears to be doing nothing. That is why promscrape.Config makes the path
// explicit rather than defaulting it.
//
// # What the curated set does and does not carry
//
// Verified against a real v24.2.18 reading rather than from memory (testdata/public_metrics.txt):
//
//   - redpanda_kafka_handler_latency_seconds{handler} — produce and fetch only. This is service
//     latency: what a client waited for.
//   - redpanda_kafka_request_latency_seconds{redpanda_request} — produce and consume. This is the
//     broker's INTERNAL latency, a different question, kept separate rather than merged.
//   - redpanda_cpu_busy_seconds_total{shard} — cumulative seconds, one series per shard. Typed as a
//     gauge by the broker, which is why values are read type-agnostically.
//   - redpanda_kafka_request_bytes_total{redpanda_request,redpanda_topic} — per topic. It is the view
//     control: a zero delta means the scrape missed the traffic, NOT that the broker was idle.
//
// offset_commit has NO curated series. step-201e asked for it; the broker does not offer it here, and
// reading /metrics for that one figure is left to whoever needs it (the internal family is
// vectorized_kafka_handler_latency_microseconds{handler}). Recorded rather than guessed at.
package redpandametrics

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/martialanouman/go-gateway/test/load/promscrape"
)

const (
	// namespace prefixes every error, so a run with three readers says which one failed.
	namespace = "redpandametrics"

	// publicMetricsPath is the curated exposition. See the package doc: the other one parses fine and
	// answers nothing.
	publicMetricsPath = "/public_metrics"

	// minWindow is the shortest interval Rate accepts. Below it the elapsed time is dominated by scrape
	// jitter and every derived figure is noise wearing a unit.
	minWindow = time.Second

	metricHandlerLatency = "redpanda_kafka_handler_latency_seconds"
	metricRequestLatency = "redpanda_kafka_request_latency_seconds"
	metricCPUBusy        = "redpanda_cpu_busy_seconds_total"
	metricRequestBytes   = "redpanda_kafka_request_bytes_total"

	labelHandler = "handler"
	labelRequest = "redpanda_request"
	labelTopic   = "redpanda_topic"
	labelShard   = "shard"
)

// Bucket is one cumulative histogram bucket: how many observations were at or below UpperBound.
type Bucket struct {
	UpperBound float64
	Cumulative uint64
}

// Latency is one API's latency histogram, as the broker exposes it: cumulative since boot.
type Latency struct {
	Sum     float64
	Count   uint64
	Buckets []Bucket
}

// TopicKey identifies one direction of traffic on one topic.
type TopicKey struct{ Request, Topic string }

// Snapshot is one reading of the broker's curated exposition.
type Snapshot struct {
	// At is stamped just before the request went out, so a window built from two of these is never
	// shorter than the real one. See promscrape.Client.Scrape.
	At time.Time

	// Handlers is service latency by Kafka API ("produce", "fetch").
	Handlers map[string]Latency

	// Requests is the broker's internal latency by API ("produce", "consume").
	Requests map[string]Latency

	// CPUBusy is cumulative busy seconds per shard id. Kept split rather than summed: the number of
	// shards is the denominator of any utilisation figure, so it has to be counted, not assumed.
	CPUBusy map[string]float64

	// Bytes is traffic per topic and direction, the view control.
	Bytes map[TopicKey]float64
}

// CPUBusySeconds totals the busy time across every shard.
func (s Snapshot) CPUBusySeconds() float64 {
	var total float64
	for _, v := range s.CPUBusy {
		total += v
	}
	return total
}

// Client scrapes one broker's curated exposition. It is safe for concurrent use.
type Client struct{ inner *promscrape.Client }

// Option configures a Client.
type Option func(*clientOptions)

type clientOptions struct{ http *http.Client }

// WithHTTPClient replaces the HTTP client used for scraping. The scrape deadline comes from the
// context passed to Scrape, so a client set here needs no timeout of its own.
func WithHTTPClient(c *http.Client) Option {
	return func(o *clientOptions) {
		if c != nil {
			o.http = c
		}
	}
}

// NewClient builds a scraper for endpoint, which may be the broker's admin origin
// ("http://127.0.0.1:9644") or a full URL. A bare origin is scraped at /public_metrics — never
// /metrics, which would return a healthy-looking 200 carrying none of the families below.
func NewClient(endpoint string, opts ...Option) (*Client, error) {
	var o clientOptions
	for _, opt := range opts {
		opt(&o)
	}
	var popts []promscrape.Option
	if o.http != nil {
		popts = append(popts, promscrape.WithHTTPClient(o.http))
	}
	inner, err := promscrape.New(promscrape.Config{
		Namespace:   namespace,
		Endpoint:    endpoint,
		DefaultPath: publicMetricsPath,
	}, popts...)
	if err != nil {
		return nil, err
	}
	return &Client{inner: inner}, nil
}

// URL reports the URL the client scrapes, with any password masked.
func (c *Client) URL() string { return c.inner.URL() }

// Scrape takes one reading.
func (c *Client) Scrape(ctx context.Context) (Snapshot, error) {
	var snap Snapshot
	err := c.inner.Scrape(ctx, func(body io.Reader, at time.Time) error {
		var err error
		snap, err = Parse(body, at)
		return err
	})
	if err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// Parse reads a curated exposition. It is pure, so it is tested against a captured reading rather than
// a live broker.
func Parse(r io.Reader, at time.Time) (Snapshot, error) {
	// NewTextParser, not a zero-value TextParser: since prometheus/common v0.66 the parser carries its
	// own validation scheme, whose zero value is UnsetValidation and panics on the first metric name.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%s: parse exposition: %w", namespace, err)
	}

	snap := Snapshot{
		At:       at,
		Handlers: map[string]Latency{},
		Requests: map[string]Latency{},
		CPUBusy:  map[string]float64{},
		Bytes:    map[TopicKey]float64{},
	}

	for name, fam := range families {
		for _, m := range fam.GetMetric() {
			switch name {
			case metricHandlerLatency:
				lat, err := readHistogram(m, name)
				if err != nil {
					return Snapshot{}, err
				}
				snap.Handlers[labelValue(m, labelHandler)] = lat
			case metricRequestLatency:
				lat, err := readHistogram(m, name)
				if err != nil {
					return Snapshot{}, err
				}
				snap.Requests[labelValue(m, labelRequest)] = lat
			case metricCPUBusy:
				v, err := finite(sampleValue(m), name)
				if err != nil {
					return Snapshot{}, err
				}
				// Summed rather than assigned: a shard label the broker stops emitting would otherwise
				// silently drop its contribution.
				snap.CPUBusy[labelValue(m, labelShard)] += v
			case metricRequestBytes:
				v, err := finite(sampleValue(m), name)
				if err != nil {
					return Snapshot{}, err
				}
				snap.Bytes[TopicKey{Request: labelValue(m, labelRequest), Topic: labelValue(m, labelTopic)}] += v
			}
			// Anything else is skipped in silence: the curated set carries hundreds of families this
			// reader has no question for, and treating an unknown one as an error would make the reader
			// fail on the next broker release.
		}
	}
	return snap, nil
}

// readHistogram turns one histogram series into a Latency, rebuilding the +Inf bucket from the family's
// own count rather than trusting the exposition's — the same choice gatewaymetrics made, for the same
// reason: the two must agree, and the count is the one the mean divides by.
func readHistogram(m *dto.Metric, name string) (Latency, error) {
	h := m.GetHistogram()
	if h == nil {
		return Latency{}, fmt.Errorf("%s: %s carried no histogram", namespace, name)
	}
	sum, err := finite(h.GetSampleSum(), name)
	if err != nil {
		return Latency{}, err
	}
	out := Latency{Sum: sum, Count: h.GetSampleCount()}
	for _, b := range h.GetBucket() {
		if math.IsInf(b.GetUpperBound(), 1) {
			continue
		}
		out.Buckets = append(out.Buckets, Bucket{UpperBound: b.GetUpperBound(), Cumulative: b.GetCumulativeCount()})
	}
	sort.Slice(out.Buckets, func(i, j int) bool { return out.Buckets[i].UpperBound < out.Buckets[j].UpperBound })
	out.Buckets = append(out.Buckets, Bucket{UpperBound: math.Inf(1), Cumulative: out.Count})
	return out, nil
}

// finite refuses NaN and ±Inf. Both are legal in the text format and expfmt decodes them as-is; a NaN
// propagates into a mean and prints as a figure, and a +Inf passes any "value >= 0" guard and prints as
// an infinite ceiling. A reading that cannot be trusted must not read as a measurement.
func finite(v float64, name string) (float64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("%s: %s carried a non-finite value (%v): this reading cannot be scored", namespace, name, v)
	}
	return v, nil
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// sampleValue reads a counter, gauge or untyped sample, so an exposition served without "# TYPE" lines
// parses identically to one with them — and so redpanda_cpu_busy_seconds_total, which the broker types
// as a gauge despite the _total suffix, is read at all.
func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetUntyped() != nil:
		return m.GetUntyped().GetValue()
	default:
		return 0
	}
}

// Served is what one API did during the window.
type Served struct {
	API      string
	Requests uint64
	// Mean is total service time divided by requests served IN THE WINDOW, never since boot.
	Mean time.Duration
}

// Report is what the broker did between two readings.
type Report struct {
	Window time.Duration
	// Handlers is per-API service, sorted by request count descending so the busiest reads first.
	Handlers []Served
	// CPUCores is the CPU the broker itself burned, in cores.
	CPUCores float64
	// Shards is what CPUCores is out of. A core count without it is unreadable.
	Shards int
	// Bytes is total traffic across every topic, the view control.
	Bytes float64
}

// Rate derives what the broker did between two readings.
//
// It refuses rather than reports on three inputs: readings out of order, a window too short to divide
// by, and a counter that went backwards — which means the broker restarted mid-window and every delta
// is meaningless. Each would otherwise produce a plausible number.
func Rate(before, after Snapshot) (Report, error) {
	window := after.At.Sub(before.At)
	if window < minWindow {
		return Report{}, fmt.Errorf("%s: the two readings are %v apart, need at least %v: below that the "+
			"window is scrape jitter and every figure derived from it is noise", namespace, window, minWindow)
	}

	rep := Report{Window: window, Shards: len(after.CPUBusy)}
	for api, a := range after.Handlers {
		b := before.Handlers[api]
		if a.Count < b.Count || a.Sum < b.Sum {
			return Report{}, fmt.Errorf("%s: %s went backwards over the window (%d -> %d requests): the "+
				"broker restarted, and no delta across it means anything", namespace, api, b.Count, a.Count)
		}
		served := a.Count - b.Count
		if served == 0 {
			continue
		}
		mean := (a.Sum - b.Sum) / float64(served)
		rep.Handlers = append(rep.Handlers, Served{
			API:      api,
			Requests: served,
			Mean:     time.Duration(mean * float64(time.Second)),
		})
	}
	sort.Slice(rep.Handlers, func(i, j int) bool { return rep.Handlers[i].Requests > rep.Handlers[j].Requests })

	cpu := after.CPUBusySeconds() - before.CPUBusySeconds()
	if cpu < 0 {
		return Report{}, fmt.Errorf("%s: broker CPU went backwards over the window: the broker restarted", namespace)
	}
	rep.CPUCores = cpu / window.Seconds()

	for k, v := range after.Bytes {
		rep.Bytes += v - before.Bytes[k]
	}
	return rep, nil
}

// Render states what the broker served and what it burned, and names what the figure excludes.
//
// The exclusion clause is not decoration. cpuShare in internal/e2e carries the same one for the same
// reason: a core count with nothing said about its scope gets read as a host total, and the whole
// point of this reader is that the host total was never what anyone was measuring.
func (r Report) Render() string {
	if len(r.Handlers) == 0 {
		return fmt.Sprintf("no kafka request served in %v — the scrape did not cover the run, "+
			"which is not the same as an idle broker", r.Window)
	}
	parts := make([]string, 0, len(r.Handlers))
	for _, h := range r.Handlers {
		parts = append(parts, fmt.Sprintf("%s %d req at %v mean", h.API, h.Requests, roundLatency(h.Mean)))
	}
	return fmt.Sprintf("broker served %s over %v; it burned %.2f cores across %d shards (the gateway "+
		"process is NOT counted here, and neither is any other container)",
		strings.Join(parts, ", "), r.Window, r.CPUCores, r.Shards)
}

// roundLatency keeps the rendered mean readable without pretending to nanosecond precision on a
// figure derived from a bucketed histogram.
func roundLatency(d time.Duration) time.Duration {
	if d < time.Millisecond {
		return d.Round(time.Microsecond)
	}
	return d.Round(10 * time.Microsecond)
}
