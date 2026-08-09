// Package gatewaymetrics reads the gateway's own Prometheus endpoint and turns the end-to-end latency
// histogram into a verdict on the NFR budget (spec §1.2: p50 < 400 ms, p99 < 2 s, submission → SMSC
// delivery attempt).
//
// It is the gateway-side counterpart of test/load/smscmetrics, and it exists for the same reason: the
// number that decides a run must come from the process that owns the fact. message_e2e_duration_seconds
// is observed by connector-pool on the submit_sm_resp, with both ends of the span inside one process, so
// nothing here has to correlate a message id or trust an injector's own timing.
//
// # What a verdict from an exposition can and cannot say
//
// A Prometheus text exposition carries CUMULATIVE BUCKETS, not values. The q-th quantile is therefore
// never a number — it is an interval (lower, upper] between two bucket edges, and this package returns
// exactly that. It never interpolates: on this histogram the edges are a factor of 2 apart, so an
// interpolated figure would carry up to 100% error while reading like a measurement.
//
// The verdict is consequently three-valued:
//
//   - [Pass] — the interval's upper edge is at or below the budget. Proven: every observation at or
//     below the quantile is at most that edge.
//   - [Fail] — the interval's lower edge is at or above the budget. Proven the other way.
//   - [Indeterminate] — the budget falls strictly inside the interval. Nothing in the exposition can
//     decide it, and this package says so rather than picking a side.
//
// Indeterminate is not reachable for the two budgets the spec states: the catalogue splices 0.4 s and
// 2 s into the histogram's bucket edges precisely so those two questions have answers. Any other budget
// may well land mid-bucket.
//
// Native histograms would remove the whole limitation, and the catalogue enables them — but they only
// travel over the protobuf exposition, which promhttp serves solely to a client that negotiates it.
// This package reads the text format, so it sees classic buckets. Reading the protobuf form is the
// upgrade path if the interval ever proves too coarse.
//
// Usage is one scrape before the load, one after, and a subtraction so the pod's prior traffic does not
// dilute the run:
//
//	c, _ := gatewaymetrics.NewClient("http://127.0.0.1:9100")
//	before, _ := c.Scrape(ctx)
//	// ... hold the load steady ...
//	after, _ := c.Scrape(ctx)
//	win, _ := gatewaymetrics.Sub(before.Total(), after.Total())
//	verdict, q, err := win.CheckBudget(0.99, 2*time.Second)
package gatewaymetrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/martialanouman/go-gateway/test/load/promscrape"
)

// MetricE2E is the histogram this package reads, declared in internal/observability/metrics.
const MetricE2E = "message_e2e_duration_seconds"

// Label names carried by MetricE2E.
const (
	labelConnectorID = "connector_id"
	labelStatus      = "status"
)

// Errors returned by this package. Every one of them means "this run cannot be scored", never "the
// budget was met" — a reading that cannot be trusted must not read as a pass.
var (
	// ErrNoObservations reports a histogram with nothing in it. It is the failure mode this package was
	// written against: a metric nobody feeds exposes zero observations, and zero observations answer no
	// question at all.
	ErrNoObservations = errors.New("gatewaymetrics: histogram carries no observation")

	// ErrBadQuantile reports a quantile outside the open interval (0, 1).
	ErrBadQuantile = errors.New("gatewaymetrics: quantile must be strictly between 0 and 1")

	// ErrCounterReset reports a histogram that went backwards between two readings — the pod restarted,
	// or its registry was reset. The window spans a discontinuity, so no delta over it is meaningful.
	ErrCounterReset = errors.New("gatewaymetrics: histogram went backwards between readings")

	// ErrBucketMismatch reports two readings whose bucket edges differ, which a redeploy with changed
	// buckets produces. Subtracting them would silently compare different partitions of the same range.
	ErrBucketMismatch = errors.New("gatewaymetrics: readings do not share the same bucket boundaries")
)

// Bucket is one cumulative bucket: Cumulative observations were at most UpperBound seconds.
type Bucket struct {
	// UpperBound is the bucket's inclusive upper edge in seconds, +Inf for the overflow bucket.
	UpperBound float64

	// Cumulative counts every observation at or below UpperBound, not just those inside this bucket.
	Cumulative uint64
}

// Unbounded reports whether this is the overflow (+Inf) bucket, whose observations have no upper edge.
func (b Bucket) Unbounded() bool { return math.IsInf(b.UpperBound, 1) }

// bucketJSON carries UpperBound as a string so a Histogram survives a round trip through a file.
//
// encoding/json rejects +Inf outright — and every Histogram ends at +Inf, so the obvious struct tags
// would make a recorded baseline impossible rather than merely lossy. Prometheus already spells that
// edge "+Inf" in its own exposition, so the same spelling is used here.
type bucketJSON struct {
	UpperBound string `json:"upper_bound"`
	Cumulative uint64 `json:"cumulative"`
}

// MarshalJSON renders the bucket with its upper edge as a string, "+Inf" included.
func (b Bucket) MarshalJSON() ([]byte, error) {
	bound := "+Inf"
	if !b.Unbounded() {
		bound = strconv.FormatFloat(b.UpperBound, 'g', -1, 64)
	}
	return json.Marshal(bucketJSON{UpperBound: bound, Cumulative: b.Cumulative})
}

// UnmarshalJSON reads back what MarshalJSON wrote, "+Inf" included.
func (b *Bucket) UnmarshalJSON(data []byte) error {
	var raw bucketJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	bound, err := strconv.ParseFloat(raw.UpperBound, 64)
	if err != nil {
		return fmt.Errorf("gatewaymetrics: bucket upper bound %q: %w", raw.UpperBound, err)
	}
	b.UpperBound, b.Cumulative = bound, raw.Cumulative
	return nil
}

// Histogram is one message_e2e_duration_seconds series, or an aggregate of several.
//
// Buckets are ascending by UpperBound and always end at +Inf, whether or not the exposition spelled
// that bucket out: every quantile above the last finite edge depends on it existing.
type Histogram struct {
	Buckets []Bucket
	Count   uint64
	Sum     float64
}

// Mean is the arithmetic mean of the observations, zero when there are none.
//
// It is NOT a percentile and must not stand in for one: a burst of slow sends barely moves it. It is
// reported alongside a verdict as context, never as the verdict.
func (h Histogram) Mean() time.Duration {
	if h.Count == 0 {
		return 0
	}
	return seconds(h.Sum / float64(h.Count))
}

// Key identifies one series of the histogram.
type Key struct {
	ConnectorID string
	Status      string
}

// Snapshot is one reading of the gateway's /metrics, with the instant it was taken.
type Snapshot struct {
	// At is when the reading was taken, stamped before the request goes out — see [Client.Scrape].
	At time.Time

	// Series is the per-(connector, status) breakdown. Empty when the gateway exposed no e2e histogram
	// at all, which is a legitimate parse and a fatal reading: see [Histogram.CheckBudget].
	Series map[Key]Histogram
}

// Keys lists the series present in the reading, sorted by connector then status.
func (s Snapshot) Keys() []Key {
	keys := make([]Key, 0, len(s.Series))
	for k := range s.Series {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ConnectorID != keys[j].ConnectorID {
			return keys[i].ConnectorID < keys[j].ConnectorID
		}
		return keys[i].Status < keys[j].Status
	})
	return keys
}

// Total aggregates every series in the reading. It is the default view: the NFR budgets a delivery
// attempt, and a rejected attempt took just as long as an accepted one.
func (s Snapshot) Total() Histogram {
	return s.Where(func(Key) bool { return true })
}

// Where aggregates the series keep selects — one connector during a run that shares a pod with others,
// or one status when a fair comparison demands it. An unmatched selector yields an empty histogram,
// which CheckBudget then refuses rather than passing.
func (s Snapshot) Where(keep func(Key) bool) Histogram {
	var picked []Histogram
	for _, k := range s.Keys() { // sorted, so the aggregate is deterministic
		if keep(k) {
			picked = append(picked, s.Series[k])
		}
	}
	return merge(picked)
}

// merge sums histograms sharing the same bucket edges. Series of one metric always do, coming from one
// HistogramOpts; a mismatch would mean two processes on different builds behind one endpoint, and the
// shorter bucket list wins rather than the sum being silently wrong at the tail.
func merge(hs []Histogram) Histogram {
	var out Histogram
	for _, h := range hs {
		out.Count += h.Count
		out.Sum += h.Sum
		if out.Buckets == nil {
			out.Buckets = append([]Bucket(nil), h.Buckets...)
			continue
		}
		if len(h.Buckets) < len(out.Buckets) {
			out.Buckets = out.Buckets[:len(h.Buckets)]
		}
		for i := range out.Buckets {
			if out.Buckets[i].UpperBound != h.Buckets[i].UpperBound {
				out.Buckets = out.Buckets[:i]
				break
			}
			out.Buckets[i].Cumulative += h.Buckets[i].Cumulative
		}
	}
	return out
}

// Sub windows a histogram: the observations that landed between two readings, bucket by bucket.
//
// A load run cares about the traffic it injected, and a pod that has been up for a day carries a
// lifetime distribution that would drown it. Every counter must move forward and the bucket edges must
// match, or the readings span a restart or a redeploy and no delta over them means anything.
func Sub(before, after Histogram) (Histogram, error) {
	if after.Count < before.Count {
		return Histogram{}, fmt.Errorf("%w: count went from %d to %d", ErrCounterReset, before.Count, after.Count)
	}
	if after.Sum < before.Sum {
		return Histogram{}, fmt.Errorf("%w: sum went from %v to %v", ErrCounterReset, before.Sum, after.Sum)
	}
	// A baseline with no buckets is the zero it looks like, not a shape change. It is the ordinary
	// first use rather than a degenerate one: a gateway that has sent nothing exposes no series at
	// all, so the reading taken before the very first run is empty. Refusing it here would fail the
	// documented sequence on its first command, blaming a redeployment that never happened.
	if len(before.Buckets) == 0 && before.Count == 0 {
		before.Buckets = make([]Bucket, len(after.Buckets))
		for i, b := range after.Buckets {
			before.Buckets[i] = Bucket{UpperBound: b.UpperBound}
		}
	}
	if len(before.Buckets) != len(after.Buckets) {
		return Histogram{}, fmt.Errorf("%w: %d buckets then %d",
			ErrBucketMismatch, len(before.Buckets), len(after.Buckets))
	}

	out := Histogram{
		Buckets: make([]Bucket, len(after.Buckets)),
		Count:   after.Count - before.Count,
		Sum:     after.Sum - before.Sum,
	}
	for i := range after.Buckets {
		if after.Buckets[i].UpperBound != before.Buckets[i].UpperBound {
			return Histogram{}, fmt.Errorf("%w: bucket %d is le=%v then le=%v",
				ErrBucketMismatch, i, before.Buckets[i].UpperBound, after.Buckets[i].UpperBound)
		}
		if after.Buckets[i].Cumulative < before.Buckets[i].Cumulative {
			return Histogram{}, fmt.Errorf("%w: bucket le=%v went from %d to %d", ErrCounterReset,
				after.Buckets[i].UpperBound, before.Buckets[i].Cumulative, after.Buckets[i].Cumulative)
		}
		out.Buckets[i] = Bucket{
			UpperBound: after.Buckets[i].UpperBound,
			Cumulative: after.Buckets[i].Cumulative - before.Buckets[i].Cumulative,
		}
	}
	return out, nil
}

// Quantile is where a quantile falls: the interval (Lower, Upper] between two bucket edges. It is
// never a single value, because the exposition does not carry one.
type Quantile struct {
	// Q is the quantile asked for, e.g. 0.99.
	Q float64

	// Lower is the exclusive lower edge of the bucket the quantile falls in — zero for the first bucket.
	Lower time.Duration

	// Upper is the inclusive upper edge. Meaningless when Unbounded: the overflow bucket has none, and
	// Upper then saturates rather than overflowing into a negative duration.
	Upper time.Duration

	// Unbounded reports that the quantile landed in the +Inf bucket, i.e. past the histogram's last
	// finite edge. Nothing bounds it from above, so no budget can be met.
	Unbounded bool

	// Observations is the histogram's total count, so a caller can weigh a verdict drawn from twelve
	// samples differently from one drawn from a million.
	Observations uint64
}

// String renders the interval the way it should be reported: as an interval.
func (q Quantile) String() string {
	if q.Unbounded {
		return fmt.Sprintf("p%g > %v (overflow bucket, n=%d)", q.Q*100, q.Lower, q.Observations)
	}
	return fmt.Sprintf("p%g ∈ (%v, %v] (n=%d)", q.Q*100, q.Lower, q.Upper, q.Observations)
}

// Verdict is the three-valued answer to a budget question. See the package doc for why it is not a
// boolean.
type Verdict string

// The three verdicts.
const (
	// Pass: the quantile's whole bucket lies at or below the budget.
	Pass Verdict = "pass"
	// Fail: the quantile's whole bucket lies at or above the budget.
	Fail Verdict = "fail"
	// Indeterminate: the budget falls strictly inside the quantile's bucket. Not an error and not a
	// pass — the exposition simply does not resolve it. Add a bucket edge at the budget, or read the
	// protobuf exposition, to make it decidable.
	Indeterminate Verdict = "indeterminate"
)

// QuantileBounds locates the q-th quantile between two bucket edges.
//
// The rank convention is Prometheus's own: the quantile is the first bucket whose cumulative count
// reaches q × Count, so it never reports a bucket the data does not reach.
func (h Histogram) QuantileBounds(q float64) (Quantile, error) {
	if !(q > 0 && q < 1) {
		return Quantile{}, fmt.Errorf("%w: got %v", ErrBadQuantile, q)
	}
	if h.Count == 0 {
		return Quantile{}, ErrNoObservations
	}
	if len(h.Buckets) == 0 {
		return Quantile{}, fmt.Errorf("%w: %d observations but no bucket", ErrNoObservations, h.Count)
	}

	target := q * float64(h.Count)
	var lower float64
	for _, b := range h.Buckets {
		if float64(b.Cumulative) >= target {
			return Quantile{
				Q: q, Lower: seconds(lower), Upper: seconds(b.UpperBound),
				Unbounded: b.Unbounded(), Observations: h.Count,
			}, nil
		}
		lower = b.UpperBound
	}
	// Unreachable on a well-formed histogram: the +Inf bucket is cumulative to Count, and target <=
	// Count. Reported rather than assumed away, because the alternative is returning the wrong bucket.
	return Quantile{}, fmt.Errorf(
		"gatewaymetrics: malformed histogram: no bucket reaches %v of %d observations", target, h.Count)
}

// CheckBudget answers whether the q-th quantile is within budget, three-valued. It returns the interval
// it decided on, so a report can print what was actually read rather than a verdict on its own.
//
// It errors — never passes — on an empty histogram, on a quantile out of range, and on a malformed one.
func (h Histogram) CheckBudget(q float64, budget time.Duration) (Verdict, Quantile, error) {
	bounds, err := h.QuantileBounds(q)
	if err != nil {
		return Indeterminate, Quantile{}, err
	}
	switch {
	case bounds.Unbounded:
		// The overflow bucket has no upper edge. It can only fail, and only Lower decides.
		if bounds.Lower >= budget {
			return Fail, bounds, nil
		}
		return Indeterminate, bounds, nil
	case bounds.Upper <= budget:
		return Pass, bounds, nil
	case bounds.Lower >= budget:
		return Fail, bounds, nil
	default:
		return Indeterminate, bounds, nil
	}
}

// Parse reads a Prometheus text exposition and stamps the resulting snapshot with at.
//
// It uses the official text parser, so label escaping, NaN, +Inf and multi-line families are handled by
// the format's own implementation. An exposition carrying no e2e histogram parses cleanly into an empty
// snapshot: other metrics are legitimately absent from a partial scrape, and it is CheckBudget that
// refuses to score nothing. Series missing either label are skipped rather than folded into an empty
// key, which would silently mix connectors.
func Parse(r io.Reader, at time.Time) (Snapshot, error) {
	// NewTextParser, not a zero-value TextParser: since prometheus/common v0.66 the parser carries its
	// own validation scheme, whose zero value is UnsetValidation and panics on the first metric name.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return Snapshot{}, fmt.Errorf("gatewaymetrics: parse exposition: %w", err)
	}

	snap := Snapshot{At: at, Series: make(map[Key]Histogram)}
	fam, ok := families[MetricE2E]
	if !ok {
		return snap, nil
	}
	for _, m := range fam.GetMetric() {
		hist := m.GetHistogram()
		if hist == nil {
			continue
		}
		key := Key{ConnectorID: labelValue(m, labelConnectorID), Status: labelValue(m, labelStatus)}
		if key.ConnectorID == "" || key.Status == "" {
			continue
		}
		snap.Series[key] = readHistogram(hist)
	}
	return snap, nil
}

// readHistogram converts one exposed histogram, normalising the overflow bucket.
//
// The +Inf bucket is rebuilt from the sample count rather than trusted from the exposition: client_golang
// omits it over protobuf (where it is redundant with the count) and the text parser's handling of an
// explicit le="+Inf" line has changed across versions. Rebuilding is right under both, and the quantile
// search depends on that last bucket being cumulative to the full count.
func readHistogram(h *dto.Histogram) Histogram {
	out := Histogram{Count: h.GetSampleCount(), Sum: h.GetSampleSum()}
	for _, b := range h.GetBucket() {
		if math.IsInf(b.GetUpperBound(), 1) {
			continue
		}
		out.Buckets = append(out.Buckets, Bucket{
			UpperBound: b.GetUpperBound(),
			Cumulative: b.GetCumulativeCount(),
		})
	}
	sort.Slice(out.Buckets, func(i, j int) bool {
		return out.Buckets[i].UpperBound < out.Buckets[j].UpperBound
	})
	out.Buckets = append(out.Buckets, Bucket{UpperBound: math.Inf(1), Cumulative: out.Count})
	return out
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// seconds converts a float count of seconds into a Duration, saturating instead of overflowing: a
// float-to-int conversion out of range is implementation-dependent in Go, and +Inf read back as a
// negative duration would be worse than one pinned at the maximum.
func seconds(v float64) time.Duration {
	ns := v * float64(time.Second)
	switch {
	case math.IsNaN(ns):
		return 0
	case ns >= float64(math.MaxInt64):
		return time.Duration(math.MaxInt64)
	case ns <= float64(math.MinInt64):
		return time.Duration(math.MinInt64)
	}
	return time.Duration(ns)
}

// Client scrapes one gateway pod's ops endpoint. It is safe for concurrent use.
//
// It used to mirror smscmetrics.Client rather than share with it, on the standing note that
// "extracting a common scraper is worth doing the day a third one appears". step-201e added the third
// — the Redpanda broker's admin exposition — so the shape those two had in common (origin or full URL,
// credentials never echoed, early timestamp, status guard) now lives in test/load/promscrape, and only
// this peer's metric contract lives here.
type Client struct{ inner *promscrape.Client }

// Option configures a Client.
type Option func(*clientOptions)

type clientOptions struct{ http *http.Client }

// WithHTTPClient replaces the HTTP client used for scraping. The scrape deadline comes from the context
// passed to Scrape, so a client set here needs no timeout of its own.
func WithHTTPClient(c *http.Client) Option {
	return func(o *clientOptions) {
		if c != nil {
			o.http = c
		}
	}
}

// NewClient builds a scraper for endpoint, which may be a bare origin ("http://127.0.0.1:9100") or the
// full metrics URL. A bare origin gets "/metrics" appended, so a recopied base URL is not a 404 that
// then gets blamed on the system under test.
//
// An endpoint may carry credentials. They are used for the request and never repeated back: no error
// returned here, nor any produced later by this client, echoes the raw endpoint.
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
		Namespace:   "gatewaymetrics",
		Endpoint:    endpoint,
		DefaultPath: "/metrics",
	}, popts...)
	if err != nil {
		return nil, err
	}
	return &Client{inner: inner}, nil
}

// URL reports the metrics URL the client scrapes, after any "/metrics" was appended, with any password
// masked. It is a diagnostic accessor — callers log it — so it returns the redacted form rather than
// url.URL.String, which does not mask userinfo.
func (c *Client) URL() string { return c.inner.URL() }

// Scrape takes one reading, stamping it with the instant just before the request goes out.
//
// The stamp is early on purpose. The pod gathers its counters somewhere between the request leaving and
// the body being read, so an early stamp can only put At before the counters it labels. Unlike a rate,
// a latency verdict does not divide by the window — but Sub does use the pair to say which
// observations belong to the run, and a stamp that lands after the gather would let observations from
// before the run leak into it.
//
// The context governs the whole exchange, connection included.
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
