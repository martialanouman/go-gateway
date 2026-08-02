// Package smscmetrics reads the SMSC simulator's Prometheus endpoint and turns two readings
// into an absorbed submit_sm throughput (step-201, peer ceiling).
//
// The whole point is where the counter lives. smsc_submit_sm_received_total is incremented by
// the *peer*, so a rate derived from it says what the simulator actually took in — not what an
// injector believes it sent. An injector that queues, retries or lies about its own send rate
// cannot inflate this number, which is exactly the property needed to call a tier a ceiling.
//
// One distinction the peer's own HELP strings make, and this package preserves: received_total
// counts submit_sm PDUs *accepted*, while outcome_total counts them *served*. Accepted is not
// throughput. A peer that reads PDUs into a queue and silently drops the overflow — without
// emitting a non-success outcome — inflates the former while the latter stalls. Callers that
// need a sustained rate must compare Throughput.Served against Throughput.Submitted; Qualified()
// alone does not catch that case, because nothing was ever reported as shed.
//
// Usage is two scrapes and a subtraction:
//
//	c, _ := smscmetrics.NewClient("http://127.0.0.1:9000")
//	before, _ := c.Scrape(ctx)
//	// ... hold the load steady ...
//	after, _ := c.Scrape(ctx)
//	tp, err := smscmetrics.Rate(before, after)
//
// A tier only counts when tp.Qualified() is true: any non-success outcome appearing during the
// window means the peer was shedding, so the rate measured is not a rate it sustained.
//
// The simulator's metric contract is mirrored from go-smsc-simulator/internal/metrics/metrics.go.
// Only smsc_active_scenario is deliberately ignored: it is an info gauge, not a measurement.
package smscmetrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// MinWindow is the shortest interval Rate accepts between two readings. Below it the elapsed
// time is dominated by scrape jitter and the division amplifies it into a meaningless figure,
// so Rate refuses rather than returning a number nobody should trust.
const MinWindow = 100 * time.Millisecond

// Errors returned by Rate. Both mean "this tier cannot be scored", never "the rate was zero".
var (
	// ErrCounterReset reports a counter that went backwards, a series that disappeared
	// between the two readings, or a reading that is not a finite number — the simulator
	// restarted, or its registry was reset. The window spans a discontinuity, so no delta
	// over it is meaningful.
	ErrCounterReset = errors.New("smscmetrics: counter reset between readings")

	// ErrWindowTooShort reports readings closer together than MinWindow (or out of order).
	ErrWindowTooShort = errors.New("smscmetrics: window shorter than MinWindow")
)

// Metric and label names exposed by the simulator.
const (
	metricSubmitReceived = "smsc_submit_sm_received_total"
	metricSubmitOutcome  = "smsc_submit_sm_outcome_total"
	metricActiveBinds    = "smsc_active_binds"
	metricServedLatency  = "smsc_served_latency_seconds"

	labelVirtualSMSC = "virtual_smsc"
	labelBindType    = "bind_type"
	labelOutcome     = "outcome"

	// outcomeSuccess is the only outcome that leaves a tier qualified. Everything else is
	// treated as shedding, by exclusion rather than by an enumerated list, so an outcome
	// label the simulator grows later still disqualifies instead of being silently ignored.
	outcomeSuccess = "success"
)

// SMSC holds one virtual SMSC's counters at a point in time. All values are cumulative, as
// read from the endpoint; only differences between two readings carry meaning.
type SMSC struct {
	// SubmitReceived is smsc_submit_sm_received_total.
	SubmitReceived float64

	// Outcomes is smsc_submit_sm_outcome_total, keyed by the outcome label.
	Outcomes map[string]float64

	// ActiveBinds is the smsc_active_binds gauge, keyed by bind_type. A gauge, so its value
	// is meaningful on its own: it says how many binds are really up, which is how a sweep
	// distinguishes "40 binds pushing" from "40 requested, 12 refused".
	ActiveBinds map[string]float64

	// LatencySum and LatencyCount are the smsc_served_latency_seconds histogram's _sum and
	// _count, summed over the scenario label. Buckets are not kept, because there is no
	// distribution to look at: the simulator observes the latency its scenario *decided*
	// (ObserveServedLatency receives decision.LatencyMS), not a duration it measured. The
	// sum/count mean therefore reports the configured value and does NOT detect saturation
	// — it read 5 ms flat from 10 to 320 binds while throughput varied by 25x. Saturation
	// is visible in smsc_submit_sm_outcome_total and in the throughput curve bending.
	//
	// LatencyCount is still worth keeping: it says how many submit_sm the peer actually
	// served, which is a count, not a timing.
	LatencySum   float64
	LatencyCount float64
}

// Snapshot is one reading of the simulator's /metrics, with the instant it was taken. That
// timestamp is the only clock Rate uses: elapsed time comes from the readings themselves,
// never from an interval the caller thinks it slept.
type Snapshot struct {
	// At is when the reading was taken. Scrape sets it before the request goes out, i.e. at
	// or before the peer's own gather, so a window computed from two of them is never
	// shorter than the real one. See Scrape for why the bias points that way.
	At time.Time

	// SMSCs is the per-virtual-SMSC breakdown, keyed by the virtual_smsc label.
	SMSCs map[string]SMSC
}

// Names lists the virtual SMSCs present in the reading, sorted.
func (s Snapshot) Names() []string {
	names := make([]string, 0, len(s.SMSCs))
	for name := range s.SMSCs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// SubmitReceived totals smsc_submit_sm_received_total over every virtual SMSC in the reading.
func (s Snapshot) SubmitReceived() float64 {
	var total float64
	for _, smsc := range s.SMSCs {
		total += smsc.SubmitReceived
	}
	return total
}

// ActiveBinds totals the smsc_active_binds gauge over every virtual SMSC and bind type.
func (s Snapshot) ActiveBinds() float64 {
	var total float64
	for _, smsc := range s.SMSCs {
		for _, n := range smsc.ActiveBinds {
			total += n
		}
	}
	return total
}

// Outcomes totals smsc_submit_sm_outcome_total per outcome over every virtual SMSC.
func (s Snapshot) Outcomes() map[string]float64 {
	totals := make(map[string]float64)
	for _, smsc := range s.SMSCs {
		for outcome, n := range smsc.Outcomes {
			totals[outcome] += n
		}
	}
	return totals
}

// Select narrows the reading to a single virtual SMSC, keeping At. It returns a snapshot with
// no SMSCs when the name is absent, and never mutates or aliases the receiver.
//
// Aggregating every virtual SMSC is the default because the ceiling being measured is the
// peer's, taken whole. Select exists because a sweep that points its binds at one virtual SMSC
// among several configured would otherwise fold a neighbour's traffic into its own rate.
func (s Snapshot) Select(virtualSMSC string) Snapshot {
	out := Snapshot{At: s.At, SMSCs: make(map[string]SMSC, 1)}
	if smsc, ok := s.SMSCs[virtualSMSC]; ok {
		out.SMSCs[virtualSMSC] = smsc.clone()
	}
	return out
}

func (s SMSC) clone() SMSC {
	out := s
	out.Outcomes = copyFloats(s.Outcomes)
	out.ActiveBinds = copyFloats(s.ActiveBinds)
	return out
}

func copyFloats(src map[string]float64) map[string]float64 {
	if src == nil {
		return nil
	}
	dst := make(map[string]float64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// Throughput is what the peer absorbed between two readings, aggregated over the virtual SMSCs
// they contain. A zero-value Throughput is never returned alongside a nil error.
type Throughput struct {
	// Window is after.At minus before.At — the elapsed time the rate is divided by.
	Window time.Duration

	// Submitted is the number of submit_sm the peer *accepted* during the window — read off
	// its receive counter, not its outcome counter. Accepted is not served: compare against
	// Served before treating this as a sustained rate. See the package doc.
	Submitted float64

	// SubmitPerSecond is Submitted divided by Window: the absorbed rate.
	SubmitPerSecond float64

	// Outcomes is the per-outcome delta over the window. An outcome whose counter did not
	// move reads 0 here rather than its cumulative total.
	Outcomes map[string]float64

	// NonSuccess is the total of every Outcomes entry other than "success".
	NonSuccess float64

	// Served is the number of submit_sm the latency histogram observed during the window —
	// what the peer actually served, as opposed to what it accepted (Submitted).
	//
	// A gap between the two is the one signal of silent loss: a peer whose intake queue
	// overflows keeps accepting while it stops serving, and reports no non-success outcome
	// for what it drops, so Qualified() stays true. Callers scoring a tier should refuse it
	// when Served falls materially below Submitted.
	Served float64

	// MeanServedLatency is the mean service time over the window (latency sum delta over
	// Served), zero when Served is zero.
	//
	// It does NOT detect saturation, and must not be read as if it did: the simulator
	// observes the latency its scenario *decided*, not a duration it measured
	// (ObserveServedLatency receives decision.LatencyMS). It reads the configured value
	// unchanged however hard the peer is pushed — measured flat at 5 ms from 10 to 320
	// binds. Saturation shows up in Outcomes/NonSuccess and in the throughput curve
	// bending, nowhere else.
	MeanServedLatency time.Duration

	// ActiveBinds is the bind gauge total from the later reading. It is a point sample taken
	// at the end of the window, not an average over it: a bind that dropped and came back
	// mid-window leaves no trace here.
	ActiveBinds float64
}

// Qualified reports whether the tier counts: true when every submit_sm outcome served during
// the window was a success. A caller decides on this boolean, never by re-reading label strings.
func (t Throughput) Qualified() bool { return t.NonSuccess == 0 }

// Rate derives the absorbed submit_sm throughput between two readings.
//
// It returns ErrWindowTooShort when the readings are closer than MinWindow or out of order, and
// ErrCounterReset when any counter went backwards, read NaN or ±Inf, or a virtual SMSC vanished
// — a negative delta is a discontinuity, not a zero. A virtual SMSC that appears only in the
// later reading is counted from zero: its series was created mid-window, so its full value did
// accrue in it.
//
// Outcomes other than success do not fail the call. They land in NonSuccess and flip Qualified,
// because "the peer shed traffic at this tier" is a result worth reading, not an error.
func Rate(before, after Snapshot) (Throughput, error) {
	window := after.At.Sub(before.At)
	if window < MinWindow {
		return Throughput{}, fmt.Errorf("%w: got %v, need at least %v", ErrWindowTooShort, window, MinWindow)
	}

	for name := range before.SMSCs {
		if _, ok := after.SMSCs[name]; !ok {
			return Throughput{}, fmt.Errorf("%w: virtual SMSC %q is absent from the later reading", ErrCounterReset, name)
		}
	}

	tp := Throughput{Window: window, Outcomes: make(map[string]float64), ActiveBinds: after.ActiveBinds()}
	var latencySum float64

	for name, a := range after.SMSCs {
		b := before.SMSCs[name] // zero value for a virtual SMSC that appeared during the window

		d, err := delta(a.SubmitReceived, b.SubmitReceived, name, metricSubmitReceived)
		if err != nil {
			return Throughput{}, err
		}
		tp.Submitted += d

		for outcome := range b.Outcomes {
			if _, ok := a.Outcomes[outcome]; !ok {
				return Throughput{}, fmt.Errorf("%w: %s{%s=%q,%s=%q} is absent from the later reading",
					ErrCounterReset, metricSubmitOutcome, labelVirtualSMSC, name, labelOutcome, outcome)
			}
		}
		for outcome, v := range a.Outcomes {
			d, err := delta(v, b.Outcomes[outcome], name, metricSubmitOutcome+"{"+labelOutcome+"="+outcome+"}")
			if err != nil {
				return Throughput{}, err
			}
			tp.Outcomes[outcome] += d
			if outcome != outcomeSuccess {
				tp.NonSuccess += d
			}
		}

		d, err = delta(a.LatencyCount, b.LatencyCount, name, metricServedLatency+"_count")
		if err != nil {
			return Throughput{}, err
		}
		tp.Served += d

		d, err = delta(a.LatencySum, b.LatencySum, name, metricServedLatency+"_sum")
		if err != nil {
			return Throughput{}, err
		}
		latencySum += d
	}

	tp.SubmitPerSecond = tp.Submitted / window.Seconds()
	if tp.Served > 0 {
		tp.MeanServedLatency = meanDuration(latencySum, tp.Served)
	}
	return tp, nil
}

// delta subtracts two cumulative readings, rejecting anything that is not a finite forward move.
//
// Non-finite is checked first and on its own: NaN and ±Inf are both legal in the Prometheus text
// format and expfmt decodes them unchanged, so they do reach here. A NaN compares false to
// everything, but a +Inf satisfies d >= 0 and would sail through into SubmitPerSecond, printing
// an infinite ceiling out of a tool that exits 0.
func delta(after, before float64, virtualSMSC, metric string) (float64, error) {
	d := after - before
	switch {
	case math.IsNaN(d) || math.IsInf(d, 0):
		return 0, fmt.Errorf("%w: %s{%s=%q} read %v then %v, which is not a finite counter",
			ErrCounterReset, metric, labelVirtualSMSC, virtualSMSC, before, after)
	case d < 0:
		return 0, fmt.Errorf("%w: %s{%s=%q} went from %v to %v",
			ErrCounterReset, metric, labelVirtualSMSC, virtualSMSC, before, after)
	}
	return d, nil
}

// meanDuration converts a mean expressed in seconds into a Duration, saturating instead of
// overflowing: a float-to-int conversion out of the target type's range is
// implementation-dependent in Go, and a nonsense duration read as a negative one is worse than
// one pinned at the maximum. Only reachable on absurd input, both arguments being finite here.
func meanDuration(sumSeconds, count float64) time.Duration {
	ns := float64(time.Second) * sumSeconds / count
	if ns >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(ns)
}

// Parse reads a Prometheus text exposition and stamps the resulting snapshot with at.
//
// It uses the official text parser, so label escaping, NaN, +Inf and multi-line families are
// handled by the format's own implementation rather than by an ad-hoc scanner. Sample order is
// irrelevant and # HELP / # TYPE lines are optional: without a TYPE the histogram arrives as
// untyped _sum and _count series, which are read as such. Series carrying no virtual_smsc
// label, and every family the simulator exposes beyond the four measured here, are ignored.
func Parse(r io.Reader, at time.Time) (Snapshot, error) {
	// NewTextParser, not a zero-value TextParser: since prometheus/common v0.66 the parser
	// carries its own validation scheme, whose zero value is UnsetValidation and *panics* on
	// the first metric name. UTF8Validation matches the library's own default and accepts
	// everything the legacy scheme does.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return Snapshot{}, fmt.Errorf("smscmetrics: parse exposition: %w", err)
	}

	acc := make(map[string]*SMSC)
	entry := func(name string) *SMSC {
		if e, ok := acc[name]; ok {
			return e
		}
		e := &SMSC{Outcomes: make(map[string]float64), ActiveBinds: make(map[string]float64)}
		acc[name] = e
		return e
	}

	for family, fam := range families {
		for _, m := range fam.GetMetric() {
			name := labelValue(m, labelVirtualSMSC)
			if name == "" {
				continue
			}
			switch family {
			case metricSubmitReceived:
				entry(name).SubmitReceived += sampleValue(m)
			case metricSubmitOutcome:
				entry(name).Outcomes[labelValue(m, labelOutcome)] += sampleValue(m)
			case metricActiveBinds:
				entry(name).ActiveBinds[labelValue(m, labelBindType)] += sampleValue(m)
			case metricServedLatency:
				if h := m.GetHistogram(); h != nil {
					e := entry(name)
					e.LatencySum += h.GetSampleSum()
					e.LatencyCount += float64(h.GetSampleCount())
				}
			case metricServedLatency + "_sum":
				entry(name).LatencySum += sampleValue(m)
			case metricServedLatency + "_count":
				entry(name).LatencyCount += sampleValue(m)
			}
		}
	}

	snap := Snapshot{At: at, SMSCs: make(map[string]SMSC, len(acc))}
	for name, e := range acc {
		snap.SMSCs[name] = *e
	}
	return snap, nil
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// sampleValue reads a scalar sample whatever its declared type, so an exposition served without
// # TYPE lines (everything untyped) parses identically to one with them.
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

// Client scrapes one simulator's metrics endpoint. It is safe for concurrent use.
type Client struct {
	// url is the URL actually requested, userinfo included. It never leaves the client:
	// everything logged or wrapped in an error uses redacted instead.
	url      string
	redacted string
	http     *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the HTTP client used for scraping. The scrape deadline comes from the
// context passed to Scrape, so a client set here needs no timeout of its own.
func WithHTTPClient(c *http.Client) Option {
	return func(cl *Client) {
		if c != nil {
			cl.http = c
		}
	}
}

// NewClient builds a scraper for endpoint, which may be the simulator's control-plane origin
// ("http://127.0.0.1:9000") or the full metrics URL. A bare origin gets "/metrics" appended:
// the harness README already records how easily a recopied base URL turns into a 404 that then
// gets blamed on the system under test.
//
// An endpoint may carry credentials ("https://scrape:secret@sim.internal:9000"). They are used
// for the request and never repeated back: no error returned here, nor any produced later by
// this client, echoes the raw endpoint.
func NewClient(endpoint string, opts ...Option) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		// url.Error prints the raw URL it failed on, credentials included, so only the
		// reason is reported. The endpoint itself cannot be shown: it is precisely the
		// string that could not be parsed, hence could not be redacted either.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		return nil, fmt.Errorf("smscmetrics: parse endpoint: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("smscmetrics: endpoint %q needs an http or https scheme", u.Redacted())
	}
	if u.Host == "" {
		return nil, fmt.Errorf("smscmetrics: endpoint %q has no host", u.Redacted())
	}
	if p := strings.TrimSuffix(u.Path, "/"); p == "" {
		u.Path = "/metrics"
	}

	c := &Client{url: u.String(), redacted: u.Redacted(), http: &http.Client{}}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// URL reports the metrics URL the client scrapes, after any "/metrics" was appended, with any
// password masked. It is a diagnostic accessor — callers log it — so it returns the redacted
// form rather than url.URL.String, which does not mask userinfo.
func (c *Client) URL() string { return c.redacted }

// Scrape takes one reading, stamping it with the instant just before the request goes out —
// not the instant the body finished arriving.
//
// The stamp is deliberately early, and the asymmetry matters. The peer's counters are gathered
// somewhere between the request leaving and the body being read, so an early stamp can only put
// At before the counters it labels. Applied to both readings of a pair, that makes the measured
// window no shorter than the real one, so the derived rate is understated rather than
// overstated. Stamping late does the opposite whenever the first scrape is the slower of the two
// — the usual case, with a TCP connection to open and a cold registry to encode against a
// keep-alive second scrape — and this number is a ceiling a reference run has to stay under.
//
// The context governs the whole exchange, connection included.
func (c *Client) Scrape(ctx context.Context) (Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return Snapshot{}, fmt.Errorf("smscmetrics: build request: %w", err)
	}
	at := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return Snapshot{}, fmt.Errorf("smscmetrics: scrape %s: %w", c.redacted, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return Snapshot{}, fmt.Errorf("smscmetrics: scrape %s: status %d, want 200", c.redacted, resp.StatusCode)
	}
	snap, err := Parse(resp.Body, at)
	if err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}
