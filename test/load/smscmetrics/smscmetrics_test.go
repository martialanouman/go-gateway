package smscmetrics_test

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/smscmetrics"
)

// fixture is a verbatim promhttp rendering of the simulator's registry (collectors copied
// from go-smsc-simulator/internal/metrics/metrics.go), so the parser is exercised against
// the real wire format — buckets, +Inf, float noise on _sum and all.
func fixture(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/metrics.txt")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return string(b)
}

func closeTo(got, want float64) bool {
	return math.Abs(got-want) < 1e-6
}

func TestParse_ReadsEveryFamilyPerVirtualSMSC(t *testing.T) {
	t.Parallel()

	at := time.Unix(1_700_000_000, 0).UTC()
	snap, err := smscmetrics.Parse(strings.NewReader(fixture(t)), at)
	if err != nil {
		t.Fatalf("Parse() = %v, want no error", err)
	}

	if !snap.At.Equal(at) {
		t.Errorf("At = %v, want %v", snap.At, at)
	}
	if got, want := snap.Names(), []string{"carrier-a", "carrier-b"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Names() = %v, want %v", got, want)
	}

	a, ok := snap.SMSCs["carrier-a"]
	if !ok {
		t.Fatalf("SMSCs[carrier-a] missing, want present")
	}
	if !closeTo(a.SubmitReceived, 120000) {
		t.Errorf("carrier-a SubmitReceived = %v, want %v", a.SubmitReceived, 120000.0)
	}
	if !closeTo(a.Outcomes["success"], 119880) {
		t.Errorf("carrier-a Outcomes[success] = %v, want %v", a.Outcomes["success"], 119880.0)
	}
	if !closeTo(a.Outcomes["error"], 120) {
		t.Errorf("carrier-a Outcomes[error] = %v, want %v", a.Outcomes["error"], 120.0)
	}
	if !closeTo(a.ActiveBinds["transceiver"], 38) {
		t.Errorf("carrier-a ActiveBinds[transceiver] = %v, want %v", a.ActiveBinds["transceiver"], 38.0)
	}
	if !closeTo(a.ActiveBinds["transmitter"], 2) {
		t.Errorf("carrier-a ActiveBinds[transmitter] = %v, want %v", a.ActiveBinds["transmitter"], 2.0)
	}
	// Latency is summed across scenarios: 3 observations at 0.012 (healthy) + 1 at 2.5 (slow-carrier).
	if !closeTo(a.LatencyCount, 4) {
		t.Errorf("carrier-a LatencyCount = %v, want %v", a.LatencyCount, 4.0)
	}
	if !closeTo(a.LatencySum, 2.536) {
		t.Errorf("carrier-a LatencySum = %v, want %v", a.LatencySum, 2.536)
	}

	// Totals aggregate every virtual SMSC.
	if !closeTo(snap.SubmitReceived(), 120500) {
		t.Errorf("SubmitReceived() = %v, want %v", snap.SubmitReceived(), 120500.0)
	}
	if !closeTo(snap.ActiveBinds(), 44) {
		t.Errorf("ActiveBinds() = %v, want %v", snap.ActiveBinds(), 44.0)
	}
	if !closeTo(snap.Outcomes()["success"], 120380) {
		t.Errorf("Outcomes()[success] = %v, want %v", snap.Outcomes()["success"], 120380.0)
	}
}

func TestParse_NoDependenceOnLineOrderOrHelpAndTypeLines(t *testing.T) {
	t.Parallel()

	// No # HELP, no # TYPE, families interleaved, samples out of order: the exposition
	// format guarantees none of that, so the parser must not lean on it.
	body := `smsc_submit_sm_outcome_total{virtual_smsc="carrier-b",outcome="timeout"} 7
smsc_active_binds{virtual_smsc="carrier-a",bind_type="transceiver"} 12
smsc_submit_sm_received_total{virtual_smsc="carrier-b"} 900
smsc_submit_sm_received_total{virtual_smsc="carrier-a"} 100
`
	snap, err := smscmetrics.Parse(strings.NewReader(body), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("Parse() = %v, want no error", err)
	}
	if !closeTo(snap.SubmitReceived(), 1000) {
		t.Errorf("SubmitReceived() = %v, want %v", snap.SubmitReceived(), 1000.0)
	}
	if !closeTo(snap.Outcomes()["timeout"], 7) {
		t.Errorf("Outcomes()[timeout] = %v, want %v", snap.Outcomes()["timeout"], 7.0)
	}
	if !closeTo(snap.ActiveBinds(), 12) {
		t.Errorf("ActiveBinds() = %v, want %v", snap.ActiveBinds(), 12.0)
	}
}

func TestSnapshot_SelectNarrowsToOneVirtualSMSC(t *testing.T) {
	t.Parallel()

	snap, err := smscmetrics.Parse(strings.NewReader(fixture(t)), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("Parse() = %v, want no error", err)
	}

	only := snap.Select("carrier-b")
	if got, want := len(only.SMSCs), 1; got != want {
		t.Fatalf("len(Select(carrier-b).SMSCs) = %d, want %d", got, want)
	}
	if !closeTo(only.SubmitReceived(), 500) {
		t.Errorf("Select(carrier-b).SubmitReceived() = %v, want %v", only.SubmitReceived(), 500.0)
	}
	if !only.At.Equal(snap.At) {
		t.Errorf("Select kept At = %v, want %v", only.At, snap.At)
	}
	if got := len(snap.SMSCs); got != 2 {
		t.Errorf("Select mutated the source: len(SMSCs) = %d, want 2", got)
	}

	// The narrowed snapshot must own its maps: writing through it must not reach the source,
	// or a caller holding both a whole reading and a per-SMSC view corrupts one via the other.
	sel := only.SMSCs["carrier-b"]
	sel.Outcomes["success"] = 999
	sel.ActiveBinds["transceiver"] = 999
	if got := snap.SMSCs["carrier-b"].Outcomes["success"]; !closeTo(got, 500) {
		t.Errorf("source Outcomes[success] = %v, want 500 (Select aliased the source map)", got)
	}
	if got := snap.SMSCs["carrier-b"].ActiveBinds["transceiver"]; !closeTo(got, 4) {
		t.Errorf("source ActiveBinds[transceiver] = %v, want 4 (Select aliased the source map)", got)
	}

	if got := len(snap.Select("nope").SMSCs); got != 0 {
		t.Errorf("len(Select(nope).SMSCs) = %d, want 0", got)
	}
}

func TestRate_SubmitPerSecondFromTheSnapshotTimestamps(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)
	before := snapshotOf(t0, 1000, map[string]float64{"success": 1000}, 40, 0.5, 1000)
	after := snapshotOf(t0.Add(2*time.Second), 17000, map[string]float64{"success": 17000}, 40, 8.5, 17000)

	tp, err := smscmetrics.Rate(before, after)
	if err != nil {
		t.Fatalf("Rate() = %v, want no error", err)
	}
	if !closeTo(tp.Submitted, 16000) {
		t.Errorf("Submitted = %v, want %v", tp.Submitted, 16000.0)
	}
	if !closeTo(tp.SubmitPerSecond, 8000) {
		t.Errorf("SubmitPerSecond = %v, want %v", tp.SubmitPerSecond, 8000.0)
	}
	if tp.Window != 2*time.Second {
		t.Errorf("Window = %v, want %v", tp.Window, 2*time.Second)
	}
	if !tp.Qualified() {
		t.Errorf("Qualified() = false, want true (no non-success outcome in the window)")
	}
	if !closeTo(tp.ActiveBinds, 40) {
		t.Errorf("ActiveBinds = %v, want %v", tp.ActiveBinds, 40.0)
	}
	// 8.0s of served latency spread over 16000 served submits = 500µs mean.
	if want := 500 * time.Microsecond; tp.MeanServedLatency != want {
		t.Errorf("MeanServedLatency = %v, want %v", tp.MeanServedLatency, want)
	}
}

func TestRate_DisqualifiesTheTierOnNonSuccessOutcomes(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)
	before := snapshotOf(t0, 1000, map[string]float64{"success": 990, "error": 10}, 40, 0.5, 1000)
	after := snapshotOf(t0.Add(time.Second), 3000, map[string]float64{"success": 2955, "error": 10, "timeout": 35}, 40, 1.5, 3000)

	tp, err := smscmetrics.Rate(before, after)
	if err != nil {
		t.Fatalf("Rate() = %v, want no error", err)
	}
	if tp.Qualified() {
		t.Errorf("Qualified() = true, want false (35 timeouts appeared in the window)")
	}
	if !closeTo(tp.NonSuccess, 35) {
		t.Errorf("NonSuccess = %v, want %v", tp.NonSuccess, 35.0)
	}
	if !closeTo(tp.Outcomes["timeout"], 35) {
		t.Errorf("Outcomes[timeout] = %v, want %v", tp.Outcomes["timeout"], 35.0)
	}
	// An error count that did not move during the window must not disqualify the tier.
	if got, ok := tp.Outcomes["error"]; ok && !closeTo(got, 0) {
		t.Errorf("Outcomes[error] = %v, want 0 (unchanged across the window)", got)
	}
}

func TestRate_CounterResetIsInvalidNotZero(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)
	before := snapshotOf(t0, 120000, map[string]float64{"success": 120000}, 40, 60, 120000)
	// The simulator restarted mid-window: its counters are back near zero.
	after := snapshotOf(t0.Add(2*time.Second), 800, map[string]float64{"success": 800}, 40, 0.4, 800)

	_, err := smscmetrics.Rate(before, after)
	if !errors.Is(err, smscmetrics.ErrCounterReset) {
		t.Fatalf("Rate() = %v, want ErrCounterReset", err)
	}
}

func TestRate_VanishedSeriesIsACounterReset(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)
	before := smscmetrics.Snapshot{At: t0, SMSCs: map[string]smscmetrics.SMSC{
		"carrier-a": {SubmitReceived: 100, Outcomes: map[string]float64{"success": 100}},
		"carrier-b": {SubmitReceived: 50, Outcomes: map[string]float64{"success": 50}},
	}}
	after := smscmetrics.Snapshot{At: t0.Add(time.Second), SMSCs: map[string]smscmetrics.SMSC{
		"carrier-a": {SubmitReceived: 300, Outcomes: map[string]float64{"success": 300}},
	}}

	if _, err := smscmetrics.Rate(before, after); !errors.Is(err, smscmetrics.ErrCounterReset) {
		t.Fatalf("Rate() with a vanished virtual SMSC = %v, want ErrCounterReset", err)
	}
}

func TestRate_NewSeriesCountsFromZero(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)
	before := smscmetrics.Snapshot{At: t0, SMSCs: map[string]smscmetrics.SMSC{
		"carrier-a": {SubmitReceived: 100, Outcomes: map[string]float64{"success": 100}},
	}}
	// carrier-b took its first submit_sm during the window: the series was created at 0.
	after := smscmetrics.Snapshot{At: t0.Add(time.Second), SMSCs: map[string]smscmetrics.SMSC{
		"carrier-a": {SubmitReceived: 300, Outcomes: map[string]float64{"success": 300}},
		"carrier-b": {SubmitReceived: 40, Outcomes: map[string]float64{"success": 40}},
	}}

	tp, err := smscmetrics.Rate(before, after)
	if err != nil {
		t.Fatalf("Rate() = %v, want no error", err)
	}
	if !closeTo(tp.SubmitPerSecond, 240) {
		t.Errorf("SubmitPerSecond = %v, want %v", tp.SubmitPerSecond, 240.0)
	}
}

func TestRate_RefusesAWindowShorterThanMinWindow(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)
	cases := map[string]time.Duration{
		"zero":     0,
		"backward": -time.Second,
		"sub-min":  smscmetrics.MinWindow - time.Millisecond,
	}
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			before := snapshotOf(t0, 1000, map[string]float64{"success": 1000}, 40, 0.5, 1000)
			after := snapshotOf(t0.Add(d), 2000, map[string]float64{"success": 2000}, 40, 1, 2000)
			if _, err := smscmetrics.Rate(before, after); !errors.Is(err, smscmetrics.ErrWindowTooShort) {
				t.Fatalf("Rate() over %v = %v, want ErrWindowTooShort", d, err)
			}
		})
	}
}

func TestRate_MeanServedLatencyIsZeroWhenNothingWasServed(t *testing.T) {
	t.Parallel()

	t0 := time.Unix(1_700_000_000, 0)
	before := snapshotOf(t0, 1000, map[string]float64{"success": 1000}, 40, 0.5, 1000)
	after := snapshotOf(t0.Add(time.Second), 1000, map[string]float64{"success": 1000}, 40, 0.5, 1000)

	tp, err := smscmetrics.Rate(before, after)
	if err != nil {
		t.Fatalf("Rate() = %v, want no error", err)
	}
	if tp.Served != 0 {
		t.Errorf("Served = %v, want 0", tp.Served)
	}
	if tp.MeanServedLatency != 0 {
		t.Errorf("MeanServedLatency = %v, want 0", tp.MeanServedLatency)
	}
	if !tp.Qualified() {
		t.Errorf("Qualified() = false, want true (an idle window is not a failed one)")
	}
}

func TestClient_ScrapeAppendsMetricsToABareOrigin(t *testing.T) {
	t.Parallel()

	body := fixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	for name, endpoint := range map[string]string{
		"bare origin": srv.URL,
		"trailing /":  srv.URL + "/",
		"explicit":    srv.URL + "/metrics",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, err := smscmetrics.NewClient(endpoint)
			if err != nil {
				t.Fatalf("NewClient(%q) = %v, want no error", endpoint, err)
			}
			before := time.Now()
			snap, err := c.Scrape(context.Background())
			if err != nil {
				t.Fatalf("Scrape() = %v, want no error", err)
			}
			if snap.At.Before(before) {
				t.Errorf("At = %v, want at or after %v (the reading stamps itself)", snap.At, before)
			}
			if !closeTo(snap.SubmitReceived(), 120500) {
				t.Errorf("SubmitReceived() = %v, want %v", snap.SubmitReceived(), 120500.0)
			}
		})
	}
}

func TestClient_ScrapeReportsANonOKStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	c, err := smscmetrics.NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient() = %v, want no error", err)
	}
	if _, err := c.Scrape(context.Background()); err == nil {
		t.Fatalf("Scrape() against a 503 = nil, want an error")
	}
}

func TestClient_ScrapeHonoursContextCancellation(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = w.Write([]byte("smsc_submit_sm_received_total{virtual_smsc=\"a\"} 1\n"))
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})

	c, err := smscmetrics.NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient() = %v, want no error", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := c.Scrape(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Scrape() with an expired deadline = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Scrape() returned after %v, want it to give up on the deadline", elapsed)
	}
}

func TestNewClient_RejectsAnUnusableEndpoint(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{"", "://nope", "127.0.0.1:9000"} {
		if _, err := smscmetrics.NewClient(endpoint); err == nil {
			t.Errorf("NewClient(%q) = nil error, want an error", endpoint)
		}
	}
}

func TestClient_ScrapeStampsTheReadingBeforeTheRequest(t *testing.T) {
	t.Parallel()

	// The peer gathers its registry when the handler runs, so the counters in the body are
	// no older than the instant the handler was entered. Stamping the reading after the body
	// is read puts At *later* than that instant; when the first scrape of a pair is the slow
	// one (TCP connect, cold registry) the measured window is then shorter than the truth and
	// the derived rate is overstated. The stamp must never be later than the peer's Gather.
	body := fixture(t)
	var (
		mu     sync.Mutex
		served time.Time
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		served = time.Now()
		mu.Unlock()
		time.Sleep(200 * time.Millisecond) // a slow Gather, or a slow body
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	c, err := smscmetrics.NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient() = %v, want no error", err)
	}
	snap, err := c.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape() = %v, want no error", err)
	}

	mu.Lock()
	gather := served
	mu.Unlock()
	if snap.At.After(gather) {
		t.Errorf("At = %v, want at or before %v (the instant the peer gathered); stamping after the body read shortens the window and overstates the rate",
			snap.At, gather)
	}
}

func TestClient_RedactsCredentialsInURLAndErrors(t *testing.T) {
	t.Parallel()

	const secret = "S3cr3tScrapePassword"

	withCredentials := func(rawURL string) string {
		u, err := url.Parse(rawURL)
		if err != nil {
			t.Fatalf("parse %q: %v", rawURL, err)
		}
		u.User = url.UserPassword("scrape", secret)
		return u.String()
	}

	t.Run("URL accessor", func(t *testing.T) {
		t.Parallel()

		c, err := smscmetrics.NewClient(withCredentials("http://sim.internal:9000"))
		if err != nil {
			t.Fatalf("NewClient() = %v, want no error", err)
		}
		if got := c.URL(); strings.Contains(got, secret) {
			t.Errorf("URL() = %q, want the password masked", got)
		}
		if got := c.URL(); !strings.Contains(got, "sim.internal:9000/metrics") {
			t.Errorf("URL() = %q, want it to still name the endpoint scraped", got)
		}
	})

	t.Run("non-OK status", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		t.Cleanup(srv.Close)

		c, err := smscmetrics.NewClient(withCredentials(srv.URL))
		if err != nil {
			t.Fatalf("NewClient() = %v, want no error", err)
		}
		_, err = c.Scrape(context.Background())
		if err == nil {
			t.Fatalf("Scrape() against a 401 = nil, want an error")
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("Scrape() error = %q, want the password masked", err)
		}
	})

	t.Run("transport failure", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		endpoint := withCredentials(srv.URL)
		srv.Close() // nothing is listening any more: c.http.Do fails

		c, err := smscmetrics.NewClient(endpoint)
		if err != nil {
			t.Fatalf("NewClient() = %v, want no error", err)
		}
		_, err = c.Scrape(context.Background())
		if err == nil {
			t.Fatalf("Scrape() against a closed listener = nil, want an error")
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("Scrape() error = %q, want the password masked", err)
		}
	})

	t.Run("rejected endpoint", func(t *testing.T) {
		t.Parallel()

		for name, endpoint := range map[string]string{
			"bad scheme": "ftp://scrape:" + secret + "@sim.internal:9000/metrics",
			"no host":    "http://scrape:" + secret + "@/metrics",
			"unparsable": "http://scrape:" + secret + "@sim.internal:9000/met rics\x7f",
		} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				_, err := smscmetrics.NewClient(endpoint)
				if err == nil {
					t.Fatalf("NewClient(%q) = nil error, want an error", name)
				}
				if strings.Contains(err.Error(), secret) {
					t.Errorf("NewClient() error = %q, want the password masked", err)
				}
			})
		}
	})
}

func TestParse_ReadsAnUntypedHistogramSumAndCount(t *testing.T) {
	t.Parallel()

	// Served without # TYPE the histogram is not a histogram to the parser: it arrives as
	// untyped _sum/_count/_bucket series. The mean must still come out, and the _bucket
	// series must not be mistaken for either.
	body := `smsc_served_latency_seconds_bucket{virtual_smsc="carrier-a",scenario="healthy",le="0.016"} 3
smsc_served_latency_seconds_bucket{virtual_smsc="carrier-a",scenario="healthy",le="+Inf"} 3
smsc_served_latency_seconds_sum{virtual_smsc="carrier-a",scenario="healthy"} 0.036
smsc_served_latency_seconds_count{virtual_smsc="carrier-a",scenario="healthy"} 3
smsc_served_latency_seconds_sum{virtual_smsc="carrier-a",scenario="slow-carrier"} 2.5
smsc_served_latency_seconds_count{virtual_smsc="carrier-a",scenario="slow-carrier"} 1
smsc_submit_sm_received_total{virtual_smsc="carrier-a"} 100
`
	snap, err := smscmetrics.Parse(strings.NewReader(body), time.Unix(0, 0))
	if err != nil {
		t.Fatalf("Parse() = %v, want no error", err)
	}
	a := snap.SMSCs["carrier-a"]
	if !closeTo(a.LatencySum, 2.536) {
		t.Errorf("LatencySum = %v, want %v", a.LatencySum, 2.536)
	}
	if !closeTo(a.LatencyCount, 4) {
		t.Errorf("LatencyCount = %v, want %v", a.LatencyCount, 4.0)
	}
}

func TestRate_VanishedOutcomeSeriesIsACounterReset(t *testing.T) {
	t.Parallel()

	// The virtual SMSC is still there; one of its outcome series is not. A registry that
	// dropped a series it had already published is a discontinuity, and treating the missing
	// key as a zero would silently rewrite that outcome's delta to -total, i.e. to nothing.
	t0 := time.Unix(1_700_000_000, 0)
	before := snapshotOf(t0, 1000, map[string]float64{"success": 900, "timeout": 100}, 40, 0.5, 1000)
	after := snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 1900}, 40, 1.0, 2000)

	_, err := smscmetrics.Rate(before, after)
	if !errors.Is(err, smscmetrics.ErrCounterReset) {
		t.Fatalf("Rate() with a vanished outcome series = %v, want ErrCounterReset", err)
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("Rate() error = %q, want it to name the vanished outcome", err)
	}
}

func TestRate_RefusesANonFiniteReading(t *testing.T) {
	t.Parallel()

	// NaN and +Inf are both legal in the Prometheus text format and expfmt decodes them as
	// they are. Neither can yield a rate: a NaN silently poisons every downstream comparison,
	// and a +Inf prints as an infinite ceiling while the tool exits 0.
	t0 := time.Unix(1_700_000_000, 0)
	inf := math.Inf(1)
	nan := math.NaN()

	cases := map[string]smscmetrics.Snapshot{
		"NaN _sum":          snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 2000}, 40, nan, 2000),
		"NaN received":      snapshotOf(t0.Add(time.Second), nan, map[string]float64{"success": 2000}, 40, 1.0, 2000),
		"+Inf received":     snapshotOf(t0.Add(time.Second), inf, map[string]float64{"success": 2000}, 40, 1.0, 2000),
		"+Inf outcome":      snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": inf}, 40, 1.0, 2000),
		"+Inf _count":       snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 2000}, 40, 1.0, inf),
		"+Inf _sum":         snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 2000}, 40, inf, 2000),
		"+Inf both sides":   snapshotOf(t0.Add(time.Second), inf, map[string]float64{"success": 2000}, 40, 1.0, 2000),
		"-Inf is backwards": snapshotOf(t0.Add(time.Second), math.Inf(-1), map[string]float64{"success": 2000}, 40, 1.0, 2000),
	}
	for name, after := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			before := snapshotOf(t0, 1000, map[string]float64{"success": 1000}, 40, 0.5, 1000)
			if name == "+Inf both sides" {
				before = snapshotOf(t0, inf, map[string]float64{"success": 1000}, 40, 0.5, 1000)
			}
			tp, err := smscmetrics.Rate(before, after)
			if !errors.Is(err, smscmetrics.ErrCounterReset) {
				t.Fatalf("Rate() = (%+v, %v), want ErrCounterReset", tp, err)
			}
		})
	}
}

func TestRate_PropagatesADeltaErrorFromEveryCounter(t *testing.T) {
	t.Parallel()

	// Four counters are differenced per virtual SMSC. Each one's rejection has to reach the
	// caller: a dropped error check on any of them turns a discontinuity into a zero delta.
	t0 := time.Unix(1_700_000_000, 0)
	cases := map[string]smscmetrics.Snapshot{
		"received goes backwards": snapshotOf(t0.Add(time.Second), 900, map[string]float64{"success": 2000}, 40, 1.0, 2000),
		"outcome goes backwards":  snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 900}, 40, 1.0, 2000),
		"_count goes backwards":   snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 2000}, 40, 1.0, 900),
		"_sum goes backwards":     snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 2000}, 40, 0.4, 2000),
	}
	for name, after := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			before := snapshotOf(t0, 1000, map[string]float64{"success": 1000}, 40, 0.5, 1000)
			tp, err := smscmetrics.Rate(before, after)
			if !errors.Is(err, smscmetrics.ErrCounterReset) {
				t.Fatalf("Rate() = (%+v, %v), want ErrCounterReset", tp, err)
			}
		})
	}
}

func TestRate_MeanServedLatencyStaysInRange(t *testing.T) {
	t.Parallel()

	// A float-to-int conversion out of the target's range is implementation-dependent in Go,
	// so the mean is clamped rather than left to the architecture. Absurd input, but a
	// nonsense duration read as a negative one is worse than a saturated one.
	t0 := time.Unix(1_700_000_000, 0)
	before := snapshotOf(t0, 1000, map[string]float64{"success": 1000}, 40, 0, 1000)
	after := snapshotOf(t0.Add(time.Second), 2000, map[string]float64{"success": 2000}, 40, 1e12, 1001)

	tp, err := smscmetrics.Rate(before, after)
	if err != nil {
		t.Fatalf("Rate() = %v, want no error", err)
	}
	if want := time.Duration(math.MaxInt64); tp.MeanServedLatency != want {
		t.Errorf("MeanServedLatency = %v, want %v", tp.MeanServedLatency, want)
	}
}

// snapshotOf builds a one-virtual-SMSC snapshot, the shape most rate assertions need.
func snapshotOf(at time.Time, received float64, outcomes map[string]float64, binds, latencySum, latencyCount float64) smscmetrics.Snapshot {
	return smscmetrics.Snapshot{
		At: at,
		SMSCs: map[string]smscmetrics.SMSC{
			"carrier-a": {
				SubmitReceived: received,
				Outcomes:       outcomes,
				ActiveBinds:    map[string]float64{"transceiver": binds},
				LatencySum:     latencySum,
				LatencyCount:   latencyCount,
			},
		},
	}
}
