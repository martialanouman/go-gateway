package metrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

func TestGuardAcceptsBoundedLabels(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "probe_total",
		Help: "canary",
	}, []string{"connector_id", "status"})

	if err := reg.Register(counter); err != nil {
		t.Fatalf("Register: %v", err)
	}
}

// TestGuardRefusesAnUnboundedLabel is the acceptance criterion of step-180: the offending metric never makes
// it onto the registry, so the series is never created — as opposed to a lint someone can wave through.
func TestGuardRefusesAnUnboundedLabel(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "leaky_total",
		Help: "one series per destination number",
	}, []string{"msisdn"})

	err := reg.Register(counter)
	if err == nil {
		t.Fatal("Register accepted an msisdn label")
	}
	if !strings.Contains(err.Error(), "msisdn") || !strings.Contains(err.Error(), "leaky_total") {
		t.Errorf("error %q should name both the metric and the label", err)
	}

	// And it must not be half-registered: nothing may be gathered from it.
	families, gatherErr := reg.Gather()
	if gatherErr != nil {
		t.Fatalf("Gather: %v", gatherErr)
	}
	for _, f := range families {
		if f.GetName() == "leaky_total" {
			t.Error("the refused metric reached the registry anyway")
		}
	}
}

// TestGuardRefusesEveryCollectorKind: the guard reads Descs, so it must hold for gauges and histograms too,
// not only the counter the happy path was written against.
func TestGuardRefusesEveryCollectorKind(t *testing.T) {
	collectors := map[string]prometheus.Collector{
		"gauge": prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "g", Help: "h"}, []string{"message_id"}),
		"histogram": prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: "h_seconds", Help: "h"}, []string{"msisdn"}),
		"summary": prometheus.NewSummaryVec(
			prometheus.SummaryOpts{Name: "s_seconds", Help: "h"}, []string{"body"}),
	}
	for kind, c := range collectors {
		t.Run(kind, func(t *testing.T) {
			reg := metrics.Guard(prometheus.NewRegistry())
			if err := reg.Register(c); err == nil {
				t.Fatalf("%s with an unbounded label was accepted", kind)
			}
		})
	}
}

// TestMustRegisterPanicsOnAnUnboundedLabel: services wire their metrics with MustRegister at startup, so this
// is what actually turns a bad label into a boot failure rather than a production surprise.
func TestMustRegisterPanicsOnAnUnboundedLabel(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	defer func() {
		if recover() == nil {
			t.Fatal("MustRegister did not panic on an unbounded label")
		}
	}()
	reg.MustRegister(prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "leaky_total", Help: "h"}, []string{"msisdn"}))
}

// uncheckedCollector describes nothing and only reveals its metrics when scraped. It is the guard's blind
// spot: registration cannot see a label that does not exist yet, so this one publishes an MSISDN and a body
// as const labels the moment Prometheus collects it.
type uncheckedCollector struct{}

func (uncheckedCollector) Describe(chan<- *prometheus.Desc) {}

func (uncheckedCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("leak_total", "h",
			nil, prometheus.Labels{"msisdn": "33612345678", "body": "SECRET"}),
		prometheus.CounterValue, 1)
}

// TestGuardRefusesAnUncheckedCollector closes that blind spot. Verified beforehand that every collector this
// repository registers — its own vectors, the Go and process collectors, promhttp's handler counter —
// describes at least one Desc, so refusing unchecked collectors turns nothing legitimate away.
func TestGuardRefusesAnUncheckedCollector(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())

	if err := reg.Register(uncheckedCollector{}); err == nil {
		t.Fatal("an unchecked collector was accepted: it can publish any label at scrape time")
	}
}

// lyingCollector declares a bounded label and emits a different one. Registration validates what a collector
// DECLARES, so it passes the guard — only the registry can catch the mismatch, and only if it is pedantic.
type lyingCollector struct{}

func (lyingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("liar_total", "h", []string{"status"}, nil)
}

func (lyingCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("liar_total", "h", []string{"msisdn"}, nil),
		prometheus.CounterValue, 1, "33600000000")
}

// TestGatherDropsACollectorThatLiesAboutItsLabels is the second half of the guard.
//
// A pedantic registry does NOT catch this: Prometheus identifies a Desc by its name and constant labels, so
// "liar_total with status" and "liar_total with msisdn" share an id and the mismatch goes unnoticed. Only
// checking the exposition itself catches it — and the family must be DROPPED, not merely reported, or the
// MSISDN is served alongside the error.
func TestGatherDropsACollectorThatLiesAboutItsLabels(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	if err := reg.Register(lyingCollector{}); err != nil {
		t.Fatalf("Register: %v — the lie is only visible when gathering", err)
	}
	// A healthy metric alongside it: dropping the offender must not blind everything else.
	ok := prometheus.NewCounter(prometheus.CounterOpts{Name: "probe_total", Help: "h"})
	reg.MustRegister(ok)
	ok.Inc()

	families, err := reg.Gather()
	if err == nil {
		t.Fatal("Gather reported no error on an undeclared msisdn label")
	}
	for _, f := range families {
		if f.GetName() == "liar_total" {
			t.Errorf("the offending family was served anyway: %v", f)
		}
	}
	var kept bool
	for _, f := range families {
		if f.GetName() == "probe_total" {
			kept = true
		}
	}
	if !kept {
		t.Error("the healthy metric was dropped too; only the offender should be")
	}
}

// TestGatherRefusesAConstLabelLeak: constant labels are skipped at registration (they are fixed at
// construction, so they cannot explode), but nothing stops one from holding an MSISDN. The exposition check
// covers them.
func TestGatherRefusesAConstLabelLeak(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	leak := prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "leak_total",
		Help:        "h",
		ConstLabels: prometheus.Labels{"msisdn": "33612345678"},
	})
	if err := reg.Register(leak); err != nil {
		t.Fatalf("Register: %v", err)
	}

	families, err := reg.Gather()
	if err == nil {
		t.Fatal("a constant msisdn label was served")
	}
	for _, f := range families {
		if f.GetName() == "leak_total" {
			t.Error("the leaking family reached the exposition")
		}
	}
}

// TestGuardStillReportsPrometheusOwnErrors: the guard must not swallow duplicate registration, the error the
// registry itself raises.
func TestGuardStillReportsPrometheusOwnErrors(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "probe_total", Help: "h"})

	if err := reg.Register(c); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := reg.Register(c); err == nil {
		t.Fatal("duplicate registration was accepted")
	}
}

// TestGuardAcceptsTheRuntimeCollectors: the Go and process collectors are registered by every ops server. If
// the guard rejected them, no service would boot.
func TestGuardAcceptsTheRuntimeCollectors(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	// And they must SCRAPE cleanly, not merely register: the gather-time check sees constant labels that
	// registration skips — go_info{version} is one, and it blanked /metrics until the vocabulary knew it.
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("the runtime collectors do not survive a scrape: %v", err)
	}
}

// TestGuardAcceptsThePromhttpHandlerMetric: promhttp.HandlerFor registers its own error counter on whatever
// registry it is handed, and PANICS if registration fails. The guard therefore has to accept it, or no ops
// server could be constructed — a real failure this test caught the first time the guard was wired in.
func TestGuardAcceptsThePromhttpHandlerMetric(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

func TestGuardGathersWhatItRegistered(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "probe_total", Help: "h"})
	reg.MustRegister(c)
	c.Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var found bool
	for _, f := range families {
		if f.GetName() == "probe_total" {
			found = true
		}
	}
	if !found {
		t.Error("probe_total is not gathered — the guard must stay a working registry")
	}
}
