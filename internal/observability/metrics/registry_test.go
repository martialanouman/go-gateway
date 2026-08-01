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
