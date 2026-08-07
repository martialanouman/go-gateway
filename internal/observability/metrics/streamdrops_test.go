package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

// countingSource stands in for anything that counts what it refused: a Kafka producer with a full buffer, an
// event publisher over its rate cap.
type countingSource struct{ n int64 }

func (s *countingSource) Dropped() int64 { return s.n }

// TestStreamDropCollectorFollowsItsSource is the assertion step-184 was missing. The publisher counted its
// drops and no collector read the counter, so a truncated feed looked identical to a complete one. Reading a
// real gather — not the source — is what ties the two together.
func TestStreamDropCollectorFollowsItsSource(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	src := &countingSource{}
	reg.MustRegister(metrics.StreamDropCollector("rate_cap", src))

	if got := gatherDrops(t, reg)["rate_cap"]; got != 0 {
		t.Fatalf("rate_cap = %v, want 0 before any drop", got)
	}

	src.n = 7
	if got := gatherDrops(t, reg)["rate_cap"]; got != 7 {
		t.Errorf("rate_cap = %v, want 7 after seven drops", got)
	}
}

// TestStreamDropReasonsCoexist: the three reasons share one metric name and differ only by a constant label,
// which Prometheus allows because a Desc is identified by name AND constant labels. If that were wrong the
// second registration would fail — and a service would panic at boot rather than lose a series silently.
func TestStreamDropReasonsCoexist(t *testing.T) {
	reg := metrics.Guard(prometheus.NewRegistry())
	buffer, rateCap, encode := &countingSource{n: 1}, &countingSource{n: 2}, &countingSource{n: 3}
	reg.MustRegister(
		metrics.StreamDropCollector("buffer", buffer),
		metrics.StreamDropCollector("rate_cap", rateCap),
		metrics.StreamDropCollector("encode", encode),
	)

	drops := gatherDrops(t, reg)
	for reason, want := range map[string]float64{"buffer": 1, "rate_cap": 2, "encode": 3} {
		if got, ok := drops[reason]; !ok {
			t.Errorf("reason %q missing from the exposition", reason)
		} else if got != want {
			t.Errorf("reason %q = %v, want %v", reason, got, want)
		}
	}
}

// gatherDrops returns the exposed drop counters keyed by their reason label. It gathers through the guarded
// registry, so a label outside the bounded vocabulary would drop the family and fail the lookup.
func gatherDrops(t *testing.T, reg *metrics.Registry) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	drops := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "metrics_stream_dropped_total" {
			continue
		}
		if family.GetType() != dto.MetricType_COUNTER {
			t.Errorf("metrics_stream_dropped_total is a %v, want a counter", family.GetType())
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "reason" {
					drops[pair.GetValue()] = metric.GetCounter().GetValue()
				}
			}
		}
	}
	return drops
}
