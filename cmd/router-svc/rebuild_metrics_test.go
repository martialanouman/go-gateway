package main

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestRebuildMetricsCountTheWholeClosure pins what the step-395 metric must not repeat from the Bloom
// gauges: last-success moves only when the WHOLE rebuild returned nil, so a rebuild that swapped its
// routes and then failed further down never reads as fresh.
func TestRebuildMetricsCountTheWholeClosure(t *testing.T) {
	m := newRebuildMetrics()
	const earlier = 1_000
	m.lastSuccess.Set(earlier)

	failing := m.observe(func(context.Context) error { return errors.New("postgres gone") })
	if err := failing(context.Background()); err == nil {
		t.Fatal("observe swallowed the rebuild error: the Watcher would never retry")
	}
	if got := testutil.ToFloat64(m.lastSuccess); got != earlier {
		t.Errorf("a failed rebuild moved last-success to %v: the gauge would report a stale config as fresh", got)
	}
	if got := testutil.ToFloat64(m.total.WithLabelValues("error")); got != 1 {
		t.Errorf(`config_rebuild_total{outcome="error"} = %v, want 1`, got)
	}

	succeeding := m.observe(func(context.Context) error { return nil })
	if err := succeeding(context.Background()); err != nil {
		t.Fatalf("observe returned %v on a successful rebuild", err)
	}
	if got := testutil.ToFloat64(m.lastSuccess); got <= earlier {
		t.Errorf("a successful rebuild left last-success at %v", got)
	}
	if got := testutil.ToFloat64(m.total.WithLabelValues("ok")); got != 1 {
		t.Errorf(`config_rebuild_total{outcome="ok"} = %v, want 1`, got)
	}
}
