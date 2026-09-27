package metrics_test

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

// TestConfigRebuildCountsTheWholeClosure pins what the step-395 metric must not repeat from the Bloom
// gauges: last-success moves only when the WHOLE rebuild returned nil, so a rebuild that swapped its first
// component and then failed further down never reads as fresh.
func TestConfigRebuildCountsTheWholeClosure(t *testing.T) {
	c := metrics.NewCatalog()
	const earlier = 1_000
	c.ConfigRebuildLastSuccess.Set(earlier)

	failing := c.ObserveConfigRebuild(func(context.Context) error { return errors.New("postgres gone") })
	if err := failing(context.Background()); err == nil {
		t.Fatal("the wrapper swallowed the rebuild error: the Watcher would never retry")
	}
	if got := testutil.ToFloat64(c.ConfigRebuildLastSuccess); got != earlier {
		t.Errorf("a failed rebuild moved last-success to %v: the gauge would report a stale config as fresh", got)
	}
	if got := testutil.ToFloat64(c.ConfigRebuilds.WithLabelValues("error")); got != 1 {
		t.Errorf(`config_rebuild_total{outcome="error"} = %v, want 1`, got)
	}

	succeeding := c.ObserveConfigRebuild(func(context.Context) error { return nil })
	if err := succeeding(context.Background()); err != nil {
		t.Fatalf("the wrapper returned %v on a successful rebuild", err)
	}
	if got := testutil.ToFloat64(c.ConfigRebuildLastSuccess); got <= earlier {
		t.Errorf("a successful rebuild left last-success at %v", got)
	}
	if got := testutil.ToFloat64(c.ConfigRebuilds.WithLabelValues("ok")); got != 1 {
		t.Errorf(`config_rebuild_total{outcome="ok"} = %v, want 1`, got)
	}
}

// TestSeedConfigRebuildExposesBothOutcomesAndABootTimestamp: a counter vector exposes nothing until it
// has a child, so a failure rate would have no series to read; and an unseeded last-success reads 0 —
// "never refreshed" — on a healthy pod whose boot load IS its last successful build.
func TestSeedConfigRebuildExposesBothOutcomesAndABootTimestamp(t *testing.T) {
	c := metrics.NewCatalog()
	c.SeedConfigRebuild()

	if got := testutil.CollectAndCount(c.ConfigRebuilds); got != 2 {
		t.Errorf("config_rebuild_total exposes %d series after the seed, want 2 (ok and error)", got)
	}
	if got := testutil.ToFloat64(c.ConfigRebuildLastSuccess); got == 0 {
		t.Error("config_rebuild_last_success_timestamp_seconds reads 0 after the seed")
	}
}
