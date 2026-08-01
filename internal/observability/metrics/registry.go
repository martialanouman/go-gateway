package metrics

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Registry is a Prometheus registry that refuses collectors whose variable labels leave the bounded
// vocabulary (see [ValidateLabelNames]).
//
// It deliberately exposes no way to reach the wrapped registry: an escape hatch would be used, and the guard
// is only worth having if it cannot be stepped around by accident. It satisfies both prometheus.Registerer
// and prometheus.Gatherer, which is all promhttp and the service wiring need.
type Registry struct {
	inner *prometheus.Registry
}

// Guard wraps a registry so that every registration is checked. Metrics already on inner are not
// re-examined — guard a registry before anything registers on it.
func Guard(inner *prometheus.Registry) *Registry {
	return &Registry{inner: inner}
}

var (
	_ prometheus.Registerer = (*Registry)(nil)
	_ prometheus.Gatherer   = (*Registry)(nil)
)

// Register checks the collector's labels, then delegates. A rejected collector is never handed to the
// wrapped registry, so no series is created and nothing has to be undone.
func (r *Registry) Register(c prometheus.Collector) error {
	if err := checkCollector(c); err != nil {
		return err
	}
	return r.inner.Register(c)
}

// MustRegister panics on any rejection. Services call it during startup, which is the point: an unbounded
// label becomes a boot failure the first time the binary runs, not a slow TSDB death in production.
func (r *Registry) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := r.Register(c); err != nil {
			panic(err)
		}
	}
}

// Unregister removes a collector. Nothing to check on the way out.
func (r *Registry) Unregister(c prometheus.Collector) bool { return r.inner.Unregister(c) }

// Gather implements prometheus.Gatherer so the guarded registry can back /metrics directly.
func (r *Registry) Gather() ([]*dto.MetricFamily, error) { return r.inner.Gather() }

// checkCollector validates every Desc a collector describes.
//
// A collector that describes nothing is "unchecked" in Prometheus terms: it declares its metrics only when
// scraped, so its labels cannot be known here. Nothing in this repository is unchecked, and the runtime
// collectors carry no variable labels; should one ever appear, it bypasses the guard by construction and must
// be reviewed by hand.
func checkCollector(c prometheus.Collector) error {
	descs := make(chan *prometheus.Desc)
	go func() {
		defer close(descs)
		c.Describe(descs)
	}()

	var errs []error
	for d := range descs {
		name, labels, err := variableLabelsOf(d)
		if err != nil {
			// Drain the rest so Describe cannot block on an unread channel, then report.
			errs = append(errs, err)
			continue
		}
		if err := ValidateLabelNames(name, labels); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("metrics: label guard refused a collector: %w", errors.Join(errs...))
	}
	return nil
}
