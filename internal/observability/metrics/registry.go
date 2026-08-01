package metrics

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Registry is a Prometheus registry that keeps every published label inside the bounded vocabulary (see
// [ValidateLabelNames]): it refuses offending collectors at registration and drops offending families at
// gather time.
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

// Gather implements prometheus.Gatherer, and validates once more — this time against what is actually about
// to be served.
//
// Registration only sees what a collector DECLARES. A hand-written Collector can describe a bounded label and
// then emit something else: Prometheus identifies a Desc by its name and CONSTANT labels only (desc.go, the
// id hash), so even a pedantic registry accepts a metric whose variable labels differ from the declared ones.
// Checking the exposition itself closes that for good, and covers constant labels too, which registration
// deliberately ignores.
//
// An offending family is DROPPED, never served, and reported as an error. Dropping rather than failing whole
// keeps the other metrics intact; the error still surfaces (promhttp answers 500 by default), which is right
// — a collector emitting an unbounded label is a code defect, not a traffic condition.
func (r *Registry) Gather() ([]*dto.MetricFamily, error) {
	families, err := r.inner.Gather()

	var errs []error
	if err != nil {
		errs = append(errs, err)
	}
	kept := families[:0]
	for _, family := range families {
		if err := checkFamily(family); err != nil {
			errs = append(errs, err)
			continue
		}
		kept = append(kept, family)
	}
	return kept, errors.Join(errs...)
}

// checkFamily validates the label names a metric family is about to expose.
func checkFamily(family *dto.MetricFamily) error {
	for _, metric := range family.GetMetric() {
		names := make([]string, 0, len(metric.GetLabel()))
		for _, pair := range metric.GetLabel() {
			names = append(names, pair.GetName())
		}
		if err := ValidateLabelNames(family.GetName(), names); err != nil {
			return fmt.Errorf("metrics: label guard dropped a metric family before exposition: %w", err)
		}
	}
	return nil
}

// checkCollector validates every Desc a collector describes.
//
// A collector that describes NOTHING is "unchecked" in Prometheus terms: it declares its metrics only when
// scraped, so no label can be validated here — it would be a hole straight through the guard. Such a
// collector is refused. Every collector in this repository and in the libraries it uses (the Go and process
// collectors, promhttp's handler counter) describes its metrics, so nothing legitimate is turned away; the
// day an unchecked collector is genuinely needed, refusing it forces the conversation.
//
// Note that Describe runs twice per registration — once here, once inside the wrapped registry. A collector
// whose Describe is not idempotent (one that drains a slice, say) would look unchecked the second time; that
// is a broken collector either way, but the failure would be confusing.
func checkCollector(c prometheus.Collector) error {
	descs := make(chan *prometheus.Desc)
	go func() {
		defer close(descs)
		c.Describe(descs)
	}()

	var (
		errs  []error
		count int
	)
	for d := range descs {
		count++
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
	if count == 0 {
		errs = append(errs, errors.New(
			"collector describes no metric (unchecked): its labels cannot be validated, so it could expose any"+
				" label at scrape time"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("metrics: label guard refused a collector: %w", errors.Join(errs...))
	}
	return nil
}
