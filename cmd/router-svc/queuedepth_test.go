package main

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
)

// fakeLag is a consumer group whose backlog is whatever the test says it is.
type fakeLag struct {
	lags map[string]int64
	err  error
}

func (f fakeLag) Lag(context.Context) (map[string]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.lags, nil
}

// discardSink drops every snapshot: this test asserts on the Prometheus gauge, not on the dashboard feed.
type discardSink struct{}

func (discardSink) TryPublish([]byte, []byte) {}

func newTestEmitter(t *testing.T) *metricstream.Emitter {
	t.Helper()

	e, err := metricstream.New(serviceName, discardSink{})
	if err != nil {
		t.Fatalf("metricstream.New: %v", err)
	}
	return e
}

// TestQueueDepthCoversTheOutcomeProjection: router-svc consumes TWO groups and owns the depth of both
// topics. mt.outcome's backlog is the numerator of the status-lag alert (step-201c, D13); polled from
// mt.inbound's consumer alone, as it was before this step, that series never exists and the alert has
// nothing to divide.
func TestQueueDepthCoversTheOutcomeProjection(t *testing.T) {
	t.Parallel()

	cat := metrics.NewCatalog()
	readers := []lagReader{
		fakeLag{lags: map[string]int64{"mt.inbound": 10}},
		fakeLag{lags: map[string]int64{"mt.outcome": 42}},
	}

	publishQueueDepth(t.Context(), readers, newTestEmitter(t), cat, silentLogger())

	for topic, want := range map[string]float64{"mt.inbound": 10, "mt.outcome": 42} {
		if got := testutil.ToFloat64(cat.QueueDepth.WithLabelValues(topic)); got != want {
			t.Errorf("queue_depth_records{queue=%q} = %v, want %v", topic, got, want)
		}
	}
}

// TestQueueDepthOfOneGroupSurvivesAnotherGroupsFault pins D16: the skipped tick is PER CONSUMER.
//
// Sharing one tick would let a fault on mt.inbound erase mt.outcome's depth from the same scrape — we
// would install the status-lag alert and then have it silenced by an unrelated neighbour's outage.
func TestQueueDepthOfOneGroupSurvivesAnotherGroupsFault(t *testing.T) {
	t.Parallel()

	cat := metrics.NewCatalog()
	readers := []lagReader{
		fakeLag{err: errors.New("group not described")},
		fakeLag{lags: map[string]int64{"mt.outcome": 7}},
	}

	publishQueueDepth(t.Context(), readers, newTestEmitter(t), cat, silentLogger())

	if got := testutil.ToFloat64(cat.QueueDepth.WithLabelValues("mt.outcome")); got != 7 {
		t.Errorf("queue_depth_records{queue=\"mt.outcome\"} = %v, want 7: a fault reading one group's lag "+
			"must not take the others' depth down with it", got)
	}
}
