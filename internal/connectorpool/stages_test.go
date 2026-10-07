package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

type passLimiter struct{}

func (passLimiter) WaitConnector(context.Context, uuid.UUID) error { return nil }

// TestSubmitStagesAreTimed: where a submit_sm spends its time is what the pool could not say during the
// step-287 campaign (~390 ms per message for a 5 ms SMSC). Each stage of a successful send is observed
// once, and each poll batch once for itself and once per shard.
func TestSubmitStagesAreTimed(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }})
	rec, err := pipeline.EncodeRouted(meteredRouted(0))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	reg := metrics.Guard(prometheus.NewRegistry())
	cat := metrics.NewCatalog()
	reg.MustRegister(cat.Collectors()...)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{rec}}, CDR: &fakeCDR{}, Metrics: cat,
		Producer: newRecordingProducer(), ConnectorID: meteredConnector, SendLimiter: passLimiter{},
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
	})
	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	counts := map[string]uint64{}
	for _, f := range families {
		if f.GetName() != "connector_submit_stage_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "stage" {
					counts[l.GetValue()] = m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	for _, stage := range []string{"limit", "claim", "sender", "submit", "dlrmap", "pin", "capture", "outcome", "shard", "batch"} {
		if counts[stage] != 1 {
			t.Errorf("stage %q observed %d times, want 1 (all: %v)", stage, counts[stage], counts)
		}
	}
}
