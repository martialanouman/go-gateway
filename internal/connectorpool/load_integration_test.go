package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/status"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestStatusHeartbeatFeedsConnectorLoad closes the loop step-114 left open: the pool's status heartbeat
// publishes each bind's in_flight, and what least_loaded reads — connectorload:{id}, through the same
// LoadReader the router wires — is derived from it. Until step-260d nothing wrote that key, so the
// strategy always read 0. The SMSC answers slowly so the window is genuinely occupied while the
// heartbeat fires: a pool that published 0 instead of its window depth would fail this test too.
func TestStatusHeartbeatFeedsConnectorLoad(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	connectorID := uuid.New()

	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp {
		return fakesmsc.Delay(1500 * time.Millisecond)
	}})
	recs := make([]kafka.Record, 0, 4)
	for range 4 {
		r := routed()
		r.ConnectorID = connectorID
		rec, err := pipeline.EncodeRouted(r)
		if err != nil {
			t.Fatalf("encode routed: %v", err)
		}
		recs = append(recs, rec)
	}
	sink := newPoolSink()
	rrec := otelrec.New(t)
	bind := poolBind(smsc.Addr(), 1)
	bind.WindowSize = 4
	svc := connectorpool.New(connectorpool.Deps{
		ConnectorID:     connectorID,
		Consumer:        &batchConsumer{records: recs},
		CDR:             sink.cdr,
		Producer:        sink.out,
		StatusControl:   status.NewReader(rdb),
		PodID:           "pod-test",
		StatusHeartbeat: 50 * time.Millisecond,
		Bind:            bind,
		Tracer:          observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	runErr := make(chan error, 1)
	go func() { runErr <- svc.Run(ctx) }()

	// Read the key, not the cache: this asserts the writer.
	lr := status.NewLoadReader(rdb, status.WithLoadCacheTTL(0))
	var seen int
	if !waitFor(3*time.Second, func() bool {
		seen = lr.InFlight(ctx, connectorID)
		return seen > 0
	}) {
		t.Fatalf("connectorload never rose above 0 while 4 submits were in flight: the status heartbeat "+
			"does not feed the gauge least_loaded reads (last read %d)", seen)
	}
	if seen > 4 {
		t.Errorf("connectorload = %d, more than the 4 messages ever in flight", seen)
	}

	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
