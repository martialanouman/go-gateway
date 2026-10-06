package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

type flagConfigSource struct{ flagDefault uint8 }

func (c flagConfigSource) Load(context.Context, uuid.UUID) (connectorpool.LiveConfig, error) {
	return connectorpool.LiveConfig{BindPoolSize: 1, PriorityFlagDefault: c.flagDefault}, nil
}

// TestSubmitCarriesThePriorityFlag pins ADR-0020 §3 through the live config: the effective priority when
// it is non-zero, the connector's priority_flag_default otherwise.
func TestSubmitCarriesThePriorityFlag(t *testing.T) {
	for _, c := range []struct {
		name     string
		priority int
		want     uint8
	}{
		{"marketing takes the connector default", 0, 2},
		{"a non-zero effective priority wins", 3, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := make(chan uint8, 1)
			smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: func(sm smpp.SubmitSM) fakesmsc.Resp {
				got <- sm.PriorityFlag
				return fakesmsc.OK()
			}})
			r := routed()
			r.TrafficCategory, r.Priority = cp.TrafficOTP, c.priority
			rec, err := pipeline.EncodeRouted(r)
			if err != nil {
				t.Fatalf("encode routed: %v", err)
			}
			sink := newPoolSink()
			svc := connectorpool.New(connectorpool.Deps{
				Consumer:     &fakeConsumer{records: []kafka.Record{rec}},
				CDR:          sink.cdr,
				Producer:     sink.out,
				ConfigSource: flagConfigSource{flagDefault: 2},
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
			if flag := <-got; flag != c.want {
				t.Errorf("priority_flag = %d, want %d", flag, c.want)
			}
		})
	}
}
