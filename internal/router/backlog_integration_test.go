package router_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// slowReserver times out like billing-svc under a resumption burst, then answers.
type slowReserver struct{ timeouts atomic.Int32 }

func (s *slowReserver) Reserve(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int) (bool, string, error) {
	if s.timeouts.Add(-1) >= 0 {
		return false, "", status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	}
	return true, cp.OwnerTypeCustomer, nil
}

// TestRouterDrainsABacklogThroughReserveTimeouts is step-285's DoD: the router used to return the first
// reserve timeout of a backlog, and the supervisor restarted the process into the same burst.
func TestRouterDrainsABacklogThroughReserveTimeouts(t *testing.T) {
	cfg := config.Kafka{Brokers: kafkatest.Brokers(t), Timeout: 3 * time.Second}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	defer producer.Close()

	const backlog = 200
	want := make(map[uuid.UUID]bool, backlog)
	for range backlog {
		in := inbound("+2250700000000")
		rec, err := pipeline.EncodeInbound(in)
		if err != nil {
			t.Fatalf("encode inbound: %v", err)
		}
		if err := producer.Produce(t.Context(), rec); err != nil {
			t.Fatalf("produce: %v", err)
		}
		want[in.MessageID] = true
	}

	consumer, err := kafka.NewConsumer(cfg, "router-backlog-"+uuid.NewString(), kafka.TopicMTInbound)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()
	reserver := &slowReserver{}
	reserver.timeouts.Store(3)
	prod := &fakeProducer{}
	r := newRouterWithReserver(t, stubResolver{conn: uuid.New()}, reserver, prod, &fakeCDR{}, consumer)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(ctx) }()

	deadline := time.After(60 * time.Second)
	for routed := map[uuid.UUID]bool{}; len(routed) < backlog; {
		select {
		case err := <-runErr:
			t.Fatalf("the router stopped on a reserve timeout (%v): its supervisor would restart it into the same backlog", err)
		case <-deadline:
			t.Fatalf("%d of %d backlog messages routed", len(routed), backlog)
		case <-time.After(100 * time.Millisecond):
		}
		prod.mu.Lock()
		for _, rec := range prod.produced {
			if m, err := pipeline.DecodeRouted(rec); err == nil && want[m.MessageID] {
				routed[m.MessageID] = true
			}
		}
		prod.mu.Unlock()
	}
	for {
		lag, err := consumer.Lag(t.Context())
		if err == nil && lag[kafka.TopicMTInbound] == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("the backlog was routed but not committed: lag %v (%v)", lag, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Errorf("Run after cancel = %v, want nil", err)
	}
}
