package connectorpool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

func failing(err error) feederConsumer {
	return feederConsumer{fn: func(context.Context, kafka.BatchHandler) error { return err }}
}

func TestFanInFeedsOneHandlerFromEveryConsumer(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]bool{}
	ctx, cancel := context.WithCancel(t.Context())
	fan := connectorpool.FanIn{&feedThenBlock{records: []kafka.Record{{Topic: "a"}}}, &feedThenBlock{records: []kafka.Record{{Topic: "b"}}}}
	done := make(chan error, 1)
	go func() {
		done <- fan.RunBatch(ctx, func(_ context.Context, recs []kafka.Record) []error {
			mu.Lock()
			defer mu.Unlock()
			seen[recs[0].Topic] = true
			if len(seen) == 2 {
				cancel()
			}
			return make([]error, len(recs))
		})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("FanIn never returned; handler saw %v", seen)
	}
	mu.Lock()
	defer mu.Unlock()
	if !seen["a"] || !seen["b"] {
		t.Errorf("handler saw %v, want both consumers' records", seen)
	}
}

func TestFanInStopsEveryConsumerOnTheFirstError(t *testing.T) {
	boom := errors.New("boom")
	fan := connectorpool.FanIn{blockingConsumer{}, failing(boom), blockingConsumer{}}
	done := make(chan error, 1)
	go func() {
		done <- fan.RunBatch(t.Context(), func(context.Context, []kafka.Record) []error { return nil })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Errorf("FanIn returned %v, want %v", err, boom)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a failed consumer left the others running: FanIn never returned")
	}
}

func TestFanInWaitsForEveryConsumerBeforeReturning(t *testing.T) {
	var slowStopped atomic.Bool
	slow := feederConsumer{fn: func(ctx context.Context, _ kafka.BatchHandler) error {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		slowStopped.Store(true)
		return nil
	}}
	fan := connectorpool.FanIn{failing(errors.New("boom")), slow}
	_ = fan.RunBatch(t.Context(), func(context.Context, []kafka.Record) []error { return nil })
	if !slowStopped.Load() {
		t.Error("FanIn returned while a consumer was still running")
	}
}
