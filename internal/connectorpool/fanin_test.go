package connectorpool_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

type oneRecordThenBlock struct{ rec kafka.Record }

func (c oneRecordThenBlock) RunBatch(ctx context.Context, h kafka.BatchHandler) error {
	h(ctx, []kafka.Record{c.rec})
	<-ctx.Done()
	return nil
}

type failing struct{ err error }

func (f failing) RunBatch(context.Context, kafka.BatchHandler) error { return f.err }

func TestFanInFeedsOneHandlerFromEveryConsumer(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]bool{}
	ctx, cancel := context.WithCancel(t.Context())
	fan := connectorpool.FanIn(oneRecordThenBlock{kafka.Record{Topic: "a"}}, oneRecordThenBlock{kafka.Record{Topic: "b"}})
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
	fan := connectorpool.FanIn(blockingConsumer{}, failing{boom}, blockingConsumer{})
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
