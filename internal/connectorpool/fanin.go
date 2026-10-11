package connectorpool

import (
	"context"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// FanIn runs every consumer with the same handler, so the pool reads one topic per traffic category
// (ADR-0021 §4) through a single Consumer. The first consumer to return stops the others.
type FanIn []Consumer

// RunBatch runs every consumer until the first returns, then waits for the rest.
func (f FanIn) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(f))
	for _, c := range f {
		go func() { errs <- c.RunBatch(ctx, handle) }()
	}
	first := <-errs
	cancel()
	for range len(f) - 1 {
		if err := <-errs; first == nil {
			first = err
		}
	}
	return first
}
