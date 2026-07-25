package smppserver

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/martialanouman/go-gateway/internal/session/disconnect"
)

// disconnectRetryBackoff paces re-reads after a subscription receive error, so a Redis outage neither
// spins the CPU nor stops the pod: the subscriber keeps retrying, degraded, while binds and submits
// carry on.
const disconnectRetryBackoff = time.Second

// DisconnectStream yields raw disconnect payloads until it is closed or ctx is cancelled.
// *redisstore.Subscription satisfies it; a fake satisfies it in tests.
type DisconnectStream interface {
	Receive(ctx context.Context) ([]byte, error)
	Close() error
}

// DisconnectApplier applies a decoded force-disconnect order to the sessions a pod owns. *Listener
// satisfies it.
type DisconnectApplier interface {
	Disconnect(scope disconnect.Scope, id, reason string)
}

// RunDisconnectSubscriber consumes force-disconnect orders from stream and applies them to target
// until ctx is cancelled (step-032). It is a supervisor component and is deliberately fail-open: a
// malformed payload is logged and skipped, and a receive error (a Redis blip) is logged and retried
// after a short backoff, so the disconnect fan-out degrading never crashes the pod or stops it
// accepting binds. It returns nil on an orderly stop (ctx cancelled), closing the stream on the way
// out.
func RunDisconnectSubscriber(ctx context.Context, stream DisconnectStream, target DisconnectApplier, logger *slog.Logger) error {
	// Close the stream when ctx ends so a Receive blocked on the socket unblocks: the underlying Redis
	// read does not reliably observe a context cancellation on its own, only a Close. A sync.Once
	// dedupes the watcher's close and the return-path close, and guarantees the stream is closed by the
	// time the subscriber returns.
	var closeOnce sync.Once
	closeStream := func() { closeOnce.Do(func() { _ = stream.Close() }) }
	defer closeStream()

	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
		case <-stopped:
		}
		closeStream()
	}()

	for {
		payload, err := stream.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.WarnContext(ctx, "smpp disconnect subscriber: receive failed; retrying", "err", err)
			sleep(ctx, disconnectRetryBackoff)
			continue
		}

		ev, err := disconnect.Decode(payload)
		if err != nil {
			// A corrupt or forward-incompatible order is dropped, never fanned out to an over-broad set
			// of sessions.
			logger.WarnContext(ctx, "smpp disconnect subscriber: dropping malformed order", "err", err)
			continue
		}
		target.Disconnect(ev.Scope, ev.ID, ev.Reason)
	}
}
