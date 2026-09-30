package connectorpool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

type sendLog struct{ events []string }

type loggingSendLimiter struct {
	log *sendLog
	err error
}

func (l *loggingSendLimiter) WaitConnector(context.Context, uuid.UUID) error {
	l.log.events = append(l.log.events, "wait")
	return l.err
}

type loggingCancelFlags struct{ log *sendLog }

func (f loggingCancelFlags) Claim(context.Context, uuid.UUID, cancel.Holder) (cancel.Holder, error) {
	f.log.events = append(f.log.events, "claim")
	return cancel.HolderNone, nil
}

func (loggingCancelFlags) Peek(context.Context, uuid.UUID) (cancel.Holder, error) {
	return cancel.HolderNone, nil
}

func sendLimitedService(t *testing.T, limiter SendLimiter, log *sendLog) *Service {
	t.Helper()
	return New(Deps{
		Producer:    &haltProducer{},
		CancelFlags: loggingCancelFlags{log: log},
		SendLimiter: limiter,
		Tracer:      observability.Tracer(otelrec.New(t).Provider(), "connector-pool"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// step-283: the connector's ceiling slows a send, it never rejects one. The wait comes before the cancel
// claim, so a message held by backpressure can still be cancelled.
func TestASendWaitsForTheConnectorsTokenBeforeClaimingIt(t *testing.T) {
	log := &sendLog{}
	s := sendLimitedService(t, &loggingSendLimiter{log: log}, log)
	routed := pipeline.RoutedMT{MessageID: uuid.New(), ConnectorID: uuid.New(), SegmentCount: 3, SubmittedAt: time.Now()}

	done, err := s.preDispatch(context.Background(), trace.SpanFromContext(context.Background()), 0, routed)
	if done || err != nil {
		t.Fatalf("preDispatch = (%v, %v), want the message sent", done, err)
	}
	if len(log.events) != 2 || log.events[0] != "wait" || log.events[1] != "claim" {
		t.Errorf("events = %v, want [wait claim]", log.events)
	}
}

// A wait cut short (shutdown, rebalance) leaves the record uncommitted: it is redelivered, never dropped.
func TestASendWhoseWaitIsCutShortIsRedelivered(t *testing.T) {
	log := &sendLog{}
	s := sendLimitedService(t, &loggingSendLimiter{log: log, err: context.Canceled}, log)
	routed := pipeline.RoutedMT{MessageID: uuid.New(), ConnectorID: uuid.New(), SegmentCount: 1, SubmittedAt: time.Now()}

	done, err := s.preDispatch(context.Background(), trace.SpanFromContext(context.Background()), 0, routed)
	if !done || !errors.Is(err, context.Canceled) {
		t.Fatalf("preDispatch = (%v, %v), want settled with the cancellation, so nothing commits", done, err)
	}
	if len(log.events) != 1 {
		t.Errorf("events = %v, want the wait alone: a message that never got its token is not claimed", log.events)
	}
}
