package restapi

import (
	"context"
	"log/slog"
	"time"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// acceptedWriteTimeout bounds a single accepted-row insert, including during the shutdown drain, so
// a stalled ClickHouse cannot hold the service from terminating.
const acceptedWriteTimeout = 5 * time.Second

// AcceptedWriter writes the accepted CDR row off the request path (§1.10): submit-messages earns
// its 202 from the durable Kafka write and never blocks on ClickHouse, so the accepted row is a
// best-effort read-model projection written by this bounded pool. If a write is ever dropped under
// saturation, the connector's enroute row still supersedes it — get-message shows enroute a moment
// later, never a 404.
//
// The pool is bounded and supervised: workers stop on ctx and are joined by Run, so there is no
// orphan goroutine (guide §5).
type AcceptedWriter struct {
	cdr     CDRWriter
	ch      chan clickhouse.CDRRow
	workers int
	logger  *slog.Logger
}

// CDRWriter appends CDR rows. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// NewAcceptedWriter builds a pool with the given worker count and queue depth. Both are clamped to
// at least 1.
func NewAcceptedWriter(cdr CDRWriter, workers, queue int, logger *slog.Logger) *AcceptedWriter {
	if workers < 1 {
		workers = 1
	}
	if queue < 1 {
		queue = 1
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &AcceptedWriter{
		cdr:     cdr,
		ch:      make(chan clickhouse.CDRRow, queue),
		workers: workers,
		logger:  logger,
	}
}

// Enqueue schedules an accepted-row write. It never blocks the request: if the queue is saturated
// the row is dropped with a warning — the message is already durable in Kafka and will get an
// enroute row, so the only cost is a brief window where get-message could 404 under overload.
func (w *AcceptedWriter) Enqueue(row clickhouse.CDRRow) {
	select {
	case w.ch <- row:
	default:
		w.logger.Warn("accepted-row queue saturated, dropping projection",
			"message_id", row.MessageID)
	}
}

// Run starts the workers and blocks until ctx is cancelled, then drains what is buffered and
// returns. It is meant to run as one supervised goroutine.
func (w *AcceptedWriter) Run(ctx context.Context) error {
	done := make(chan struct{})
	for i := 0; i < w.workers; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			w.work(ctx)
		}()
	}
	for i := 0; i < w.workers; i++ {
		<-done
	}
	return nil
}

func (w *AcceptedWriter) work(ctx context.Context) {
	for {
		select {
		case row := <-w.ch:
			w.write(row) //nolint:contextcheck // detached on purpose: the request context is gone
		case <-ctx.Done():
			// Drain what is buffered, then exit. Each worker drains until the channel is empty; a
			// concurrent receive by another worker is safe.
			for {
				select {
				case row := <-w.ch:
					w.write(row) //nolint:contextcheck // detached: run the write to completion on shutdown
				default:
					return
				}
			}
		}
	}
}

// write inserts one row on a bounded, detached context: the request that produced the row has long
// returned, so the write must not depend on its (cancelled) context, but it must still be bounded.
func (w *AcceptedWriter) write(row clickhouse.CDRRow) {
	ctx, cancel := context.WithTimeout(context.Background(), acceptedWriteTimeout)
	defer cancel()
	if err := w.cdr.Insert(ctx, row); err != nil {
		w.logger.Error("accepted-row write failed", "message_id", row.MessageID, "err", err)
	}
}
