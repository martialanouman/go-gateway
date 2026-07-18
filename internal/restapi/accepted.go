package restapi

import (
	"context"
	"log/slog"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/e164"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

const (
	// acceptedWriteTimeout bounds a single accepted-row batch flush, including during the shutdown
	// drain, so a stalled ClickHouse cannot hold the service from terminating.
	acceptedWriteTimeout = 5 * time.Second
	// acceptedBatchSize flushes a worker's buffer once it reaches this many rows: one ClickHouse
	// round-trip amortizes the prepare+send cost across the batch instead of paying it per row.
	acceptedBatchSize = 128
	// acceptedFlushInterval bounds how long a partial batch waits before flushing. It is short so a
	// just-accepted message lands almost immediately at low traffic (keeping the get-message 404 window
	// tight); under load the batch fills to acceptedBatchSize and flushes on size well before this.
	acceptedFlushInterval = 25 * time.Millisecond
)

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

// CDRWriter writes a batch of CDR rows. *clickhouse.CDRWriter satisfies it. The accepted-row pool
// only ever writes in batches, off the request path.
type CDRWriter interface {
	InsertBatch(ctx context.Context, rows []clickhouse.CDRRow) error
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
	buf := make([]clickhouse.CDRRow, 0, acceptedBatchSize)
	ticker := time.NewTicker(acceptedFlushInterval)
	defer ticker.Stop()

	// flush writes the buffer on a detached, bounded context (see writeBatch): the requests are gone.
	flush := func() { //nolint:contextcheck // detached on purpose: the request context is gone
		if len(buf) == 0 {
			return
		}
		w.writeBatch(buf)
		buf = buf[:0]
	}

	for {
		select {
		case row := <-w.ch:
			buf = append(buf, row)
			if len(buf) >= acceptedBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// Drain what is buffered, then flush the remainder and exit. Each worker drains until the
			// channel is empty; a concurrent receive by another worker is safe.
			for {
				select {
				case row := <-w.ch:
					buf = append(buf, row)
					if len(buf) >= acceptedBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// writeBatch normalizes each row's destination — the heavy phone parse the request path deliberately
// deferred here — then flushes the batch on a bounded, detached context: the requests that produced
// the rows have long returned, so the write must not depend on their (cancelled) contexts, but it
// must still be bounded. Normalization is best-effort: a number that fails to parse keeps its raw
// form (the router is the single rejection authority). A failed batch is logged and dropped — the
// connector's enroute row supersedes an accepted projection, so get-message shows enroute a moment
// later, never a persistent 404.
func (w *AcceptedWriter) writeBatch(rows []clickhouse.CDRRow) {
	for i := range rows {
		if norm, err := e164.Normalize(rows[i].DestAddr); err == nil {
			rows[i].DestAddr = norm
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), acceptedWriteTimeout)
	defer cancel()
	if err := w.cdr.InsertBatch(ctx, rows); err != nil {
		w.logger.Error("accepted-row batch write failed", "rows", len(rows), "err", err)
	}
}
