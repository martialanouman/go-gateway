package ingest

import (
	"context"
	"log/slog"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
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

// AcceptedWriter writes the accepted CDR row off the request path (§1.10): a submission earns its
// acknowledgement from the durable Kafka write and never blocks on ClickHouse, so the accepted row
// is a best-effort read-model projection written by this bounded pool. If a write is ever dropped
// under saturation, the connector's enroute row still supersedes it — get-message shows enroute a
// moment later, never a 404.
//
// The pool is bounded and supervised: workers stop on ctx and are joined by Run, so there is no
// orphan goroutine (guide §5).
type AcceptedWriter struct {
	cdr     CDRWriter
	sealer  *ContentSealer
	ch      chan acceptedItem
	workers int
	logger  *slog.Logger
}

// acceptedItem is one queued accepted row plus the plaintext body it may need sealed into the CDR. body is
// empty unless the customer's effective content policy stores it (resolved once at Enqueue, off the async
// worker), so off-policy messages — the default — carry no body through the queue.
type acceptedItem struct {
	row    clickhouse.CDRRow
	body   msg.Body
	policy cp.ContentStorage
}

// CDRWriter writes a batch of CDR rows. *clickhouse.CDRWriter satisfies it. The accepted-row pool
// only ever writes in batches, off the request path.
type CDRWriter interface {
	InsertBatch(ctx context.Context, rows []clickhouse.CDRRow) error
}

// NewAcceptedWriter builds a pool with the given worker count and queue depth. Both are clamped to
// at least 1. sealer may be nil: content storage is then disabled and no body is ever carried or written
// (the pre-M10 behaviour).
func NewAcceptedWriter(cdr CDRWriter, sealer *ContentSealer, workers, queue int, logger *slog.Logger) *AcceptedWriter {
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
		sealer:  sealer,
		ch:      make(chan acceptedItem, queue),
		workers: workers,
		logger:  logger,
	}
}

// Enqueue schedules an accepted-row write. It never blocks the request: if the queue is saturated
// the row is dropped with a warning — the message is already durable in Kafka and will get an
// enroute row, so the only cost is a brief window where get-message could 404 under overload.
//
// The content policy is resolved here, on the request path (a lock-free map read), so that only messages
// whose customer actually stores content carry the plaintext body through the queue to the async sealer.
func (w *AcceptedWriter) Enqueue(row clickhouse.CDRRow, body msg.Body) {
	item := acceptedItem{row: row, policy: cp.ContentOff}
	if w.sealer != nil {
		if policy := w.sealer.Policy(row.CustomerID); policy != cp.ContentOff {
			item.policy, item.body = policy, body
		}
	}
	select {
	case w.ch <- item:
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
	buf := make([]acceptedItem, 0, acceptedBatchSize)
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
		case item := <-w.ch:
			buf = append(buf, item)
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
				case item := <-w.ch:
					buf = append(buf, item)
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
func (w *AcceptedWriter) writeBatch(items []acceptedItem) {
	// Known limit: this single deadline bounds both the (serial) per-row DEK fetches and the final
	// InsertBatch. In steady state the DEK cache is warm so sealing is negligible; under a cold-cache burst
	// with a slow billing-svc, serial fetches could eat the budget and expire the write, dropping the whole
	// batch's projection (never a message — the connector's enroute row supersedes it). A separate seal budget
	// is a follow-up if this bites at the 8k/s target.
	ctx, cancel := context.WithTimeout(context.Background(), acceptedWriteTimeout)
	defer cancel()

	rows := make([]clickhouse.CDRRow, len(items))
	for i := range items {
		row := items[i].row
		if norm, err := e164.Normalize(row.DestAddr); err == nil {
			row.DestAddr = norm
		}
		// Seal the body into the content column per policy, off the request path. Never fails the row.
		if w.sealer != nil && items[i].policy != cp.ContentOff {
			w.sealer.seal(ctx, &row, items[i].body, items[i].policy)
		}
		rows[i] = row
	}
	if err := w.cdr.InsertBatch(ctx, rows); err != nil {
		w.logger.Error("accepted-row batch write failed", "rows", len(rows), "err", err)
	}
}
