// Package modlrrouter is the return-path router (M4). This step handles the DLR leg: it consumes
// dlr.events, correlates a receipt's smsc_msg_id back to the original message via the dlrmap (§1.11),
// and writes a new versioned CDR row recording the final delivery outcome (delivered/failed/expired).
// A receipt whose mapping is gone (expired or unknown) is logged and counted, never silently dropped.
// The MO leg (mo.inbound) is step-045.
package modlrrouter

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Consumer reads dlr.events. *kafka.Consumer satisfies it.
type Consumer interface {
	Run(ctx context.Context, handle kafka.Handler) error
}

// Resolver looks a receipt's smsc_msg_id up in the dlrmap. *dlrmap.RedisMap satisfies it.
type Resolver interface {
	Get(ctx context.Context, connectorID uuid.UUID, smscMsgID string) (dlrmap.Mapping, bool, error)
}

// CDRWriter records the delivery outcome. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// UnmappedCounter counts receipts with no mapping. A prometheus.Counter satisfies it; New defaults a
// nil counter to a no-op so the hot path never branches on nil.
type UnmappedCounter interface {
	Inc()
}

type noopCounter struct{}

func (noopCounter) Inc() {}

// Deps are the router's collaborators.
type Deps struct {
	Consumer Consumer
	Resolver Resolver
	CDR      CDRWriter
	Unmapped UnmappedCounter
	Tracer   trace.Tracer
	Logger   *slog.Logger
}

// Service is the return-path router.
type Service struct {
	deps Deps
}

// New builds a Service. A nil logger defaults to slog.Default; a nil Unmapped counter to a no-op.
func New(deps Deps) *Service {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Unmapped == nil {
		deps.Unmapped = noopCounter{}
	}
	return &Service{deps: deps}
}

// Run consumes dlr.events until ctx is cancelled.
func (s *Service) Run(ctx context.Context) error {
	return s.deps.Consumer.Run(ctx, s.handler())
}

func (s *Service) handler() kafka.Handler {
	return func(ctx context.Context, rec kafka.Record) error {
		ctx, span := s.deps.Tracer.Start(ctx, "modlrrouter.dlr")
		defer span.End()

		dlr, err := pipeline.DecodeDLR(rec)
		if err != nil {
			// A record we cannot decode is permanently bad: returning an error would redeliver it
			// forever and wedge the partition. Log and commit — it is not thrown away silently.
			s.deps.Logger.ErrorContext(ctx, "modlrrouter: undecodable dlr.events record, skipping", "err", err)
			return nil
		}

		status, ok := terminalStatus(dlr.State)
		if !ok {
			// A non-terminal receipt (enroute/accepted/unknown) has no final outcome to record yet.
			return nil
		}

		m, found, err := s.deps.Resolver.Get(ctx, dlr.ConnectorID, dlr.SMSCMessageID)
		if err != nil {
			// A Redis infrastructure error is transient: do not commit, so the receipt is reprocessed
			// once Redis recovers (the mapping outlives it, TTL 72h). Redis is deliberately kept out of
			// readiness — a blip self-heals here rather than flapping the pod.
			return fmt.Errorf("modlrrouter: resolve %s/%s: %w", dlr.ConnectorID, dlr.SMSCMessageID, err)
		}
		if !found {
			// No mapping: an expired or unknown smsc_msg_id. Count and log — never dropped silently —
			// then commit (there is nothing to correlate; redelivery would not help).
			s.deps.Unmapped.Inc()
			s.deps.Logger.WarnContext(ctx, "modlrrouter: dlr without mapping, counted",
				"connector_id", dlr.ConnectorID, "smsc_message_id", dlr.SMSCMessageID, "state", dlr.State)
			return nil
		}

		row := buildCDRRow(dlr, m, status)
		if err := s.deps.CDR.Insert(ctx, row); err != nil {
			return fmt.Errorf("modlrrouter: write delivered cdr for %s: %w", m.MessageID, err)
		}
		return nil
	}
}

// terminalStatus maps an SMPP message_state to the terminal CDR status it records. ok is false for a
// non-terminal state (enroute/accepted/unknown), which produces no CDR row.
func terminalStatus(state uint8) (clickhouse.Status, bool) {
	switch state {
	case smpp.MessageStateDelivered:
		return clickhouse.StatusDelivered, true
	case smpp.MessageStateExpired:
		return clickhouse.StatusExpired, true
	case smpp.MessageStateDeleted, smpp.MessageStateUndeliverable, smpp.MessageStateRejected:
		return clickhouse.StatusFailed, true
	default:
		return "", false
	}
}

// errorCodeFor returns the cdr.error_code for a terminal status: none for a delivery, the published
// outcome code otherwise. It is a shared contract code (§11.3), never a raw SMSC value.
func errorCodeFor(status clickhouse.Status) *string {
	switch status {
	case clickhouse.StatusExpired:
		code := string(errs.ErrDeliveryExpired)
		return &code
	case clickhouse.StatusFailed:
		code := string(errs.ErrDeliveryFailed)
		return &code
	default:
		return nil
	}
}

// buildCDRRow projects a correlated receipt onto the versioned CDR row that supersedes the enroute
// row. It carries the full message projection from the mapping (so the ReplacingMergeTree FINAL read
// does not blank the descriptive columns) plus the delivery outcome. The version is derived from the
// status rank by the writer.
//
// delivered_at and latency_ms are set ONLY for an actual delivery: a failed or expired message was
// never delivered, so both stay NULL — matching the submit-path failed row (connectorpool.cdrRow),
// which also leaves delivered_at nil.
//
// Terminal-vs-terminal ordering note: the CDR collapses by status rank (delivered=40, failed=expired
// =50), so a duplicate same-state receipt is idempotent, and a non-terminal state never supersedes a
// terminal one. Distinct terminal states are mutually exclusive for a well-behaved message, so the
// rank between delivered and failed/expired is only reachable by an anomalous out-of-order receipt; a
// composite-rank refinement to make delivered sticky is deferred (the CDR schema anticipates widening
// the rank at M4+).
func buildCDRRow(dlr pipeline.DLREvent, m dlrmap.Mapping, status clickhouse.Status) clickhouse.CDRRow {
	connectorID := m.ConnectorID
	row := clickhouse.CDRRow{
		MessageID:    m.MessageID,
		TraceID:      m.TraceID,
		AccountID:    m.AccountID,
		CustomerID:   m.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   m.SourceAddr,
		DestAddr:     m.DestAddr,
		ConnectorID:  &connectorID,
		RouteID:      m.RouteID,
		SubmittedAt:  m.SubmittedAt,
		Status:       status,
		ErrorCode:    errorCodeFor(status),
		SegmentCount: segmentCount(m.SegmentCount),
		Encoding:     clickhouse.EncodingOf(m.Encoding),
		Billed:       false,
	}
	if status == clickhouse.StatusDelivered {
		// delivered_at is the time the connector read the receipt, not the SMSC's own DoneDate: DoneDate
		// is a raw SMPP time string on the SMSC clock (skew, parsing), and at the connector's read latency
		// the difference is small. latency_ms is therefore slightly inflated by the return-path lag — an
		// accepted approximation.
		deliveredAt := dlr.ReceivedAt
		row.DeliveredAt = &deliveredAt
		row.LatencyMs = latencyMs(deliveredAt, m.SubmittedAt)
	}
	return row
}

// latencyMs is the submit-to-delivery latency in milliseconds, clamped to a non-negative uint32. A
// receipt that predates the submit time (clock skew) yields 0 rather than a wrapped huge value.
func latencyMs(delivered, submitted time.Time) *uint32 {
	d := delivered.Sub(submitted).Milliseconds()
	if d < 0 {
		d = 0
	}
	if d > math.MaxUint32 {
		d = math.MaxUint32
	}
	v := uint32(d) //nolint:gosec // clamped to [0, MaxUint32] on the lines above
	return &v
}

// segmentCount narrows the stored segment count to the CDR's uint16, flooring at 1.
func segmentCount(n int) uint16 {
	if n < 1 {
		return 1
	}
	if n > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(n) //nolint:gosec // bounded to [1, MaxUint16] above
}
