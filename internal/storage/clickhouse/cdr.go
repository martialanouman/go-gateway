package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/platform/encoding"
)

// Status is a CDR lifecycle status. It doubles as the source of the ReplacingMergeTree `version`
// via Rank: the CDR never mutates a row, so a status change is a new row and the current status is
// the one with the highest rank (§1.10).
type Status string

// The CDR lifecycle statuses (the full REST MessageStatus set).
const (
	StatusAccepted  Status = "accepted"
	StatusEnroute   Status = "enroute"
	StatusDelivered Status = "delivered"
	StatusFailed    Status = "failed"
	StatusExpired   Status = "expired"
	StatusRejected  Status = "rejected"
	StatusRerouted  Status = "rerouted"
	StatusCancelled Status = "cancelled"
)

// statusRank is the lifecycle rank written to the `version` column. A LATER stage must always
// supersede an earlier one under ReplacingMergeTree, independent of which host wrote it or of clock
// skew (accepted is written by rest-api-svc, enroute by connector-pool-svc). Ranks are spaced to
// leave room for M4+ states. In M2 a message is accepted then exactly one of enroute/rejected/
// failed, so the enroute/rejected tie never actually occurs.
var statusRank = map[Status]uint64{
	StatusAccepted:  10,
	StatusEnroute:   20,
	StatusRejected:  20,
	StatusRerouted:  30,
	StatusDelivered: 40,
	StatusFailed:    50,
	StatusExpired:   50,
	StatusCancelled: 60,
}

// Rank returns the ReplacingMergeTree version for the status: the higher rank wins.
func (s Status) Rank() uint64 { return statusRank[s] }

// Valid reports whether s is a known status.
func (s Status) Valid() bool { _, ok := statusRank[s]; return ok }

// Direction is the CDR direction enum.
type Direction string

// The CDR directions.
const (
	DirectionMT Direction = "mt"
	DirectionMO Direction = "mo"
)

// Encoding is the CDR resolved-encoding enum (never `auto`: that is a REST request value, resolved
// before a CDR row is written).
type Encoding string

// The CDR encodings.
const (
	EncodingGSM7   Encoding = "gsm7"
	EncodingUCS2   Encoding = "ucs2"
	EncodingBinary Encoding = "binary"
)

// EncodingOf projects a requested or resolved encoding string onto the CDR enum. Anything that is
// not an explicit ucs2/binary — including "auto" and any unknown value — resolves to GSM-7, the M2
// default. This is the projection every producer of a CDR row uses (the router, the REST accepted
// row, the connector outcome); the encoding vocabulary itself is shared (internal/platform/encoding)
// so an added value lands in one place.
func EncodingOf(requested string) Encoding {
	switch requested {
	case encoding.UCS2:
		return EncodingUCS2
	case encoding.Binary:
		return EncodingBinary
	default:
		return EncodingGSM7
	}
}

// EncodingOfDataCoding projects an SMPP data_coding byte onto the CDR enum (SMPP v3.4 §5.2.19 /
// GSM 03.38): 0x08 is UCS-2, 0x04 is 8-bit binary, and anything else — including 0x00, the default
// alphabet — is GSM-7. It is the data_coding counterpart of EncodingOf: an SMPP submission carries its
// coding in data_coding rather than the REST encoding enum, so the accepted CDR row derives its
// encoding from here instead of from a bare "auto".
func EncodingOfDataCoding(dataCoding int) Encoding {
	switch dataCoding {
	case 0x08:
		return EncodingUCS2
	case 0x04:
		return EncodingBinary
	default:
		return EncodingGSM7
	}
}

// CDRRow is one CDR row: one lifecycle snapshot of a message. Every status change writes a new row
// carrying the same message_id and immutable submitted_at; the writer derives `version` from
// Status, so callers never set it. content_ciphertext/content_key_id exist for stored content and
// are always nil in M2 — the body is NEVER written to the CDR (invariant a).
type CDRRow struct {
	MessageID          uuid.UUID
	TraceID            uuid.UUID
	AccountID          uuid.UUID
	CustomerID         uuid.UUID
	Direction          Direction
	SourceAddr         string
	DestAddr           string
	OriginalSourceAddr *string
	ConnectorID        *uuid.UUID
	RouteID            *uuid.UUID
	RoutingScriptID    *uuid.UUID
	SubmittedAt        time.Time
	DeliveredAt        *time.Time
	Status             Status
	ErrorCode          *string
	SegmentCount       uint16
	Encoding           Encoding
	ContentCiphertext  *string
	ContentKeyID       *uuid.UUID
	LatencyMs          *uint32
	Billed             bool
	CreditsCharged     *int32
}

// cdrColumns is the explicit column list, in table order, shared by the insert and select
// statements so the two can never drift from the migration.
const cdrColumns = `message_id, trace_id, account_id, customer_id, direction, source_addr, dest_addr,
	original_source_addr, connector_id, route_id, routing_script_id, submitted_at, delivered_at,
	status, error_code, segment_count, encoding, content_ciphertext, content_key_id, latency_ms,
	billed, credits_charged, version`

// CDRWriter appends CDR rows.
type CDRWriter struct {
	conn driver.Conn
}

// NewCDRWriter builds a writer over conn.
func NewCDRWriter(c *Conn) *CDRWriter { return &CDRWriter{conn: c.conn} }

// Insert writes one CDR row, deriving the version from the status rank. The synchronous single-row
// path is what the router and connector use, where the row must be durable before the Kafka offset
// is committed (§7.3); high-volume callers off the request path use InsertBatch instead. It is a
// one-row batch, so the single- and batch-write paths share one implementation.
func (w *CDRWriter) Insert(ctx context.Context, row CDRRow) error {
	return w.InsertBatch(ctx, []CDRRow{row})
}

// InsertBatch writes multiple CDR rows as one batch — a single prepare and a single send instead of
// two round-trips per row — for the accepted-row writer's off-request-path projections. It is
// all-or-nothing like Insert: a bad row aborts the batch, and the (best-effort) caller logs and
// drops it. An empty slice is a no-op.
func (w *CDRWriter) InsertBatch(ctx context.Context, rows []CDRRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO cdr ("+cdrColumns+")")
	if err != nil {
		return fmt.Errorf("clickhouse: prepare cdr batch: %w", err)
	}
	for _, row := range rows {
		if !row.Status.Valid() {
			_ = batch.Abort()
			return fmt.Errorf("clickhouse: unknown CDR status %q", row.Status)
		}
		if err := appendCDR(batch, row); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("clickhouse: append cdr row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse: send cdr batch: %w", err)
	}
	return nil
}

// appendCDR appends one row's columns to a prepared cdr batch, in cdrColumns order, deriving the
// ReplacingMergeTree version from the status rank.
func appendCDR(batch driver.Batch, row CDRRow) error {
	return batch.Append(
		row.MessageID, row.TraceID, row.AccountID, row.CustomerID,
		string(row.Direction), row.SourceAddr, row.DestAddr, row.OriginalSourceAddr,
		row.ConnectorID, row.RouteID, row.RoutingScriptID, row.SubmittedAt, row.DeliveredAt,
		string(row.Status), row.ErrorCode, row.SegmentCount, string(row.Encoding),
		row.ContentCiphertext, row.ContentKeyID, row.LatencyMs, boolToUint8(row.Billed),
		row.CreditsCharged, row.Status.Rank(),
	)
}

// CDRReader reads the current status of a message.
type CDRReader struct {
	conn driver.Conn
}

// NewCDRReader builds a reader over conn.
func NewCDRReader(c *Conn) *CDRReader { return &CDRReader{conn: c.conn} }

// Current returns the latest lifecycle snapshot of a message, scoped to the caller's account. The
// scope is not just an optimization: get-message must 404 a message that is not in the caller's
// account (api/openapi-public.yaml), and filtering on (customer_id, account_id) — the ORDER BY
// prefix — both enforces that and keeps the read off a full scan. FINAL returns the merged
// (highest-version) row; at M2 volume its cost is negligible. found is false when no such row
// exists for the caller.
func (r *CDRReader) Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (CDRRow, bool, error) {
	const query = `SELECT ` + cdrColumns + `
		FROM cdr FINAL
		WHERE customer_id = ? AND account_id = ? AND message_id = ?
		LIMIT 1`

	var (
		out       CDRRow
		direction string
		status    string
		encoding  string
		billed    uint8
		version   uint64
	)
	err := r.conn.QueryRow(ctx, query, customerID, accountID, messageID).Scan(
		&out.MessageID, &out.TraceID, &out.AccountID, &out.CustomerID,
		&direction, &out.SourceAddr, &out.DestAddr, &out.OriginalSourceAddr,
		&out.ConnectorID, &out.RouteID, &out.RoutingScriptID, &out.SubmittedAt, &out.DeliveredAt,
		&status, &out.ErrorCode, &out.SegmentCount, &encoding,
		&out.ContentCiphertext, &out.ContentKeyID, &out.LatencyMs, &billed,
		&out.CreditsCharged, &version,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CDRRow{}, false, nil
		}
		return CDRRow{}, false, fmt.Errorf("clickhouse: read cdr for %s: %w", messageID, err)
	}
	out.Direction = Direction(direction)
	out.Status = Status(status)
	out.Encoding = Encoding(encoding)
	out.Billed = billed != 0
	return out, true, nil
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
