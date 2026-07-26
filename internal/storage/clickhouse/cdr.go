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
	// SegmentSeq is which segment of the message this row is about (step-082c): 0 for a message-level
	// row (the pre-dispatch `accepted` placeholder, a pipeline `rejected`), 1..SegmentCount for a
	// per-segment outcome written by the connector or the DLR path, and 1 for a single-message flow with
	// no pre-dispatch row (an MO, a STOP auto-reply). It joins the sorting key so per-segment rows do not
	// collapse into one; the read path aggregates them back to a single message status.
	SegmentSeq        uint16
	Encoding          Encoding
	ContentCiphertext *string
	ContentKeyID      *uuid.UUID
	LatencyMs         *uint32
	Billed            bool
	CreditsCharged    *int32
}

// cdrColumns is the explicit column list, in table order, shared by the insert and select
// statements so the two can never drift from the migration.
const cdrColumns = `message_id, trace_id, account_id, customer_id, direction, source_addr, dest_addr,
	original_source_addr, connector_id, route_id, routing_script_id, submitted_at, delivered_at,
	status, error_code, segment_count, segment_seq, encoding, content_ciphertext, content_key_id,
	latency_ms, billed, credits_charged, version`

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
		string(row.Status), row.ErrorCode, row.SegmentCount, row.SegmentSeq, string(row.Encoding),
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

// A message is now stored as several CDR rows — a message-level pre-dispatch row (segment_seq 0) plus
// one row per segment (segment_seq 1..N, step-082c). The read path AGGREGATES them back to one status
// per message, in SQL so get-one and the listing share one rule. It is a two-level collapse, NOT
// FINAL: the inner query collapses each (message_id, segment_seq) to its highest ReplacingMergeTree
// version via argMax (correct before a background merge, cheaper than FINAL), and the outer query
// folds the per-segment rows into a single synthetic row per message.
//
// cdrAggInnerCols is the inner per-segment collapse: every descriptive column taken from the
// highest-version row of each (submitted_at, message_id, segment_seq). customer_id, account_id and
// direction are immutable per message, so they are GROUP-BY keys (plain columns) rather than argMax
// aggregates — that also keeps them addressable by the WHERE clause, which filters on them (aliasing
// an aggregate to a filtered column's own name is a ClickHouse error).
const cdrAggInnerCols = `customer_id, account_id, direction, submitted_at, message_id, segment_seq,
	argMax(trace_id, version) AS trace_id,
	argMax(source_addr, version) AS source_addr, argMax(dest_addr, version) AS dest_addr,
	argMax(original_source_addr, version) AS original_source_addr,
	argMax(connector_id, version) AS connector_id, argMax(route_id, version) AS route_id,
	argMax(routing_script_id, version) AS routing_script_id,
	argMax(delivered_at, version) AS delivered_at, argMax(status, version) AS status,
	argMax(error_code, version) AS error_code, argMax(segment_count, version) AS segment_count,
	argMax(encoding, version) AS encoding, argMax(content_ciphertext, version) AS content_ciphertext,
	argMax(content_key_id, version) AS content_key_id, argMax(latency_ms, version) AS latency_ms,
	argMax(billed, version) AS billed, argMax(credits_charged, version) AS credits_charged`

// cdrDispatched is the predicate for "this collapsed row is a dispatched segment outcome, not the
// message-level placeholder": a status other than accepted/rejected. It is used INSTEAD of
// segment_seq > 0 so a terminal row that predates the per-segment key (step-082c added it, and existing
// rows took segment_seq 0) still counts as a segment — otherwise every historical delivered/enroute/
// failed message would fold to `accepted`. A dispatched status is only ever written at segment_seq >= 1
// under the new model, so the two agree for new data.
const cdrDispatched = `status NOT IN ('accepted', 'rejected')`

// cdrAggMessageCols is the middle level: it reduces a message's per-segment rows to one row of PLAIN
// per-message aggregates (counts and maxima). Keeping these separate from the status/delivered_at
// decision matters — ClickHouse forbids nesting one aggregate inside another, so the multiIf that reads
// them lives one level up (cdrAggOuterCols), over these as ordinary columns. dispatched_total is taken
// from the dispatched segments only, never the placeholder row whose segment_count is a provisional 1.
const cdrAggMessageCols = `message_id, submitted_at,
	any(trace_id) AS trace_id, any(account_id) AS account_id, any(customer_id) AS customer_id,
	any(direction) AS direction, any(source_addr) AS source_addr, any(dest_addr) AS dest_addr,
	any(original_source_addr) AS original_source_addr,
	anyIf(connector_id, ` + cdrDispatched + `) AS connector_id,
	anyIf(route_id, ` + cdrDispatched + `) AS route_id,
	anyIf(routing_script_id, ` + cdrDispatched + `) AS routing_script_id,
	any(encoding) AS encoding, any(content_ciphertext) AS content_ciphertext,
	any(content_key_id) AS content_key_id, maxIf(latency_ms, ` + cdrDispatched + `) AS latency_ms,
	max(billed) AS billed, any(credits_charged) AS credits_charged,
	maxIf(segment_count, ` + cdrDispatched + `) AS dispatched_total,
	countIf(status = 'delivered') AS delivered_segs,
	countIf(status = 'cancelled') AS cancelled_cnt,
	countIf(status = 'failed') AS failed_cnt,
	countIf(status = 'expired') AS expired_cnt,
	countIf(status = 'rejected') AS rejected_cnt,
	countIf(` + cdrDispatched + `) AS dispatched_cnt,
	maxIf(delivered_at, status = 'delivered') AS delivered_at_max,
	argMinIf(error_code, segment_seq, status IN ('failed', 'expired', 'rejected')) AS error_code`

// cdrAggOuterCols is the top level: it turns the per-message aggregates into the message's status with
// pure scalar functions (no aggregates), in cdrColumns order so scanCDRRow reads the synthetic row
// exactly like a raw one. Status precedence (spec §6.6): rejected (a message-level reject) → cancelled
// (any segment) → failed (any segment hard-failed) → expired (a segment expired, none hard-failed) →
// delivered (ALL segments delivered) → enroute (any segment dispatched) → accepted (pre-dispatch). Any
// failure/expiry outranks a partial delivery, so a message is never shown delivered while a part failed.
// delivered_at surfaces only when the whole message is delivered; segment_count is the dispatched total
// (a provisional 1 pre-dispatch).
const cdrAggOuterCols = `message_id, trace_id, account_id, customer_id, direction, source_addr, dest_addr,
	original_source_addr, connector_id, route_id, routing_script_id, submitted_at,
	if(dispatched_total > 0 AND delivered_segs = dispatched_total, delivered_at_max, NULL) AS delivered_at,
	multiIf(
		rejected_cnt > 0, 'rejected',
		cancelled_cnt > 0, 'cancelled',
		failed_cnt > 0, 'failed',
		expired_cnt > 0, 'expired',
		dispatched_total > 0 AND delivered_segs = dispatched_total, 'delivered',
		dispatched_cnt > 0, 'enroute',
		'accepted') AS status,
	error_code, greatest(dispatched_total, 1) AS segment_count, toUInt16(0) AS segment_seq,
	encoding, content_ciphertext, content_key_id, latency_ms, billed, credits_charged, toUInt64(0) AS version`

// cdrAggregate assembles the three levels (per-segment collapse → per-message aggregates → status
// decision) around an inner WHERE tail (added filters/keyset, bound positionally by the caller). The
// outer SELECT projects cdrColumns so scanCDRRow reads the synthetic row exactly like a raw one.
func cdrAggregate(innerWhereTail string) string {
	return `SELECT ` + cdrAggOuterCols + ` FROM (
		SELECT ` + cdrAggMessageCols + ` FROM (
			SELECT ` + cdrAggInnerCols + ` FROM cdr
			WHERE customer_id = ? AND account_id = ?` + innerWhereTail + `
			GROUP BY customer_id, account_id, direction, submitted_at, message_id, segment_seq
		) GROUP BY submitted_at, message_id
	)`
}

// Current returns the aggregated lifecycle snapshot of a message, scoped to the caller's account. The
// scope is not just an optimization: get-message must 404 a message that is not in the caller's
// account (api/openapi-public.yaml), and filtering on (customer_id, account_id) — the sorting-key
// prefix — both enforces that and keeps the read off a full scan. found is false when no such row
// exists for the caller.
func (r *CDRReader) Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (CDRRow, bool, error) {
	query := cdrAggregate(` AND message_id = ?`) + ` LIMIT 1`

	out, err := scanCDRRow(r.conn.QueryRow(ctx, query, customerID, accountID, messageID).Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CDRRow{}, false, nil
		}
		return CDRRow{}, false, fmt.Errorf("clickhouse: read cdr for %s: %w", messageID, err)
	}
	return out, true, nil
}

// scanCDRRow scans one CDR row via a Scan function — driver.Row and driver.Rows share the
// `Scan(dest ...any) error` signature, so Current and List reuse it and stay in lockstep with
// cdrColumns. The enum columns come back as strings and the boolean as uint8; they are converted
// here. The `version` column is read (it closes cdrColumns) but not surfaced: it is an internal
// ReplacingMergeTree artefact, and FINAL already yields the highest-version row.
func scanCDRRow(scan func(dest ...any) error) (CDRRow, error) {
	var (
		out       CDRRow
		direction string
		status    string
		encoding  string
		billed    uint8
		version   uint64
	)
	if err := scan(
		&out.MessageID, &out.TraceID, &out.AccountID, &out.CustomerID,
		&direction, &out.SourceAddr, &out.DestAddr, &out.OriginalSourceAddr,
		&out.ConnectorID, &out.RouteID, &out.RoutingScriptID, &out.SubmittedAt, &out.DeliveredAt,
		&status, &out.ErrorCode, &out.SegmentCount, &out.SegmentSeq, &encoding,
		&out.ContentCiphertext, &out.ContentKeyID, &out.LatencyMs, &billed,
		&out.CreditsCharged, &version,
	); err != nil {
		return CDRRow{}, err
	}
	out.Direction = Direction(direction)
	out.Status = Status(status)
	out.Encoding = Encoding(encoding)
	out.Billed = billed != 0
	return out, nil
}

// CDRKey is a keyset pagination position: the (submitted_at, message_id) of a page's last row. Both
// are immutable per message and form the tail of the CDR sorting key, so together they order rows
// deterministically — no two rows share a key, so paging never skips or repeats a message.
type CDRKey struct {
	SubmittedAt time.Time
	MessageID   uuid.UUID
}

// CDRListFilter narrows a CDR listing. Every field is optional (a nil pointer means "no
// constraint"). FromDate is an inclusive lower bound and ToDate an exclusive upper bound on
// submitted_at. After is the keyset cursor: only rows strictly before it in the
// (submitted_at DESC, message_id DESC) order are returned.
type CDRListFilter struct {
	Status    *Status
	Direction *Direction
	FromDate  *time.Time
	ToDate    *time.Time
	After     *CDRKey
}

// List returns a page of a caller's MESSAGES (aggregated across their segments), newest first, scoped
// to (customer_id, account_id) — the sorting-key prefix, which both enforces account isolation and
// keeps the read off a full scan. The date, direction and keyset filters are on immutable per-message
// columns, so they narrow the INNER scan (before aggregation); the status filter is on the AGGREGATED
// status, so it applies OUTSIDE the aggregation. Rows come back in (submitted_at DESC, message_id DESC)
// order; callers request limit+1 to detect whether a further page exists.
func (r *CDRReader) List(ctx context.Context, customerID, accountID uuid.UUID, f CDRListFilter, limit int) ([]CDRRow, error) {
	inner := ``
	args := []any{customerID, accountID}

	if f.FromDate != nil {
		inner += ` AND submitted_at >= ?`
		args = append(args, *f.FromDate)
	}
	if f.ToDate != nil {
		inner += ` AND submitted_at < ?`
		args = append(args, *f.ToDate)
	}
	if f.Direction != nil {
		inner += ` AND direction = ?`
		args = append(args, string(*f.Direction))
	}
	if f.After != nil {
		// Keyset comparison on an integer-millisecond expression rather than the raw DateTime64
		// column: binding a time.Time lets the server infer a coarser precision, which silently drops
		// the whole page once many messages share a submitted_at millisecond (routine at peak
		// throughput). The expanded OR form (instead of a tuple literal) keeps the comparison explicit;
		// message_id — immutable and unique — is the deterministic tiebreaker, matching the ORDER BY.
		inner += ` AND (toUnixTimestamp64Milli(submitted_at) < ? OR (toUnixTimestamp64Milli(submitted_at) = ? AND message_id < ?))`
		ms := f.After.SubmittedAt.UnixMilli()
		args = append(args, ms, ms, f.After.MessageID)
	}

	// Wrap the aggregate once more so the status filter can apply to the AGGREGATED status (a computed
	// column of the subquery, which a WHERE at the same level as its own SELECT could not reference) and
	// the page is ordered and cut on the message-level keyset.
	query := `SELECT ` + cdrColumns + ` FROM (` + cdrAggregate(inner) + `)`
	if f.Status != nil {
		query += ` WHERE status = ?`
		args = append(args, string(*f.Status))
	}
	query += `
		ORDER BY submitted_at DESC, message_id DESC
		LIMIT ?`
	args = append(args, limit)

	rows, err := r.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: list cdr: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]CDRRow, 0, limit)
	for rows.Next() {
		row, err := scanCDRRow(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: scan cdr row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: list cdr: %w", err)
	}
	return out, nil
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
