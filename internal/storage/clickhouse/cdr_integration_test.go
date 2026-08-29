package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

func TestCDRVersionedReadWrite(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	messageID := uuid.New()
	traceID := uuid.New()
	accountID := uuid.New()
	customerID := uuid.New()
	// submitted_at is immutable across a message's rows; both the accepted and enroute rows carry it.
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)

	accepted := clickhouse.CDRRow{
		MessageID:    messageID,
		TraceID:      traceID,
		AccountID:    accountID,
		CustomerID:   customerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   "GATEWAY",
		DestAddr:     "22507000000",
		SubmittedAt:  submittedAt,
		Status:       clickhouse.StatusAccepted,
		SegmentCount: 1,
		Encoding:     clickhouse.EncodingGSM7,
	}
	if err := writer.Insert(ctx, accepted); err != nil {
		t.Fatalf("insert accepted: %v", err)
	}

	// After only the accepted row, the current status is accepted (no 404 window, §1.10).
	got, found, err := reader.Current(ctx, customerID, accountID, messageID)
	if err != nil {
		t.Fatalf("read after accepted: %v", err)
	}
	if !found {
		t.Fatal("accepted message not found")
	}
	if got.Status != clickhouse.StatusAccepted {
		t.Fatalf("status after accepted: got %q want accepted", got.Status)
	}

	// The enroute row (higher lifecycle rank) supersedes accepted, even though it is a new row.
	connectorID := uuid.New()
	enroute := accepted
	enroute.Status = clickhouse.StatusEnroute
	enroute.SegmentSeq = 1 // a dispatched segment (the accepted placeholder stays segment_seq 0)
	enroute.ConnectorID = &connectorID
	if err := writer.Insert(ctx, enroute); err != nil {
		t.Fatalf("insert enroute: %v", err)
	}

	got, found, err = reader.Current(ctx, customerID, accountID, messageID)
	if err != nil {
		t.Fatalf("read after enroute: %v", err)
	}
	if !found {
		t.Fatal("message not found after enroute")
	}
	if got.Status != clickhouse.StatusEnroute {
		t.Fatalf("status after enroute: got %q want enroute (ReplacingMergeTree should keep the higher version)", got.Status)
	}
	if got.ConnectorID == nil || *got.ConnectorID != connectorID {
		t.Fatalf("connector_id not carried on the enroute row: %v", got.ConnectorID)
	}
	if !got.SubmittedAt.Equal(submittedAt) {
		t.Errorf("submitted_at should be immutable: got %s want %s", got.SubmittedAt, submittedAt)
	}
}

// TestCDRInsertBatch writes several accepted rows in one batch (the accepted-writer's hot path) and
// reads each back, proving the multi-row prepare+send lands every row.
func TestCDRInsertBatch(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	customerID := uuid.New()
	accountID := uuid.New()
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)

	const n = 5
	ids := make([]uuid.UUID, n)
	rows := make([]clickhouse.CDRRow, n)
	for i := range rows {
		ids[i] = uuid.New()
		rows[i] = clickhouse.CDRRow{
			MessageID:    ids[i],
			TraceID:      uuid.New(),
			AccountID:    accountID,
			CustomerID:   customerID,
			Direction:    clickhouse.DirectionMT,
			SourceAddr:   "GATEWAY",
			DestAddr:     "2250700000000",
			SubmittedAt:  submittedAt,
			Status:       clickhouse.StatusAccepted,
			SegmentCount: 1,
			Encoding:     clickhouse.EncodingGSM7,
		}
	}

	if err := writer.InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	for i, id := range ids {
		got, found, err := reader.Current(ctx, customerID, accountID, id)
		if err != nil {
			t.Fatalf("read row %d: %v", i, err)
		}
		if !found {
			t.Fatalf("row %d (%s) not found after InsertBatch", i, id)
		}
		if got.Status != clickhouse.StatusAccepted {
			t.Errorf("row %d status: got %q want accepted", i, got.Status)
		}
	}

	// An empty batch is a no-op, not an error.
	if err := writer.InsertBatch(ctx, nil); err != nil {
		t.Errorf("InsertBatch(nil) = %v, want nil (no-op)", err)
	}
}

func TestCDRCurrentScopedToAccount(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	messageID := uuid.New()
	owner := uuid.New()
	ownerCustomer := uuid.New()
	if err := writer.Insert(ctx, clickhouse.CDRRow{
		MessageID: messageID, TraceID: uuid.New(), AccountID: owner, CustomerID: ownerCustomer,
		Direction: clickhouse.DirectionMT, DestAddr: "22507000000", SubmittedAt: time.Now().UTC(),
		Status: clickhouse.StatusAccepted, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// A different account querying the same message id must not find it (get-message scoping / 404).
	_, found, err := reader.Current(ctx, uuid.New(), uuid.New(), messageID)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if found {
		t.Fatal("message leaked across account scope")
	}

	// The owner finds it.
	_, found, err = reader.Current(ctx, ownerCustomer, owner, messageID)
	if err != nil {
		t.Fatalf("read owner: %v", err)
	}
	if !found {
		t.Fatal("owner could not read its own message")
	}
}

// listAll pages through every row matching f, driving the keyset cursor exactly as the REST handler
// does: fetch pageSize+1, trim, advance After to the last kept row. It proves the storage-level
// pagination is self-consistent independent of the HTTP layer.
func listAll(ctx context.Context, t *testing.T, reader *clickhouse.CDRReader, customerID, accountID uuid.UUID, f clickhouse.CDRListFilter, pageSize int) []clickhouse.CDRRow {
	t.Helper()
	var all []clickhouse.CDRRow
	after := f.After
	for {
		f.After = after
		rows, err := reader.List(ctx, customerID, accountID, f, pageSize+1)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		hasMore := len(rows) > pageSize
		if hasMore {
			rows = rows[:pageSize]
		}
		all = append(all, rows...)
		if !hasMore || len(rows) == 0 {
			break
		}
		last := rows[len(rows)-1]
		after = &clickhouse.CDRKey{SubmittedAt: last.SubmittedAt, MessageID: last.MessageID}
	}
	return all
}

// TestCDRListPaginationCoversAllRows inserts several messages and pages through them with a page
// size smaller than the total. Every message must appear exactly once, newest first, with no
// duplicate or gap across page boundaries.
func TestCDRListPaginationCoversAllRows(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	customerID := uuid.New()
	accountID := uuid.New()
	base := time.Now().UTC().Truncate(time.Millisecond)

	const n = 5
	want := make([]uuid.UUID, n) // newest-first order
	rows := make([]clickhouse.CDRRow, n)
	for i := range rows {
		id := uuid.New()
		// Stagger submitted_at so the DESC order is deterministic: row 0 is the oldest.
		rows[i] = clickhouse.CDRRow{
			MessageID: id, TraceID: uuid.New(), AccountID: accountID, CustomerID: customerID,
			Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "2250700000000",
			SubmittedAt: base.Add(time.Duration(i) * time.Second),
			Status:      clickhouse.StatusEnroute, SegmentCount: 1, SegmentSeq: 1, Encoding: clickhouse.EncodingGSM7,
		}
		want[n-1-i] = id // newest (largest submitted_at) first
	}
	if err := writer.InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	got := listAll(ctx, t, reader, customerID, accountID, clickhouse.CDRListFilter{}, 2)
	if len(got) != n {
		t.Fatalf("paged rows: got %d want %d", len(got), n)
	}
	seen := make(map[uuid.UUID]int, n)
	for i, row := range got {
		seen[row.MessageID]++
		if row.MessageID != want[i] {
			t.Errorf("position %d: got %s want %s (newest first)", i, row.MessageID, want[i])
		}
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("message %s appeared %d times across pages", id, count)
		}
	}
}

// TestCDRListPaginationTiesOnSameMillisecond guards the keyset tiebreaker: at peak throughput many
// messages share a submitted_at millisecond, so pagination must fall through to message_id and still
// cover every row exactly once — no page silently dropped, no duplicate.
func TestCDRListPaginationTiesOnSameMillisecond(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	customerID := uuid.New()
	accountID := uuid.New()
	same := time.Now().UTC().Truncate(time.Millisecond)

	const n = 7
	rows := make([]clickhouse.CDRRow, n)
	ids := make(map[uuid.UUID]bool, n)
	for i := range rows {
		id := uuid.New()
		ids[id] = true
		rows[i] = clickhouse.CDRRow{
			MessageID: id, TraceID: uuid.New(), AccountID: accountID, CustomerID: customerID,
			Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "2250700000000",
			SubmittedAt: same, Status: clickhouse.StatusEnroute, SegmentCount: 1, SegmentSeq: 1, Encoding: clickhouse.EncodingGSM7,
		}
	}
	if err := writer.InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	got := listAll(ctx, t, reader, customerID, accountID, clickhouse.CDRListFilter{}, 2)
	if len(got) != n {
		t.Fatalf("tie pagination: got %d rows want %d (a page was dropped or duplicated)", len(got), n)
	}
	seen := make(map[uuid.UUID]int, n)
	for _, row := range got {
		seen[row.MessageID]++
	}
	for id := range ids {
		if seen[id] != 1 {
			t.Errorf("message %s appeared %d times across pages (want exactly 1)", id, seen[id])
		}
	}
}

func TestCDRListScopedToAccount(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	ownerCustomer := uuid.New()
	owner := uuid.New()
	if err := writer.Insert(ctx, clickhouse.CDRRow{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: owner, CustomerID: ownerCustomer,
		Direction: clickhouse.DirectionMT, DestAddr: "22507000000", SubmittedAt: time.Now().UTC().Truncate(time.Millisecond),
		Status: clickhouse.StatusEnroute, SegmentCount: 1, SegmentSeq: 1, Encoding: clickhouse.EncodingGSM7,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// A different account sees nothing; the owner sees its one row.
	if other := listAll(ctx, t, reader, uuid.New(), uuid.New(), clickhouse.CDRListFilter{}, 10); len(other) != 0 {
		t.Errorf("list leaked %d rows across account scope", len(other))
	}
	if own := listAll(ctx, t, reader, ownerCustomer, owner, clickhouse.CDRListFilter{}, 10); len(own) != 1 {
		t.Errorf("owner list: got %d rows want 1", len(own))
	}
}

// TestCDRListStatusIsLatestVersion proves a listing reflects the merged (highest-version) status,
// and that filtering by status is evaluated on that merged row — not on a superseded one.
func TestCDRListStatusIsLatestVersion(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	customerID := uuid.New()
	accountID := uuid.New()
	messageID := uuid.New()
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)

	accepted := clickhouse.CDRRow{
		MessageID: messageID, TraceID: uuid.New(), AccountID: accountID, CustomerID: customerID,
		Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "2250700000000",
		SubmittedAt: submittedAt, Status: clickhouse.StatusAccepted, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
	}
	enroute := accepted
	enroute.Status = clickhouse.StatusEnroute
	enroute.SegmentSeq = 1 // a dispatched segment
	if err := writer.InsertBatch(ctx, []clickhouse.CDRRow{accepted, enroute}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Unfiltered: the merged row's status is enroute.
	all := listAll(ctx, t, reader, customerID, accountID, clickhouse.CDRListFilter{}, 10)
	if len(all) != 1 || all[0].Status != clickhouse.StatusEnroute {
		t.Fatalf("merged status: got %+v want a single enroute row", all)
	}

	// Filtering by the current status finds it; filtering by the superseded status does not.
	enrouteStatus := clickhouse.StatusEnroute
	if hit := listAll(ctx, t, reader, customerID, accountID, clickhouse.CDRListFilter{Status: &enrouteStatus}, 10); len(hit) != 1 {
		t.Errorf("status=enroute filter: got %d rows want 1", len(hit))
	}
	acceptedStatus := clickhouse.StatusAccepted
	if miss := listAll(ctx, t, reader, customerID, accountID, clickhouse.CDRListFilter{Status: &acceptedStatus}, 10); len(miss) != 0 {
		t.Errorf("status=accepted filter should not match a superseded row: got %d rows", len(miss))
	}
}

// TestCDRListFilters checks the direction and submitted_at range filters narrow the result set.
func TestCDRListFilters(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	customerID := uuid.New()
	accountID := uuid.New()
	base := time.Now().UTC().Truncate(time.Millisecond)

	mk := func(dir clickhouse.Direction, at time.Time) clickhouse.CDRRow {
		return clickhouse.CDRRow{
			MessageID: uuid.New(), TraceID: uuid.New(), AccountID: accountID, CustomerID: customerID,
			Direction: dir, SourceAddr: "GATEWAY", DestAddr: "2250700000000",
			SubmittedAt: at, Status: clickhouse.StatusEnroute, SegmentCount: 1, SegmentSeq: 1, Encoding: clickhouse.EncodingGSM7,
		}
	}
	// Two MT (t0, t2) and one MO (t1).
	if err := writer.InsertBatch(ctx, []clickhouse.CDRRow{
		mk(clickhouse.DirectionMT, base),
		mk(clickhouse.DirectionMO, base.Add(time.Second)),
		mk(clickhouse.DirectionMT, base.Add(2*time.Second)),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	mt := clickhouse.DirectionMT
	if got := listAll(ctx, t, reader, customerID, accountID, clickhouse.CDRListFilter{Direction: &mt}, 10); len(got) != 2 {
		t.Errorf("direction=mt filter: got %d rows want 2", len(got))
	}

	// from_date inclusive at t1, to_date exclusive at t2 → only the t1 (MO) row.
	from := base.Add(time.Second)
	to := base.Add(2 * time.Second)
	got := listAll(ctx, t, reader, customerID, accountID, clickhouse.CDRListFilter{FromDate: &from, ToDate: &to}, 10)
	if len(got) != 1 || got[0].Direction != clickhouse.DirectionMO {
		t.Errorf("date range filter: got %+v want the single t1 MO row", got)
	}
}

// TestCDRAggregatesSegmentsToMessageStatus is the step-082c core: a multi-segment message is stored as
// N per-segment rows (plus a message-level placeholder) and the read path folds them back to ONE
// status, per the spec §6.6 precedence — delivered only when EVERY segment is delivered, failed as soon
// as one fails, and the dispatched total (not the placeholder's provisional 1) as segment_count.
func TestCDRAggregatesSegmentsToMessageStatus(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()
	customerID := uuid.New()
	accountID := uuid.New()

	// seg builds one CDR row for a message: seq 0 is the message-level placeholder, seq >= 1 a segment.
	// A delivered segment carries its delivered_at, exactly as the DLR path writes it.
	seg := func(msgID uuid.UUID, at time.Time, status clickhouse.Status, seq, total uint16) clickhouse.CDRRow {
		row := clickhouse.CDRRow{
			MessageID: msgID, TraceID: uuid.New(), AccountID: accountID, CustomerID: customerID,
			Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "2250700000000",
			SubmittedAt: at, Status: status, SegmentCount: total, SegmentSeq: seq, Encoding: clickhouse.EncodingGSM7,
		}
		if status == clickhouse.StatusDelivered {
			deliveredAt := at.Add(time.Second)
			row.DeliveredAt = &deliveredAt
		}
		return row
	}
	statusOf := func(t *testing.T, msgID uuid.UUID) clickhouse.CDRRow {
		t.Helper()
		row, found, err := reader.Current(ctx, customerID, accountID, msgID)
		if err != nil || !found {
			t.Fatalf("read %s: found=%v err=%v", msgID, found, err)
		}
		return row
	}

	t.Run("delivered only when every segment is delivered", func(t *testing.T) {
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		// accepted placeholder + 3 segments, all delivered.
		rows := []clickhouse.CDRRow{
			seg(id, at, clickhouse.StatusAccepted, 0, 1),
			seg(id, at, clickhouse.StatusEnroute, 1, 3), seg(id, at, clickhouse.StatusDelivered, 1, 3),
			seg(id, at, clickhouse.StatusEnroute, 2, 3), seg(id, at, clickhouse.StatusDelivered, 2, 3),
			seg(id, at, clickhouse.StatusEnroute, 3, 3), seg(id, at, clickhouse.StatusDelivered, 3, 3),
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("insert: %v", err)
		}
		got := statusOf(t, id)
		if got.Status != clickhouse.StatusDelivered {
			t.Errorf("status = %q, want delivered (all 3 segments delivered)", got.Status)
		}
		if got.SegmentCount != 3 {
			t.Errorf("segment_count = %d, want the dispatched total 3 (not the placeholder's 1)", got.SegmentCount)
		}
		if got.DeliveredAt == nil {
			t.Error("delivered_at should be set once the whole message is delivered")
		}
	})

	t.Run("one segment still enroute keeps the message enroute", func(t *testing.T) {
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		rows := []clickhouse.CDRRow{
			seg(id, at, clickhouse.StatusEnroute, 1, 2), seg(id, at, clickhouse.StatusDelivered, 1, 2),
			seg(id, at, clickhouse.StatusEnroute, 2, 2), // segment 2 not delivered yet
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if got := statusOf(t, id); got.Status != clickhouse.StatusEnroute {
			t.Errorf("status = %q, want enroute (segment 2 not delivered)", got.Status)
		}
	})

	t.Run("any failed segment fails the message", func(t *testing.T) {
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		rows := []clickhouse.CDRRow{
			seg(id, at, clickhouse.StatusEnroute, 1, 2), seg(id, at, clickhouse.StatusDelivered, 1, 2),
			seg(id, at, clickhouse.StatusEnroute, 2, 2), seg(id, at, clickhouse.StatusFailed, 2, 2),
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if got := statusOf(t, id); got.Status != clickhouse.StatusFailed {
			t.Errorf("status = %q, want failed (segment 2 failed even though segment 1 delivered)", got.Status)
		}
	})

	t.Run("pre-dispatch shows accepted with a provisional count", func(t *testing.T) {
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		if err := writer.Insert(ctx, seg(id, at, clickhouse.StatusAccepted, 0, 1)); err != nil {
			t.Fatalf("insert: %v", err)
		}
		got := statusOf(t, id)
		if got.Status != clickhouse.StatusAccepted {
			t.Errorf("status = %q, want accepted (no segment dispatched yet)", got.Status)
		}
		if got.SegmentCount != 1 {
			t.Errorf("segment_count = %d, want the provisional 1 pre-dispatch", got.SegmentCount)
		}
	})

	t.Run("a message-level rejection wins and keeps its error_code", func(t *testing.T) {
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		rejected := seg(id, at, clickhouse.StatusRejected, 0, 1)
		code := "invalid_destination"
		rejected.ErrorCode = &code
		if err := writer.Insert(ctx, rejected); err != nil {
			t.Fatalf("insert: %v", err)
		}
		got := statusOf(t, id)
		if got.Status != clickhouse.StatusRejected {
			t.Errorf("status = %q, want rejected", got.Status)
		}
		if got.ErrorCode == nil || *got.ErrorCode != code {
			t.Errorf("error_code = %v, want %q preserved through aggregation", got.ErrorCode, code)
		}
	})

	t.Run("a segment expiry surfaces as expired, distinct from failed", func(t *testing.T) {
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		rows := []clickhouse.CDRRow{
			seg(id, at, clickhouse.StatusEnroute, 1, 2), seg(id, at, clickhouse.StatusDelivered, 1, 2),
			seg(id, at, clickhouse.StatusEnroute, 2, 2), seg(id, at, clickhouse.StatusExpired, 2, 2),
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if got := statusOf(t, id); got.Status != clickhouse.StatusExpired {
			t.Errorf("status = %q, want expired (a segment expired, none hard-failed)", got.Status)
		}
	})

	t.Run("a pre-per-segment terminal row (segment_seq 0) still aggregates", func(t *testing.T) {
		// A message dispatched before step-082c added segment_seq: its terminal rows carry segment_seq 0
		// (the column's zero). The read path must still report delivered, not fold it to accepted.
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		rows := []clickhouse.CDRRow{
			seg(id, at, clickhouse.StatusAccepted, 0, 1),
			seg(id, at, clickhouse.StatusEnroute, 0, 1),
			seg(id, at, clickhouse.StatusDelivered, 0, 1),
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if got := statusOf(t, id); got.Status != clickhouse.StatusDelivered {
			t.Errorf("status = %q, want delivered (a legacy seq-0 terminal row must not fold to accepted)", got.Status)
		}
	})

	t.Run("a redelivered segment does not double-count", func(t *testing.T) {
		at := time.Now().UTC().Truncate(time.Millisecond)
		id := uuid.New()
		// At-least-once: segment 1 of a 2-segment message is delivered TWICE (same key, same version).
		// It must collapse — delivered_segs stays 1, so the message is NOT falsely delivered.
		rows := []clickhouse.CDRRow{
			seg(id, at, clickhouse.StatusDelivered, 1, 2),
			seg(id, at, clickhouse.StatusDelivered, 1, 2), // duplicate of segment 1
			seg(id, at, clickhouse.StatusEnroute, 2, 2),
		}
		if err := writer.InsertBatch(ctx, rows); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if got := statusOf(t, id); got.Status != clickhouse.StatusEnroute {
			t.Errorf("status = %q, want enroute — a duplicated segment 1 must not count as two deliveries", got.Status)
		}
	})
}

// TestCDRByMessageIDReturnsContent validates the audited content read's core assumption (step-163): the body
// lives only on the message-level accepted row (segment_seq 0); the per-segment enroute rows carry NULL
// content. ByMessageID (no customer/account filter) must still return the accepted row's content_ciphertext
// and content_key_id — the any(...) aggregation skips the segments' NULLs.
func TestCDRByMessageIDReturnsContent(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	messageID, accountID, customerID, keyID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)
	envelope := "\x01sealed-envelope-bytes"

	// The message-level accepted row carries the sealed content.
	accepted := clickhouse.CDRRow{
		MessageID: messageID, AccountID: accountID, CustomerID: customerID, Direction: clickhouse.DirectionMT,
		SourceAddr: "GATEWAY", DestAddr: "22507000000", SubmittedAt: submittedAt,
		Status: clickhouse.StatusAccepted, SegmentCount: 2, Encoding: clickhouse.EncodingGSM7,
		ContentCiphertext: &envelope, ContentKeyID: &keyID,
	}
	if err := writer.Insert(ctx, accepted); err != nil {
		t.Fatalf("insert accepted: %v", err)
	}
	// Two per-segment enroute rows with NO content.
	for seq := 1; seq <= 2; seq++ {
		seg := accepted
		seg.Status = clickhouse.StatusEnroute
		seg.SegmentSeq = uint16(seq)
		seg.ContentCiphertext = nil
		seg.ContentKeyID = nil
		if err := writer.Insert(ctx, seg); err != nil {
			t.Fatalf("insert segment %d: %v", seq, err)
		}
	}

	got, found, err := reader.ByMessageID(ctx, messageID)
	if err != nil {
		t.Fatalf("ByMessageID: %v", err)
	}
	if !found {
		t.Fatal("message not found by id")
	}
	if got.CustomerID != customerID {
		t.Errorf("customer_id = %s, want %s", got.CustomerID, customerID)
	}
	if got.ContentCiphertext == nil || *got.ContentCiphertext != envelope {
		t.Errorf("content_ciphertext = %v, want the accepted row's envelope (segments' NULLs must not win)", got.ContentCiphertext)
	}
	if got.ContentKeyID == nil || *got.ContentKeyID != keyID {
		t.Errorf("content_key_id = %v, want %s", got.ContentKeyID, keyID)
	}

	// An unknown message id is a clean not-found.
	if _, found, err := reader.ByMessageID(ctx, uuid.New()); err != nil || found {
		t.Errorf("unknown message: found=%v err=%v, want (false, nil)", found, err)
	}
}

// TestCancelledOutranksFailedInTheAggregate pins the precedence the replay guard of step-240 rests on.
//
// A message cancelled before dispatch and then dead-lettered carries BOTH rows: `cancelled` written by
// the Canceller at segment 0, and one `failed` per segment written by the pool when it parked the
// message. The replay refuses to put a cancelled message back on the wire, and it recognises one by
// asking this reader for the current status — so if `failed` ever won, the guard would silently open
// and nothing in the Go tests would notice: they all answer through a fake reader that never touches
// this SQL.
//
// Two precedences are at work and both are asserted here. Within a segment, ReplacingMergeTree keeps
// the highest `version` (cancelled 60 over failed 50). Across segments, the message-level aggregate
// applies its own fixed order — rejected, cancelled, failed, expired, delivered — which is why a single
// cancelled segment must outrank several failed ones.
func TestCancelledOutranksFailedInTheAggregate(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	messageID, accountID, customerID := uuid.New(), uuid.New(), uuid.New()
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)
	row := func(status clickhouse.Status, seq uint16) clickhouse.CDRRow {
		return clickhouse.CDRRow{
			MessageID: messageID, TraceID: uuid.New(), AccountID: accountID, CustomerID: customerID,
			Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "22507000000",
			SubmittedAt: submittedAt, Status: status, SegmentCount: 3, SegmentSeq: seq,
			Encoding: clickhouse.EncodingGSM7,
		}
	}

	// The message as the pipeline leaves it: accepted at ingress, cancelled by the customer, then three
	// segments parked as failed when the max-age SLA fired. The cancelled row is written LAST so the test
	// cannot pass merely because it arrived first.
	if err := writer.Insert(ctx, row(clickhouse.StatusAccepted, 0)); err != nil {
		t.Fatalf("insert accepted: %v", err)
	}
	for seq := uint16(1); seq <= 3; seq++ {
		if err := writer.Insert(ctx, row(clickhouse.StatusFailed, seq)); err != nil {
			t.Fatalf("insert failed seg %d: %v", seq, err)
		}
	}
	if err := writer.Insert(ctx, row(clickhouse.StatusCancelled, 0)); err != nil {
		t.Fatalf("insert cancelled: %v", err)
	}

	got, found, err := reader.Current(ctx, customerID, accountID, messageID)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !found {
		t.Fatal("a message with rows in this scope must be found: the replay guard reads it by this exact scope")
	}
	if got.Status != clickhouse.StatusCancelled {
		t.Fatalf("status = %q, want cancelled: three failed segments must not outrank one cancellation, "+
			"or the replay would put a cancelled message back on the wire", got.Status)
	}
}
