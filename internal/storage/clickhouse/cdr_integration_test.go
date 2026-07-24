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
			Status:      clickhouse.StatusEnroute, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
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
			SubmittedAt: same, Status: clickhouse.StatusEnroute, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
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
		Status: clickhouse.StatusEnroute, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
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
			SubmittedAt: at, Status: clickhouse.StatusEnroute, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
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
