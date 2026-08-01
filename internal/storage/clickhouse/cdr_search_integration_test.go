package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

// searchRow seeds one accepted row. submitted_at is immutable across a message's rows, so seeding the
// accepted row alone is enough to place the message in a search window.
func searchRow(customerID, accountID uuid.UUID, at time.Time, dest string) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    accountID,
		CustomerID:   customerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   "GATEWAY",
		DestAddr:     dest,
		SubmittedAt:  at,
		Status:       clickhouse.StatusAccepted,
		SegmentCount: 1,
		Encoding:     clickhouse.EncodingGSM7,
	}
}

func searchReader(t *testing.T) (*clickhouse.CDRWriter, *clickhouse.CDRReader) {
	t.Helper()

	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return clickhouse.NewCDRWriter(conn), clickhouse.NewCDRReader(conn)
}

func searchMessageIDs(rows []clickhouse.CDRRow) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.MessageID)
	}
	return out
}

// TestSearchIsScopedToItsWindowAndCustomers pins the two predicates that bound the read: a message
// outside the window and a message of another customer must both stay out, whatever else matches.
func TestSearchIsScopedToItsWindowAndCustomers(t *testing.T) {
	writer, reader := searchReader(t)
	ctx := context.Background()

	mine, other := uuid.New(), uuid.New()
	accountID := uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)

	inWindow := searchRow(mine, accountID, now.Add(-time.Hour), "22507000001")
	tooOld := searchRow(mine, accountID, now.Add(-48*time.Hour), "22507000002")
	otherCustomer := searchRow(other, accountID, now.Add(-time.Hour), "22507000003")
	if err := writer.InsertBatch(ctx, []clickhouse.CDRRow{inWindow, tooOld, otherCustomer}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	rows, err := reader.Search(ctx, clickhouse.CDRSearchFilter{
		FromDate:    now.Add(-24 * time.Hour),
		ToDate:      now.Add(time.Minute),
		CustomerIDs: []uuid.UUID{mine},
	}, 50)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(rows) != 1 || rows[0].MessageID != inWindow.MessageID {
		t.Fatalf("got %d rows %v, want only %s", len(rows), searchMessageIDs(rows), inWindow.MessageID)
	}
}

// TestSearchMatchesAnMSISDNOnEitherSide: an operator searching a subscriber number does not know
// whether it was the destination (MT) or the source (MO).
func TestSearchMatchesAnMSISDNOnEitherSide(t *testing.T) {
	writer, reader := searchReader(t)
	ctx := context.Background()

	customerID, accountID := uuid.New(), uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)
	const subscriber = "22507111222"

	asDest := searchRow(customerID, accountID, now.Add(-time.Hour), subscriber)
	asSource := searchRow(customerID, accountID, now.Add(-2*time.Hour), "22507999999")
	asSource.Direction = clickhouse.DirectionMO
	asSource.SourceAddr = subscriber
	unrelated := searchRow(customerID, accountID, now.Add(-time.Hour), "22507333444")
	if err := writer.InsertBatch(ctx, []clickhouse.CDRRow{asDest, asSource, unrelated}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	msisdn := subscriber
	rows, err := reader.Search(ctx, clickhouse.CDRSearchFilter{
		FromDate:    now.Add(-24 * time.Hour),
		ToDate:      now.Add(time.Minute),
		CustomerIDs: []uuid.UUID{customerID},
		MSISDN:      &msisdn,
	}, 50)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	found := map[uuid.UUID]bool{}
	for _, r := range rows {
		found[r.MessageID] = true
	}
	if !found[asDest.MessageID] || !found[asSource.MessageID] {
		t.Errorf("both sides must match, got %v", searchMessageIDs(rows))
	}
	if found[unrelated.MessageID] {
		t.Error("an unrelated number matched: the comparison is not an equality")
	}
}

// TestSearchFindsAMessageByTraceID covers the same trap as the MSISDN filter: trace_id is an argMax
// alias inside the aggregation, so an unqualified predicate is rejected by ClickHouse rather than
// silently wrong. A trace lookup is how an operator pivots from a span to the message.
func TestSearchFindsAMessageByTraceID(t *testing.T) {
	writer, reader := searchReader(t)
	ctx := context.Background()

	customerID, accountID := uuid.New(), uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)

	wanted := searchRow(customerID, accountID, now.Add(-time.Hour), "22507000001")
	other := searchRow(customerID, accountID, now.Add(-time.Hour), "22507000002")
	if err := writer.InsertBatch(ctx, []clickhouse.CDRRow{wanted, other}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	traceID := wanted.TraceID
	rows, err := reader.Search(ctx, clickhouse.CDRSearchFilter{
		FromDate:    now.Add(-24 * time.Hour),
		ToDate:      now.Add(time.Minute),
		CustomerIDs: []uuid.UUID{customerID},
		TraceID:     &traceID,
	}, 50)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(rows) != 1 || rows[0].MessageID != wanted.MessageID {
		t.Fatalf("got %v, want only %s", searchMessageIDs(rows), wanted.MessageID)
	}
}

// TestSearchKeysetPagesThroughTiedTimestamps seeds three messages on the SAME millisecond — the case
// that loses a whole page if the keyset compares a bound DateTime64 instead of integer milliseconds
// (the step-029 trap). Paging one row at a time must still see all three.
func TestSearchKeysetPagesThroughTiedTimestamps(t *testing.T) {
	writer, reader := searchReader(t)
	ctx := context.Background()

	customerID, accountID := uuid.New(), uuid.New()
	at := time.Now().UTC().Truncate(time.Millisecond)
	seeded := []clickhouse.CDRRow{
		searchRow(customerID, accountID, at, "22507000001"),
		searchRow(customerID, accountID, at, "22507000002"),
		searchRow(customerID, accountID, at, "22507000003"),
	}
	if err := writer.InsertBatch(ctx, seeded); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	f := clickhouse.CDRSearchFilter{
		FromDate:    at.Add(-time.Minute),
		ToDate:      at.Add(time.Minute),
		CustomerIDs: []uuid.UUID{customerID},
	}
	var seen []uuid.UUID
	for page := 0; page < len(seeded)+2; page++ {
		rows, err := reader.Search(ctx, f, 1)
		if err != nil {
			t.Fatalf("Search page %d: %v", page, err)
		}
		if len(rows) == 0 {
			break
		}
		seen = append(seen, rows[0].MessageID)
		f.After = &clickhouse.CDRKey{SubmittedAt: rows[0].SubmittedAt, MessageID: rows[0].MessageID}
	}
	if len(seen) != len(seeded) {
		t.Fatalf("paged through %d messages %v, want %d — the keyset dropped rows tied on the millisecond",
			len(seen), seen, len(seeded))
	}
}
