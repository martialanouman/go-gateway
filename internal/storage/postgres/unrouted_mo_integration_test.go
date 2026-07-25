package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestUnroutedMORepoCreateAndKeysetPage records three unrouted MO with decreasing received_at and
// pages through them newest-first, proving the keyset cursor.
func TestUnroutedMORepoCreateAndKeysetPage(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewUnroutedMORepo(pool)
	ctx := context.Background()

	connectorID := uuid.New()
	base := time.Now().UTC().Truncate(time.Millisecond)
	// Insert oldest-first so the ids don't correlate with recency; received_at drives the order.
	var created []cp.UnroutedMO
	for i := 2; i >= 0; i-- {
		row, err := repo.Create(ctx, cp.NewUnroutedMO{
			ReceivedAt:   base.Add(time.Duration(-i) * time.Second),
			ConnectorID:  &connectorID,
			SourceAddr:   "22507000001",
			DestAddr:     "36000",
			SegmentCount: 1,
			Encoding:     "gsm7",
			Reason:       cp.UnroutedNoKeywordMatch,
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		created = append(created, row)
	}
	if created[0].Reason != cp.UnroutedNoKeywordMatch || created[0].InboundNumberID != nil {
		t.Errorf("created row = %+v, want reason no_keyword_match / inbound_number_id nil", created[0])
	}

	// First page of 2: the two newest (base, base-1s).
	page1, err := repo.List(ctx, 2, nil)
	if err != nil {
		t.Fatalf("List first: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("first page = %d rows, want 2", len(page1))
	}
	if !page1[0].ReceivedAt.After(page1[1].ReceivedAt) {
		t.Errorf("page not newest-first: %v then %v", page1[0].ReceivedAt, page1[1].ReceivedAt)
	}

	// Next page after the 2nd row: the oldest remains.
	after := &cp.UnroutedMOKey{ReceivedAt: page1[1].ReceivedAt, ID: page1[1].ID}
	page2, err := repo.List(ctx, 2, after)
	if err != nil {
		t.Fatalf("List after: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("second page = %d rows, want 1", len(page2))
	}
	if !page2[0].ReceivedAt.Before(page1[1].ReceivedAt) {
		t.Errorf("keyset did not advance: %v not before %v", page2[0].ReceivedAt, page1[1].ReceivedAt)
	}
}

// TestUnroutedMORepoKeysetBreaksTiesOnID pages through rows that share an identical received_at,
// proving the (received_at, id) composite cursor separates them without a skip or a duplicate — the
// same-timestamp hazard the keyset is there to handle.
func TestUnroutedMORepoKeysetBreaksTiesOnID(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewUnroutedMORepo(pool)
	ctx := context.Background()

	// A distinct received_at far from the other test's rows, shared by three rows.
	at := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Millisecond)
	seen := map[uuid.UUID]bool{}
	for range 3 {
		row, err := repo.Create(ctx, cp.NewUnroutedMO{
			ReceivedAt: at, SourceAddr: "1", DestAddr: "2", SegmentCount: 1, Encoding: "gsm7",
			Reason: cp.UnroutedUnknownNumber,
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		seen[row.ID] = true
	}

	// Page one row at a time through the three tied rows; each must be distinct and in the tie group.
	var got []cp.UnroutedMO
	var after *cp.UnroutedMOKey
	for range 3 {
		page, err := repo.List(ctx, 1, after)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		// Skip any rows from other tests until we reach our tie group.
		for len(page) == 1 && !page[0].ReceivedAt.Equal(at) {
			after = &cp.UnroutedMOKey{ReceivedAt: page[0].ReceivedAt, ID: page[0].ID}
			page, err = repo.List(ctx, 1, after)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
		}
		if len(page) != 1 || !page[0].ReceivedAt.Equal(at) {
			break
		}
		got = append(got, page[0])
		after = &cp.UnroutedMOKey{ReceivedAt: page[0].ReceivedAt, ID: page[0].ID}
	}

	if len(got) != 3 {
		t.Fatalf("paged %d tied rows, want 3 (no skip)", len(got))
	}
	distinct := map[uuid.UUID]bool{}
	for _, r := range got {
		if distinct[r.ID] {
			t.Errorf("row %s returned twice (keyset duplicate)", r.ID)
		}
		distinct[r.ID] = true
		if !seen[r.ID] {
			t.Errorf("row %s is not one of the tie group", r.ID)
		}
	}
}
