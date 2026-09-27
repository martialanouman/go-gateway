package postgres_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// archiveDay is a day no other run of this package has catalogued: the container outlives a -count>1 run.
func archiveDay() time.Time {
	return time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, rand.IntN(3_000_000))
}

func TestCDRArchiveRecordCataloguesTheFirstVerifiedObjectOnly(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCDRArchiveRepo(pool)
	ctx := context.Background()
	day := archiveDay()

	got, err := repo.Record(ctx, day, "cdr-first.parquet", 7)
	if err != nil {
		t.Fatalf("first Record: %v", err)
	}
	if got != 7 {
		t.Errorf("first Record = %d, want its own 7 rows", got)
	}

	got, err = repo.Record(ctx, day, "cdr-second.parquet", 9)
	if err != nil {
		t.Fatalf("second Record: %v", err)
	}
	if got != 7 {
		t.Errorf("second Record = %d, want the catalogued 7: a line is never rewritten", got)
	}

	var object string
	var rows int64
	if err := pool.QueryRow(ctx, `SELECT object, row_count FROM control_plane.cdr_archives WHERE day = $1`, day).
		Scan(&object, &rows); err != nil {
		t.Fatalf("read catalogue: %v", err)
	}
	if object != "cdr-first.parquet" || rows != 7 {
		t.Errorf("catalogue holds %q/%d, want the first object cdr-first.parquet/7", object, rows)
	}
}
