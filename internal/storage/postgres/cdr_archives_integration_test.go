package postgres_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// archiveDay is almost certainly a day no other run has catalogued: the container outlives a -count>1 run.
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
	if got != "cdr-first.parquet" {
		t.Errorf("first Record = %q, want its own object", got)
	}

	got, err = repo.Record(ctx, day, "cdr-second.parquet", 9)
	if err != nil {
		t.Fatalf("second Record: %v", err)
	}
	if got != "cdr-first.parquet" {
		t.Errorf("second Record = %q, want the catalogued cdr-first.parquet: a line is never rewritten", got)
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

// Two replicas archiving the same day: the second's insert waits on the first's uncommitted line, then
// conflicts, and must still answer with the line that holds the day.
func TestCDRArchiveRecordAnswersBehindAConcurrentInsert(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCDRArchiveRepo(pool)
	ctx := context.Background()
	day := archiveDay()

	first, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = first.Rollback(ctx) }()
	if _, err := first.Exec(ctx, `INSERT INTO control_plane.cdr_archives (day, object, row_count) VALUES ($1, 'cdr-first.parquet', 7)`, day); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	type result struct {
		object string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		object, err := repo.Record(ctx, day, "cdr-second.parquet", 9)
		done <- result{object, err}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%cdr_archives%')`).Scan(&waiting); err != nil {
			t.Fatalf("poll lock wait: %v", err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the second Record never waited on the first insert")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := first.Commit(ctx); err != nil {
		t.Fatalf("commit first: %v", err)
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("second Record: %v", got.err)
	}
	if got.object != "cdr-first.parquet" {
		t.Errorf("second Record = %q, want the committed line's cdr-first.parquet", got.object)
	}
}
