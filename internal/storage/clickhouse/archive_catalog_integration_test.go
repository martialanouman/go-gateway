package clickhouse_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// archiveCatalog models control_plane.cdr_archives: the first object recorded for a day holds it for good,
// and Record answers with the object of whichever line holds the day.
type archiveCatalog struct {
	entries map[string]catalogued
	failing bool
}

type catalogued struct {
	object string
	rows   uint64
}

func newArchiveCatalog() *archiveCatalog { return &archiveCatalog{entries: map[string]catalogued{}} }

func (c *archiveCatalog) Record(_ context.Context, day time.Time, object string, rows uint64) (string, error) {
	if c.failing {
		return "", errors.New("catalogue unavailable")
	}
	key := day.Format(time.DateOnly)
	if e, ok := c.entries[key]; ok {
		return e.object, nil
	}
	c.entries[key] = catalogued{object: object, rows: rows}
	return object, nil
}

func (c *archiveCatalog) object(t *testing.T, day time.Time) string {
	t.Helper()
	e, ok := c.entries[day.Format(time.DateOnly)]
	if !ok {
		t.Fatalf("catalogue holds no archive for %s", day.Format(time.DateOnly))
	}
	return e.object
}

func archivePrefix() string { return "cdr-archive-" + uuid.NewString()[:8] }

func TestRetainerKeepsPartitionWhenTheCatalogueFails(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	day := seedDay(t, conn, 75, 3)
	catalog := newArchiveCatalog()
	catalog.failing = true
	archiver := clickhouse.NewPartitionArchiver(conn, archivePrefix(), clickhouse.FileDestination(), catalog)
	retainer := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(archiver))

	if _, err := retainer.Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n := countDay(t, conn, day); n != 3 {
		t.Fatalf("partition holds %d rows, want its 3 kept: an archive nobody can find is no archive", n)
	}

	// The first attempt left a complete object behind; the next pass writes its own and catalogues that one.
	catalog.failing = false
	if _, err := retainer.Purge(ctx); err != nil {
		t.Fatalf("second Purge: %v", err)
	}
	if n := countDay(t, conn, day); n != 0 {
		t.Errorf("partition holds %d rows after a catalogued archive, want 0", n)
	}
	var archived uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+clickhouse.FileDestination()(catalog.object(t, day))).
		Scan(&archived); err != nil {
		t.Fatalf("read the catalogued archive: %v", err)
	}
	if archived != 3 {
		t.Errorf("catalogued archive holds %d rows, want 3", archived)
	}
}

// A day already catalogued with another object (a drop that failed after cataloguing, or a late write that
// recreated a dropped day) is never dropped by the retention pass: nothing cheap proves the catalogued object
// still covers it, so an operator decides.
func TestRetainerKeepsADayCataloguedWithAnotherObject(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	day := seedDay(t, conn, 85, 3)
	before := countDay(t, conn, day)
	catalog := newArchiveCatalog()
	archiver := clickhouse.NewPartitionArchiver(conn, archivePrefix(), clickhouse.FileDestination(), catalog)
	if err := archiver.Archive(ctx, clickhouse.Partition{Day: day, Rows: before}); err != nil {
		t.Fatalf("first Archive: %v", err)
	}
	first := catalog.object(t, day)

	if _, err := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(archiver)).Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n := countDay(t, conn, day); n != before {
		t.Errorf("partition holds %d rows, want its %d kept: its catalogued archive is another object", n, before)
	}
	if got := catalog.object(t, day); got != first {
		t.Errorf("catalogue now designates %q, want the first object %q untouched", got, first)
	}
}
