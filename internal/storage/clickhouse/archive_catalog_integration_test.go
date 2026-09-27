package clickhouse_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// archiveCatalog models control_plane.cdr_archives: the first object recorded for a day holds it for good,
// and Record answers with the row count of whichever line holds the day.
type archiveCatalog struct {
	mu      sync.Mutex
	entries map[string]catalogued
	failing bool
}

type catalogued struct {
	object string
	rows   uint64
}

func newArchiveCatalog() *archiveCatalog { return &archiveCatalog{entries: map[string]catalogued{}} }

func (c *archiveCatalog) Record(_ context.Context, day time.Time, object string, rows uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failing {
		return 0, errors.New("catalogue unavailable")
	}
	key := day.Format(time.DateOnly)
	if e, ok := c.entries[key]; ok {
		return e.rows, nil
	}
	c.entries[key] = catalogued{object: object, rows: rows}
	return rows, nil
}

func (c *archiveCatalog) object(t *testing.T, day time.Time) string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
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

// A drop that failed after the day was catalogued leaves the partition for the next pass; if it grew in
// between, the catalogued object no longer holds it all, and dropping would lose what only an uncatalogued
// object holds.
func TestRetainerKeepsPartitionThatOutgrewItsCataloguedArchive(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	day := seedDay(t, conn, 85, 4)
	catalog := newArchiveCatalog()
	catalog.entries[day.Format(time.DateOnly)] = catalogued{object: "cdr-earlier.parquet", rows: 2}
	archiver := clickhouse.NewPartitionArchiver(conn, archivePrefix(), clickhouse.FileDestination(), catalog)
	retainer := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(archiver))

	if _, err := retainer.Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n := countDay(t, conn, day); n != 4 {
		t.Errorf("partition holds %d rows, want its 4 kept: the catalogued archive holds only 2", n)
	}
	if got := catalog.object(t, day); got != "cdr-earlier.parquet" {
		t.Errorf("catalogue now designates %q, want the earlier line untouched", got)
	}
}
