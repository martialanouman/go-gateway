package clickhouse_test

import (
	"context"
	"errors"
	"fmt"
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

// A drop that failed after the day was catalogued leaves the partition for the next pass, which writes its own
// object but cannot catalogue it: the day is dropped only if the catalogued object already holds every row.
func TestRetainerDropsAPartitionItsCataloguedArchiveCovers(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	day := seedDay(t, conn, 85, 3)
	catalog := newArchiveCatalog()
	archiver := clickhouse.NewPartitionArchiver(conn, archivePrefix(), clickhouse.FileDestination(), catalog)
	if err := archiver.Archive(ctx, clickhouse.Partition{Day: day, Rows: countDay(t, conn, day)}); err != nil {
		t.Fatalf("first Archive: %v", err)
	}
	first := catalog.object(t, day)

	if _, err := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(archiver)).Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n := countDay(t, conn, day); n != 0 {
		t.Errorf("partition holds %d rows, want 0: the catalogued archive covers it", n)
	}
	if got := catalog.object(t, day); got != first {
		t.Errorf("catalogue now designates %q, want the first object %q untouched", got, first)
	}
}

// A later version of a message lands in its old partition, and a merge can leave the partition with no more
// rows than the catalogued object: only the (message_id, version) pairs tell that the new version is missing.
func TestRetainerKeepsAVersionItsCataloguedArchiveLacks(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	day := seedDay(t, conn, 86, 2)
	catalog := newArchiveCatalog()
	archiver := clickhouse.NewPartitionArchiver(conn, archivePrefix(), clickhouse.FileDestination(), catalog)
	if err := archiver.Archive(ctx, clickhouse.Partition{Day: day, Rows: countDay(t, conn, day)}); err != nil {
		t.Fatalf("first Archive: %v", err)
	}

	var row clickhouse.CDRRow
	if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT message_id, account_id, customer_id, submitted_at FROM cdr
		WHERE toDate(submitted_at) = '%s' LIMIT 1`, day.Format(time.DateOnly))).
		Scan(&row.MessageID, &row.AccountID, &row.CustomerID, &row.SubmittedAt); err != nil {
		t.Fatalf("read a seeded message: %v", err)
	}
	row.Direction, row.SourceAddr, row.DestAddr = clickhouse.DirectionMT, "GATEWAY", "22507000000"
	row.Status, row.SegmentCount, row.Encoding = clickhouse.StatusDelivered, 1, clickhouse.EncodingGSM7
	if err := clickhouse.NewCDRWriter(conn).Insert(ctx, row); err != nil {
		t.Fatalf("insert a later version: %v", err)
	}
	before := countDay(t, conn, day)

	if _, err := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(archiver)).Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n := countDay(t, conn, day); n != before {
		t.Errorf("partition holds %d rows, want its %d kept: the catalogued archive lacks the delivered version", n, before)
	}
}
