package clickhouse_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

// Retention tests share one ClickHouse with the rest of the package, so they follow one rule: a purge must
// never expire TODAY's partition, which every other test writes into. Each test therefore seeds its own
// partition far in the past (but inside the table's own 90-day TTL, so ClickHouse never removes the rows
// behind the test's back) and purges with a retention that keeps today.
const (
	// retentionKeepsToday is comfortably shorter than the seeded partitions' age and longer than a day.
	retentionKeepsToday = 30 * 24 * time.Hour
)

func retentionConn(t *testing.T) *clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.NewConn(chtest.Config(t))
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// seedDay writes n CDR rows into the partition of the given day and returns that day.
func seedDay(t *testing.T, conn *clickhouse.Conn, daysAgo, n int) time.Time {
	t.Helper()
	writer := clickhouse.NewCDRWriter(conn)
	day := time.Now().UTC().AddDate(0, 0, -daysAgo).Truncate(24 * time.Hour)
	rows := make([]clickhouse.CDRRow, n)
	for i := range rows {
		rows[i] = clickhouse.CDRRow{
			MessageID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
			Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "22507000000",
			SubmittedAt: day.Add(time.Hour), Status: clickhouse.StatusAccepted,
			SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
		}
	}
	if err := writer.InsertBatch(context.Background(), rows); err != nil {
		t.Fatalf("seed partition %s: %v", day.Format("2006-01-02"), err)
	}
	return day
}

// countDay returns how many CDR rows remain in a day's partition.
func countDay(t *testing.T, conn *clickhouse.Conn, day time.Time) uint64 {
	t.Helper()
	var n uint64
	q := fmt.Sprintf("SELECT count() FROM cdr WHERE toDate(submitted_at) = '%s'", day.Format("2006-01-02"))
	if err := conn.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("count day %s: %v", day.Format("2006-01-02"), err)
	}
	return n
}

// TestRetainerDropsExpiredPartitionKeepsActive: retention drops a whole expired partition — the §14
// requirement (DROP PARTITION, never DELETE WHERE) — and leaves partitions inside the window untouched.
func TestRetainerDropsExpiredPartitionKeepsActive(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()

	// Today's partition is shared with the rest of the package, so it is asserted by DELTA, never by an
	// absolute count: what matters is that the purge leaves it exactly as it found it.
	old := seedDay(t, conn, 80, 3)
	today := seedDay(t, conn, 0, 2)
	todayBefore := countDay(t, conn, today)
	if countDay(t, conn, old) != 3 || todayBefore < 2 {
		t.Fatalf("seed failed: old=%d today=%d", countDay(t, conn, old), todayBefore)
	}

	retainer := clickhouse.NewRetainer(conn, retentionKeepsToday)

	expired, err := retainer.Expired(ctx)
	if err != nil {
		t.Fatalf("Expired: %v", err)
	}
	var sawOld, sawToday bool
	for _, p := range expired {
		if p.Name() == old.Format("2006-01-02") {
			sawOld = true
		}
		if p.Name() == today.Format("2006-01-02") {
			sawToday = true
		}
	}
	if !sawOld || sawToday {
		t.Fatalf("expired set wrong: old=%v today=%v (today must never expire)", sawOld, sawToday)
	}

	report, err := retainer.Purge(ctx)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if report.Dropped < 1 {
		t.Errorf("dropped %d partitions, want at least the expired one", report.Dropped)
	}
	if n := countDay(t, conn, old); n != 0 {
		t.Errorf("expired partition still holds %d rows, want 0 (dropped)", n)
	}
	if n := countDay(t, conn, today); n != todayBefore {
		t.Errorf("today's partition holds %d rows, want the %d it had (must not be purged)", n, todayBefore)
	}
}

// TestRetainerArchivesBeforeDrop: with an Archiver configured, an expired partition is written to cold
// storage (here a Parquet file ClickHouse writes itself) BEFORE it is dropped — the tiering path. The
// archive is read back to prove the rows really landed there.
func TestRetainerArchivesBeforeDrop(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()

	// 120 days old — deliberately OLDER than the CDR retention the Retainer enforces AND older than the
	// table's original 90-day row TTL. It is the regression guard for the tiering trap: while the table TTL
	// competed with the Retainer, ClickHouse emptied such a partition before the Retainer reached it, so the
	// archive captured nothing and the drop destroyed the data. The table TTL is now a far backstop (400
	// days), so the rows must still be here to be archived.
	day := seedDay(t, conn, 120, 4)
	if n := countDay(t, conn, day); n != 4 {
		t.Fatalf("partition holds %d rows before archiving, want 4 — nothing may delete rows ahead of the Retainer", n)
	}
	prefix := "cdr-archive-" + uuid.NewString()[:8]
	// Each archiving attempt writes its own object, so the test captures the destination the archiver
	// actually used in order to read it back.
	var archivedDest string
	dest := func(p clickhouse.Partition, token string) string {
		archivedDest = fmt.Sprintf("file('%s-%s-%s.parquet', 'Parquet')", prefix, p.Name(), token)
		return archivedDest
	}
	retainer := clickhouse.NewRetainer(conn, retentionKeepsToday,
		clickhouse.WithArchiver(clickhouse.NewPartitionArchiver(conn, dest)))

	report, err := retainer.Purge(ctx)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if report.Archived < 1 || report.Dropped < 1 {
		t.Fatalf("report = %+v, want at least one archived and dropped", report)
	}
	if n := countDay(t, conn, day); n != 0 {
		t.Errorf("archived partition still holds %d rows, want 0", n)
	}

	// The Parquet archive is readable on its own and holds the partition's rows — including the enum and
	// UUID columns rendered as strings, so it can be read without the platform's schema.
	var archived uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+archivedDest).Scan(&archived); err != nil {
		t.Fatalf("read back archive: %v", err)
	}
	if archived != 4 {
		t.Errorf("archive holds %d rows, want the partition's 4", archived)
	}
	var status, direction string
	if err := conn.QueryRow(ctx, "SELECT status, direction FROM "+archivedDest+" LIMIT 1").Scan(&status, &direction); err != nil {
		t.Fatalf("read archived enums: %v", err)
	}
	if status != string(clickhouse.StatusAccepted) || direction != string(clickhouse.DirectionMT) {
		t.Errorf("archived enums = %q/%q, want readable names (accepted/mt)", status, direction)
	}
}

// failingArchiver stands in for cold storage being unavailable.
type failingArchiver struct{ calls int }

func (f *failingArchiver) Archive(context.Context, clickhouse.Partition) error {
	f.calls++
	return errors.New("cold storage unavailable")
}

// TestRetainerKeepsPartitionWhenArchiveFails: retention must never destroy data it failed to archive — a
// failed archive leaves the partition in place for the next pass.
func TestRetainerKeepsPartitionWhenArchiveFails(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()

	day := seedDay(t, conn, 70, 2)
	archiver := &failingArchiver{}
	retainer := clickhouse.NewRetainer(conn, retentionKeepsToday, clickhouse.WithArchiver(archiver))

	report, err := retainer.Purge(ctx)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if archiver.calls == 0 {
		t.Fatal("archiver was never called")
	}
	if report.Dropped != 0 || report.Archived != 0 {
		t.Errorf("report = %+v, want nothing dropped when archiving fails", report)
	}
	if n := countDay(t, conn, day); n != 2 {
		t.Errorf("partition holds %d rows, want its 2 kept (never drop unarchived data)", n)
	}
}

// TestCDRBodyExpiresBeforeMetadata: the per-column TTL (migration 0003) clears the stored body well before
// the CDR row itself expires — content_retention_days decoupled from CDR retention (§6.14). The row and its
// metadata survive; only content_ciphertext/content_key_id are gone.
func TestCDRBodyExpiresBeforeMetadata(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)

	// 60 days old: past the 30-day body TTL, still inside the 90-day CDR retention.
	day := time.Now().UTC().AddDate(0, 0, -60).Truncate(24 * time.Hour)
	messageID, customerID, keyID := uuid.New(), uuid.New(), uuid.New()
	envelope := "sealed-body-bytes"
	row := clickhouse.CDRRow{
		MessageID: messageID, AccountID: uuid.New(), CustomerID: customerID,
		Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "22507000000",
		SubmittedAt: day.Add(time.Hour), Status: clickhouse.StatusAccepted, SegmentCount: 1,
		Encoding: clickhouse.EncodingGSM7, ContentCiphertext: &envelope, ContentKeyID: &keyID,
	}
	if err := writer.Insert(ctx, row); err != nil {
		t.Fatalf("insert aged row: %v", err)
	}

	// Column TTLs are applied on merge; force it so the test does not depend on background timing.
	stmt := fmt.Sprintf("ALTER TABLE cdr MATERIALIZE TTL IN PARTITION '%s' SETTINGS mutations_sync = 2", day.Format("2006-01-02"))
	if err := conn.Exec(ctx, stmt); err != nil {
		t.Fatalf("materialize ttl: %v", err)
	}

	got, found, err := reader.ByMessageID(ctx, messageID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !found {
		t.Fatal("the CDR row must survive its body's expiry (metadata is kept for the full retention)")
	}
	if got.CustomerID != customerID || got.Status != clickhouse.StatusAccepted {
		t.Errorf("metadata damaged by the body TTL: %+v", got)
	}
	if got.ContentCiphertext != nil {
		t.Errorf("content_ciphertext = %v, want NULL (the body must expire before the metadata)", got.ContentCiphertext)
	}
	if got.ContentKeyID != nil {
		t.Errorf("content_key_id = %v, want NULL", got.ContentKeyID)
	}
}
