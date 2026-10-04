package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// freshLedgerDB migrates a database of its own: the shared one holds today's rows in DEFAULT, written by
// every other test of the process, and the ATTACH of today's partition would refuse them.
func freshLedgerDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	admin := pgtest.Pool(t)

	name := "ledger_" + uuid.NewString()[:8]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	})

	u, err := url.Parse(pgtest.Config(t).URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Path = "/" + name
	m, err := postgres.NewMigrator(u.String(), "../../../migrations", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("migrator: %v", err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = m.Close()

	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func ledgerPartitions(t *testing.T, pool *pgxpool.Pool) map[string]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'control_plane.billing_ledger'::regclass`)
	if err != nil {
		t.Fatalf("list partitions: %v", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("list partitions: %v", err)
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	return got
}

func partitionName(day time.Time) string {
	return "billing_ledger_" + day.UTC().Format("20060102")
}

// dbNow is the clock created_at is stamped with: a test reading Go's would split a day at midnight UTC.
func dbNow(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var now time.Time
	if err := pool.QueryRow(context.Background(), `SELECT now()`).Scan(&now); err != nil {
		t.Fatalf("read now(): %v", err)
	}
	return now
}

func seedLedgerCustomer(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(),
		`INSERT INTO control_plane.customers (name) VALUES ('ledger-partition-test') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	return id
}

func recordTopup(t *testing.T, repo *postgres.BillingRepo, customer uuid.UUID) {
	t.Helper()
	if _, applied, err := repo.RecordDurable(context.Background(), cp.LedgerEntry{
		OwnerType: cp.OwnerTypeCustomer, OwnerID: customer, Direction: cp.BillingDirectionMT,
		CustomerID: customer, EntryType: cp.EntryTopup, Credits: 10,
	}); err != nil || !applied {
		t.Fatalf("RecordDurable: applied=%v err=%v", applied, err)
	}
}

func TestEnsureLedgerPartitionsCreatesTheDaysAndTodayLandsInItsOwn(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)
	// West of UTC, the local date lags the UTC one for part of the day: the days are UTC days.
	now := dbNow(t, pool).In(time.FixedZone("UTC-11", -11*3600))

	if err := repo.EnsureLedgerPartitions(ctx, now, 3); err != nil {
		t.Fatalf("EnsureLedgerPartitions: %v", err)
	}

	got := ledgerPartitions(t, pool)
	for d := range 3 {
		if !got[partitionName(now.AddDate(0, 0, d))] {
			t.Errorf("partition for day +%d missing; have %v", d, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("want DEFAULT + 3 days, have %v", got)
	}

	recordTopup(t, repo, seedLedgerCustomer(t, pool))
	var landed string
	var createdAt time.Time
	if err := pool.QueryRow(ctx,
		`SELECT tableoid::regclass::text, created_at FROM control_plane.billing_ledger`).Scan(&landed, &createdAt); err != nil {
		t.Fatalf("read ledger row: %v", err)
	}
	if want := "control_plane." + partitionName(createdAt); landed != want {
		t.Errorf("today's movement landed in %s, want %s", landed, want)
	}
}

// TestEnsureLedgerPartitionsFromConcurrentReplicas holds the advisory lock while two replicas start: both must
// wait for it, then one creates each day and the other finds it. A replica that skipped a day it could not
// lock would leave it to nobody.
func TestEnsureLedgerPartitionsFromConcurrentReplicas(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	from := dbNow(t, pool)

	holder, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	defer holder.Release()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, postgres.LedgerPartitionLock); err != nil {
		t.Fatalf("hold the lock: %v", err)
	}

	const replicas, days = 2, 3
	var wg sync.WaitGroup
	errs := make([]error, replicas)
	for r := range replicas {
		repo := postgres.NewBillingRepo(pool)
		wg.Go(func() { errs[r] = repo.EnsureLedgerPartitions(ctx, from, days) })
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, postgres.LedgerPartitionLock); err != nil {
		t.Fatalf("release the lock: %v", err)
	}
	wg.Wait()

	for r, err := range errs {
		if err != nil {
			t.Errorf("replica %d: %v", r, err)
		}
	}
	if got := ledgerPartitions(t, pool); len(got) != 1+days {
		t.Errorf("want DEFAULT + %d partitions, have %v", days, got)
	}
}

func TestAWriteOutsideThePartitionsLandsInDefaultAndIsCounted(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)
	now := dbNow(t, pool)

	if err := repo.EnsureLedgerPartitions(ctx, now, 2); err != nil {
		t.Fatalf("EnsureLedgerPartitions: %v", err)
	}
	if n, err := repo.LedgerDefaultRows(ctx); err != nil || n != 0 {
		t.Fatalf("LedgerDefaultRows on an empty DEFAULT = %d, %v; want 0", n, err)
	}

	customer := seedLedgerCustomer(t, pool)
	recordTopup(t, repo, customer)
	insertOutside := func(rows int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO control_plane.billing_ledger
			  (owner_type, owner_id, direction, customer_id, entry_type, credits, balance_after, created_at)
			SELECT 'customer', $1, 'mt', $1, 'topup', 1, 1, $2 FROM generate_series(1, $3)`,
			customer, now.AddDate(0, 0, 30), rows); err != nil {
			t.Fatalf("a write with no partition for its day failed: %v", err)
		}
	}
	insertOutside(1)
	if n, err := repo.LedgerDefaultRows(ctx); err != nil || n != 1 {
		t.Errorf("LedgerDefaultRows = %d, %v; want 1", n, err)
	}
	insertOutside(10_000)
	if n, err := repo.LedgerDefaultRows(ctx); err != nil || n != 10_000 {
		t.Errorf("LedgerDefaultRows over 10 001 rows = %d, %v; want the 10 000 cap", n, err)
	}
}

// TestEnsureLedgerPartitionsDoesNotWaitForAnOpenWriter: CREATE TABLE … PARTITION OF takes ACCESS EXCLUSIVE on
// the parent, which every hot-path COPY and INSERT holds in ROW EXCLUSIVE until it commits.
func TestEnsureLedgerPartitionsDoesNotWaitForAnOpenWriter(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)
	now := dbNow(t, pool)

	if err := repo.EnsureLedgerPartitions(ctx, now, 1); err != nil {
		t.Fatalf("EnsureLedgerPartitions: %v", err)
	}
	customer := seedLedgerCustomer(t, pool)
	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin writer: %v", err)
	}
	defer func() { _ = writer.Rollback(ctx) }()
	if _, err := writer.Exec(ctx, `
		INSERT INTO control_plane.billing_ledger
		  (owner_type, owner_id, direction, customer_id, entry_type, credits, balance_after)
		VALUES ('customer', $1, 'mt', $1, 'topup', 1, 1)`, customer); err != nil {
		t.Fatalf("open write: %v", err)
	}

	if err := repo.EnsureLedgerPartitions(ctx, now.AddDate(0, 0, 5), 1); err != nil {
		t.Fatalf("a pass behind an open hot-path write failed: %v", err)
	}
	if !ledgerPartitions(t, pool)[partitionName(now.AddDate(0, 0, 5))] {
		t.Error("the partition was not created")
	}
}

// TestEnsureLedgerPartitionsGivesUpQuicklyBehindAnOpenReader: a capture's ledger reads cannot be pruned, so they
// hold DEFAULT; every read arriving while the ATTACH waits for DEFAULT queues behind it.
func TestEnsureLedgerPartitionsGivesUpQuicklyBehindAnOpenReader(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	reader, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reader: %v", err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	if _, err := reader.Exec(ctx, `SELECT 1 FROM control_plane.billing_ledger WHERE message_id = $1`, uuid.New()); err != nil {
		t.Fatalf("open read: %v", err)
	}

	start := time.Now()
	err = repo.EnsureLedgerPartitions(ctx, dbNow(t, pool), 1)
	if err == nil {
		t.Fatal("the ATTACH went through DEFAULT held by an open reader")
	}
	if waited := time.Since(start); waited > 600*time.Millisecond {
		t.Errorf("the pass held reads queued for %v (%v), want the 200ms bound", waited, err)
	}
}

func TestEnsureLedgerPartitionsRefusesAnUnattachedTableOfTheDay(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	now := dbNow(t, pool)
	if _, err := pool.Exec(ctx, `CREATE TABLE control_plane.`+partitionName(now)+
		` (LIKE control_plane.billing_ledger INCLUDING DEFAULTS INCLUDING CONSTRAINTS)`); err != nil {
		t.Fatalf("create the stray table: %v", err)
	}

	if err := postgres.NewBillingRepo(pool).EnsureLedgerPartitions(ctx, now, 1); err == nil {
		t.Error("a table of the day that is not a partition was taken for one: the day's writes go to DEFAULT unannounced")
	}
}
