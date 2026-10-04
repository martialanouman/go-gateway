package postgres_test

import (
	"context"
	"io"
	"log/slog"
	"maps"
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

func ledgerPartitions(t *testing.T, pool *pgxpool.Pool) map[string]uint32 {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT c.relname, c.oid FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'control_plane.billing_ledger'::regclass`)
	if err != nil {
		t.Fatalf("list partitions: %v", err)
	}
	got := map[string]uint32{}
	for rows.Next() {
		var name string
		var oid uint32
		if err := rows.Scan(&name, &oid); err != nil {
			t.Fatalf("scan partition: %v", err)
		}
		got[name] = oid
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list partitions: %v", err)
	}
	return got
}

func partitionName(day time.Time) string {
	return "billing_ledger_" + day.UTC().Format("20060102")
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

func TestEnsureLedgerPartitionsCreatesTheDaysAndTodayLandsInItsOwn(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)
	now := time.Now()

	if err := repo.EnsureLedgerPartitions(ctx, now, 3); err != nil {
		t.Fatalf("EnsureLedgerPartitions: %v", err)
	}

	got := ledgerPartitions(t, pool)
	for d := range 3 {
		if _, ok := got[partitionName(now.AddDate(0, 0, d))]; !ok {
			t.Errorf("partition for day +%d missing; have %v", d, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("want DEFAULT + 3 days, have %v", got)
	}

	var unique int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_index i WHERE i.indrelid = ('control_plane.' || $1)::regclass AND i.indisunique`,
		partitionName(now)).Scan(&unique); err != nil {
		t.Fatalf("count unique indexes: %v", err)
	}
	if unique != 2 {
		t.Errorf("today's partition carries %d unique indexes, want 2 (primary key + billing_ledger_idem_idx)", unique)
	}

	customer := seedLedgerCustomer(t, pool)
	if _, applied, err := repo.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeCustomer, OwnerID: customer, Direction: cp.BillingDirectionMT,
		CustomerID: customer, EntryType: cp.EntryTopup, Credits: 10,
	}); err != nil || !applied {
		t.Fatalf("RecordDurable: applied=%v err=%v", applied, err)
	}
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

func TestEnsureLedgerPartitionsTwiceChangesNothing(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)
	now := time.Now()

	if err := repo.EnsureLedgerPartitions(ctx, now, 2); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	before := ledgerPartitions(t, pool)
	if len(before) != 3 {
		t.Fatalf("first pass: want DEFAULT + 2 days, have %v", before)
	}
	if err := repo.EnsureLedgerPartitions(ctx, now, 2); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if after := ledgerPartitions(t, pool); !maps.Equal(before, after) {
		t.Errorf("second pass changed the partitions: before %v, after %v", before, after)
	}
}

func TestEnsureLedgerPartitionsFromConcurrentReplicas(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	start := time.Now()

	const rounds, replicas, days = 10, 2, 3
	for round := range rounds {
		from := start.AddDate(0, 0, 10*(round+1))
		var wg sync.WaitGroup
		errs := make([]error, replicas)
		for r := range replicas {
			repo := postgres.NewBillingRepo(pool)
			wg.Go(func() { errs[r] = repo.EnsureLedgerPartitions(ctx, from, days) })
		}
		wg.Wait()
		for r, err := range errs {
			if err != nil {
				t.Fatalf("round %d, replica %d: %v", round, r, err)
			}
		}
	}
	if got := ledgerPartitions(t, pool); len(got) != 1+rounds*days {
		t.Errorf("want DEFAULT + %d partitions, have %d: %v", rounds*days, len(got), got)
	}
}

func TestAWriteOutsideThePartitionsLandsInDefaultAndIsCounted(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)
	now := time.Now()

	if err := repo.EnsureLedgerPartitions(ctx, now, 2); err != nil {
		t.Fatalf("EnsureLedgerPartitions: %v", err)
	}
	if n, err := repo.LedgerDefaultRows(ctx); err != nil || n != 0 {
		t.Fatalf("LedgerDefaultRows on an empty DEFAULT = %d, %v; want 0", n, err)
	}

	customer := seedLedgerCustomer(t, pool)
	if _, applied, err := repo.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeCustomer, OwnerID: customer, Direction: cp.BillingDirectionMT,
		CustomerID: customer, EntryType: cp.EntryTopup, Credits: 10,
	}); err != nil || !applied {
		t.Fatalf("RecordDurable: applied=%v err=%v", applied, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO control_plane.billing_ledger
		  (owner_type, owner_id, direction, customer_id, entry_type, credits, balance_after, created_at)
		VALUES ('customer', $1, 'mt', $1, 'topup', 1, 1, $2)`, customer, now.AddDate(0, 0, 30)); err != nil {
		t.Fatalf("a write with no partition for its day failed: %v", err)
	}
	if n, err := repo.LedgerDefaultRows(ctx); err != nil || n != 1 {
		t.Errorf("LedgerDefaultRows = %d, %v; want 1", n, err)
	}
}

// TestEnsureLedgerPartitionsDoesNotWaitForAnOpenWriter: CREATE TABLE … PARTITION OF takes ACCESS EXCLUSIVE on
// the parent, which every hot-path COPY and INSERT holds in ROW EXCLUSIVE until it commits.
func TestEnsureLedgerPartitionsDoesNotWaitForAnOpenWriter(t *testing.T) {
	pool := freshLedgerDB(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)
	now := time.Now()

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
	if _, ok := ledgerPartitions(t, pool)[partitionName(now.AddDate(0, 0, 5))]; !ok {
		t.Error("the partition was not created")
	}
}
