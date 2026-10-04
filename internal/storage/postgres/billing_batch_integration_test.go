package postgres_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
)

type batchSizes struct {
	mu    sync.Mutex
	sizes []int
}

func (s *batchSizes) Observe(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sizes = append(s.sizes, int(v))
}

func (s *batchSizes) seen() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.sizes)
}

type recorded struct {
	balance int
	applied bool
	err     error
}

// hot builds a hot-path movement: a message and the balance Redis decided, the shape the batch accepts.
func (f deltaFixture) hot(messageID uuid.UUID, credits, balanceAfter int) cp.LedgerEntry {
	e := f.entry(cp.OwnerTypeCustomer, f.customerID, cp.EntryReserve, credits)
	e.MessageID = &messageID
	e.BalanceAfter = &balanceAfter
	return e
}

// stallFirstBatch holds the customer row so the first batch's ledger insert waits on its foreign-key check,
// and returns once that batch is stuck: every entry recorded until release queues behind it, in one batch.
func stallFirstBatch(t *testing.T, f deltaFixture, b *postgres.BillingBatcher) (release func(), first <-chan recorded) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM control_plane.customers WHERE id = $1 FOR UPDATE`, f.customerID); err != nil {
		t.Fatalf("lock customer: %v", err)
	}
	out := make(chan recorded, 1)
	go func() {
		bal, applied, err := b.RecordDurable(ctx, f.hot(uuid.New(), -1, 99))
		out <- recorded{bal, applied, err}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatalf("poll locks: %v", err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first batch never blocked on the customer row")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return func() { _ = tx.Rollback(ctx) }, out
}

// recordAll records every entry concurrently and waits until they are all queued, before release lets the
// stalled batch through. The queue is unbuffered, so a queued entry is a sender the writer has not taken yet.
func recordAll(b *postgres.BillingBatcher, release func(), entries ...cp.LedgerEntry) []recorded {
	out := make([]recorded, len(entries))
	var wg sync.WaitGroup
	for i, e := range entries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bal, applied, err := b.RecordDurable(context.Background(), e)
			out[i] = recorded{bal, applied, err}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	release()
	wg.Wait()
	return out
}

func newBatcher(t *testing.T, f deltaFixture) (*postgres.BillingBatcher, *batchSizes) {
	t.Helper()
	sizes := &batchSizes{}
	b := postgres.NewBillingBatcher(f.repo, sizes)
	t.Cleanup(b.Close)
	return b, sizes
}

func (f deltaFixture) ledgerRows(t *testing.T) (rows, sum int) {
	t.Helper()
	if err := f.pool.QueryRow(context.Background(),
		`SELECT count(*), coalesce(sum(credits), 0) FROM control_plane.billing_ledger WHERE customer_id = $1`,
		f.customerID).Scan(&rows, &sum); err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	return rows, sum
}

func TestBatcherWritesQueuedMovementsInOneTransaction(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	release, first := stallFirstBatch(t, f, b)
	entries := make([]cp.LedgerEntry, 20)
	for i := range entries {
		entries[i] = f.hot(uuid.New(), -2, 50-i)
	}
	got := recordAll(b, release, entries...)
	if r := <-first; r.err != nil || !r.applied {
		t.Fatalf("stalled entry: %+v", r)
	}

	for i, r := range got {
		if r.err != nil || !r.applied || r.balance != 50-i {
			t.Fatalf("entry %d: %+v, want applied with the balance Redis decided (%d)", i, r, 50-i)
		}
	}
	if s := sizes.seen(); !slices.Equal(s, []int{1, 20}) {
		t.Fatalf("batch sizes %v, want [1 20]: the queued movements must share one transaction", s)
	}
	if rows, sum := f.ledgerRows(t); rows != 21 || sum != -41 {
		t.Fatalf("ledger holds %d rows summing %d, want 21 rows summing -41", rows, sum)
	}
	if bal, _, err := f.repo.Balance(context.Background(), cp.OwnerTypeCustomer, f.customerID, cp.BillingDirectionMT); err != nil || bal != -41 {
		t.Fatalf("durable balance %d (%v), want the ledger's sum -41", bal, err)
	}
}

func TestBatcherAppliesADuplicateWithinABatchOnce(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	release, first := stallFirstBatch(t, f, b)
	messageID := uuid.New()
	got := recordAll(b, release, f.hot(messageID, -3, 10), f.hot(messageID, -3, 10))
	<-first

	if s := sizes.seen(); !slices.Equal(s, []int{1, 2}) {
		t.Fatalf("batch sizes %v, want [1 2]: both copies must land in the same batch", s)
	}
	applied := 0
	for _, r := range got {
		if r.err != nil {
			t.Fatalf("duplicate: %v", r.err)
		}
		if r.applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("%d copies applied, want exactly 1 (invariant c)", applied)
	}
	if rows, sum := f.ledgerRows(t); rows != 2 || sum != -4 {
		t.Fatalf("ledger holds %d rows summing %d, want 2 rows summing -4", rows, sum)
	}
}

func TestBatcherIsolatesAPoisonedMovement(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	release, first := stallFirstBatch(t, f, b)
	poisoned := f.hot(uuid.New(), -1, 0)
	poisoned.CustomerID = uuid.New() // no such customer: the ledger's foreign key refuses it
	got := recordAll(b, release, f.hot(uuid.New(), -1, 7), poisoned, f.hot(uuid.New(), -1, 6))
	<-first

	if s := sizes.seen(); !slices.Equal(s, []int{1, 3}) {
		t.Fatalf("batch sizes %v, want [1 3]: the poisoned movement must share the batch it spoils", s)
	}
	if got[1].err == nil {
		t.Fatal("the poisoned movement reported no error")
	}
	for _, i := range []int{0, 2} {
		if got[i].err != nil || !got[i].applied {
			t.Fatalf("healthy movement %d: %+v, want applied despite the poisoned neighbour", i, got[i])
		}
	}
	if rows, _ := f.ledgerRows(t); rows != 3 {
		t.Fatalf("ledger holds %d rows, want 3", rows)
	}
}

func TestBatcherWritesAMovementWithoutADecidedBalanceAlone(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	e := f.entry(cp.OwnerTypeCustomer, f.customerID, cp.EntryRelease, 5)
	messageID := uuid.New()
	e.MessageID = &messageID
	bal, applied, err := b.RecordDurable(context.Background(), e)
	if err != nil || !applied || bal != 5 {
		t.Fatalf("release: balance %d applied %v err %v, want 5 read from the durable balance", bal, applied, err)
	}
	if s := sizes.seen(); len(s) != 0 {
		t.Fatalf("batch sizes %v: a movement whose balance is read from the database must not be batched", s)
	}
}
