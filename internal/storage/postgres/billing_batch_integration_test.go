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

func record(ctx context.Context, b *postgres.BillingBatcher, e cp.LedgerEntry) <-chan recorded {
	out := make(chan recorded, 1)
	go func() {
		bal, applied, err := b.RecordDurable(ctx, e)
		out <- recorded{bal, applied, err}
	}()
	return out
}

// hot builds a hot-path movement: a message and the balance Redis decided, the shape the batch accepts.
func (f deltaFixture) hot(messageID uuid.UUID, credits, balanceAfter int) cp.LedgerEntry {
	e := f.entry(cp.OwnerTypeCustomer, f.customerID, cp.EntryReserve, credits)
	e.MessageID = &messageID
	e.BalanceAfter = &balanceAfter
	return e
}

// holdCustomer locks the customer row: a batch writing its ledger then waits on the foreign-key check, and
// every movement recorded meanwhile queues behind it. The lock is released by the returned func, or at cleanup.
func holdCustomer(t *testing.T, f deltaFixture) (release func(), waitBlocked func()) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	release = func() { _ = tx.Rollback(ctx) }
	t.Cleanup(release)
	var holder int
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
		t.Fatalf("holder pid: %v", err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM control_plane.customers WHERE id = $1 FOR UPDATE`, f.customerID); err != nil {
		t.Fatalf("lock customer: %v", err)
	}
	waitBlocked = func() {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			var blocked int
			if err := f.pool.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))`, holder).Scan(&blocked); err != nil {
				t.Fatalf("poll blocked sessions: %v", err)
			}
			if blocked > 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("no batch ever blocked on the customer row")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return release, waitBlocked
}

// stallFirstBatch sends one movement and returns once its batch is stuck on the customer row.
func stallFirstBatch(t *testing.T, f deltaFixture, b *postgres.BillingBatcher) (release func(), first <-chan recorded) {
	t.Helper()
	release, waitBlocked := holdCustomer(t, f)
	first = record(context.Background(), b, f.hot(uuid.New(), -1, 99))
	waitBlocked()
	return release, first
}

// recordAll records every entry concurrently behind a stalled batch and releases it once they are all queued,
// so they leave in as few batches as the cap allows.
func recordAll(t *testing.T, b *postgres.BillingBatcher, release func(), entries ...cp.LedgerEntry) []recorded {
	t.Helper()
	pending := make([]<-chan recorded, len(entries))
	for i, e := range entries {
		pending[i] = record(context.Background(), b, e)
	}
	deadline := time.Now().Add(10 * time.Second)
	for b.Queued() < len(entries) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d movements queued", b.Queued(), len(entries))
		}
		time.Sleep(time.Millisecond)
	}
	release()
	out := make([]recorded, len(entries))
	for i, p := range pending {
		out[i] = <-p
	}
	return out
}

// await fails the test instead of hanging it when an answer never comes.
func await(t *testing.T, ch <-chan recorded) recorded {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(15 * time.Second):
		t.Fatal("no answer within 15s")
		return recorded{}
	}
}

func newBatcher(t *testing.T, f deltaFixture) (*postgres.BillingBatcher, *batchSizes) {
	t.Helper()
	sizes := &batchSizes{}
	b := postgres.NewBillingBatcher(f.repo, sizes)
	t.Cleanup(b.Close)
	return b, sizes
}

func mustApply(t *testing.T, what string, r recorded) {
	t.Helper()
	if r.err != nil || !r.applied {
		t.Fatalf("%s: %+v, want applied", what, r)
	}
}

func orNil(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
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

func (f deltaFixture) ledgerHas(t *testing.T, messageID uuid.UUID) bool {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM control_plane.billing_ledger WHERE message_id = $1`, messageID).Scan(&n); err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	return n > 0
}

func (f deltaFixture) mtBalance(t *testing.T) int {
	t.Helper()
	bal, _, err := f.repo.Balance(context.Background(), cp.OwnerTypeCustomer, f.customerID, cp.BillingDirectionMT)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	return bal
}

func TestBatcherWritesQueuedMovementsInOneTransactionUpToItsCap(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	release, first := stallFirstBatch(t, f, b)
	entries := make([]cp.LedgerEntry, 300)
	for i := range entries {
		// Captures, not reserves: the reaper's test caps the unsettled reserves the shared database may hold.
		entries[i] = f.hot(uuid.New(), -2, 1000-i)
		entries[i].EntryType = cp.EntryCapture
	}
	got := recordAll(t, b, release, entries...)
	mustApply(t, "stalled movement", <-first)

	for i, r := range got {
		if r.err != nil || !r.applied || r.balance != 1000-i {
			t.Fatalf("movement %d: %+v, want applied with the balance Redis decided (%d)", i, r, 1000-i)
		}
	}
	if s := sizes.seen(); !slices.Equal(s, []int{1, 256, 44}) {
		t.Fatalf("batch sizes %v, want [1 256 44]", s)
	}
	// now() is the transaction's start: one distinct created_at per transaction that wrote ledger rows.
	var transactions int
	if err := f.pool.QueryRow(context.Background(),
		`SELECT count(DISTINCT created_at) FROM control_plane.billing_ledger WHERE customer_id = $1`,
		f.customerID).Scan(&transactions); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if transactions != 3 {
		t.Fatalf("ledger written by %d transactions, want 3: one per batch", transactions)
	}
	if rows, sum := f.ledgerRows(t); rows != 301 || sum != -601 {
		t.Fatalf("ledger holds %d rows summing %d, want 301 rows summing -601", rows, sum)
	}
	if bal := f.mtBalance(t); bal != -601 {
		t.Fatalf("durable balance %d, want the ledger's sum -601", bal)
	}
}

func TestBatcherWritesEveryColumnOfItsMovements(t *testing.T) {
	f := newDeltaFixture(t, "smpp_account")
	b, _ := newBatcher(t, f)
	account := f.account(t)
	ref := "batch-ref"

	release, first := stallFirstBatch(t, f, b)
	mt := f.entry(cp.OwnerTypeSMPPAccount, account, cp.EntryReserve, -3)
	mtID, mtAfter := uuid.New(), 17
	mt.MessageID, mt.BalanceAfter, mt.Reference = &mtID, &mtAfter, &ref
	mo := f.entry(cp.OwnerTypeSMPPAccount, account, cp.EntryMOCharge, -5)
	moID, moAfter := uuid.New(), -5
	mo.Direction, mo.MessageID, mo.BalanceAfter = cp.BillingDirectionMO, &moID, &moAfter
	got := recordAll(t, b, release, mt, mo)
	mustApply(t, "stalled movement", <-first)
	mustApply(t, "mt", got[0])
	mustApply(t, "mo", got[1])

	for _, want := range []struct {
		id        uuid.UUID
		direction string
		credits   int
		after     int
		ref       *string
	}{{mtID, cp.BillingDirectionMT, -3, 17, &ref}, {moID, cp.BillingDirectionMO, -5, -5, nil}} {
		var direction, ownerType string
		var ownerID uuid.UUID
		var accountID *uuid.UUID
		var credits, after int
		var reference *string
		if err := f.pool.QueryRow(context.Background(),
			`SELECT direction, owner_type, owner_id, account_id, credits, balance_after, reference
			 FROM control_plane.billing_ledger WHERE message_id = $1`, want.id).
			Scan(&direction, &ownerType, &ownerID, &accountID, &credits, &after, &reference); err != nil {
			t.Fatalf("read %s row: %v", want.direction, err)
		}
		if direction != want.direction || ownerType != cp.OwnerTypeSMPPAccount || ownerID != account ||
			accountID == nil || *accountID != account || credits != want.credits || after != want.after ||
			orNil(reference) != orNil(want.ref) {
			t.Fatalf("%s row: direction %s owner %s/%s account %v credits %d after %d reference %v",
				want.direction, direction, ownerType, ownerID, accountID, credits, after, orNil(reference))
		}
		bal, _, err := f.repo.Balance(context.Background(), cp.OwnerTypeSMPPAccount, account, want.direction)
		if err != nil || bal != want.credits {
			t.Fatalf("%s balance %d (%v), want %d: the delta must land on its own direction", want.direction, bal, err, want.credits)
		}
	}
}

func TestBatcherAppliesADuplicateWithinABatchOnce(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	release, first := stallFirstBatch(t, f, b)
	messageID := uuid.New()
	got := recordAll(t, b, release, f.hot(messageID, -3, 10), f.hot(messageID, -3, 10))
	mustApply(t, "stalled movement", <-first)

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

func TestBatcherAnswersAReplayWithTheDurableBalance(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)
	replayed := uuid.New()
	if _, applied, err := f.repo.RecordDurable(context.Background(), f.hot(replayed, -4, 6)); err != nil || !applied {
		t.Fatalf("first recording: applied %v err %v", applied, err)
	}

	release, first := stallFirstBatch(t, f, b)
	got := recordAll(t, b, release, f.hot(replayed, -4, 6), f.hot(uuid.New(), -1, 5))
	mustApply(t, "stalled movement", <-first)

	if s := sizes.seen(); !slices.Equal(s, []int{1, 2}) {
		t.Fatalf("batch sizes %v, want [1 2]", s)
	}
	mustApply(t, "fresh neighbour", got[1])
	// Read before this batch's own deltas land: the original reserve and the stalled one.
	if got[0].err != nil || got[0].applied || got[0].balance != -5 {
		t.Fatalf("replay: %+v, want not applied and the durable balance -5", got[0])
	}
	if rows, sum := f.ledgerRows(t); rows != 3 || sum != -6 {
		t.Fatalf("ledger holds %d rows summing %d, want 3 rows summing -6: the replay must not debit twice", rows, sum)
	}
}

func TestBatcherIsolatesAPoisonedMovement(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	release, first := stallFirstBatch(t, f, b)
	poisoned := f.hot(uuid.New(), -1, 0)
	poisoned.CustomerID = uuid.New() // no such customer: the ledger's foreign key refuses it
	got := recordAll(t, b, release, f.hot(uuid.New(), -1, 7), poisoned, f.hot(uuid.New(), -1, 6))
	mustApply(t, "stalled movement", <-first)

	if s := sizes.seen(); !slices.Equal(s, []int{1, 3}) {
		t.Fatalf("batch sizes %v, want [1 3]: the poisoned movement must share the batch it spoils", s)
	}
	if got[1].err == nil {
		t.Fatal("the poisoned movement reported no error")
	}
	mustApply(t, "healthy movement 0", got[0])
	mustApply(t, "healthy movement 2", got[2])
	if rows, _ := f.ledgerRows(t); rows != 3 {
		t.Fatalf("ledger holds %d rows, want 3", rows)
	}
}

// A COMMIT that fails may have landed: replaying its movements one by one would answer applied=false for
// movements that were in fact applied, and the reserve would then refund a debit the ledger keeps.
func TestBatcherDoesNotReplayABatchWhoseCommitFailed(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, _ := newBatcher(t, f)
	refused := uuid.New()
	ctx := context.Background()
	trigger := "refuse_at_commit_" + refused.String()[:8]
	for _, stmt := range []string{
		`CREATE FUNCTION control_plane.` + trigger + `() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused at commit'; END $$`,
		`CREATE CONSTRAINT TRIGGER ` + trigger + ` AFTER INSERT ON control_plane.billing_idempotency
		 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.message_id = '` + refused.String() + `')
		 EXECUTE FUNCTION control_plane.` + trigger + `()`,
	} {
		if _, err := f.pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("install commit refusal: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(ctx, `DROP TRIGGER `+trigger+` ON control_plane.billing_idempotency`)
		_, _ = f.pool.Exec(ctx, `DROP FUNCTION control_plane.`+trigger+`()`)
	})

	release, first := stallFirstBatch(t, f, b)
	healthy := []uuid.UUID{uuid.New(), uuid.New()}
	got := recordAll(t, b, release, f.hot(healthy[0], -1, 7), f.hot(refused, -1, 6), f.hot(healthy[1], -1, 5))
	mustApply(t, "stalled movement", <-first)

	for i, r := range got {
		if r.err == nil {
			t.Fatalf("movement %d: %+v, want the commit's error", i, r)
		}
	}
	for _, id := range healthy {
		if f.ledgerHas(t, id) {
			t.Fatal("a movement of the refused batch was written: the batch was replayed one by one")
		}
	}
}

func TestBatcherNeverWritesAMovementPastItsCallersDeadline(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, _ := newBatcher(t, f)

	release, waitBlocked := holdCustomer(t, f)
	late := uuid.New()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := record(ctx, b, f.hot(late, -1, 9))
	waitBlocked()

	r := <-done
	if r.err == nil {
		t.Fatalf("%+v, want the deadline's error", r)
	}
	if waited := time.Since(started); waited > 2*time.Second {
		t.Fatalf("answered after %v: the batch must end at its member's deadline, not at its own bound", waited)
	}
	release()
	mustApply(t, "next movement", <-record(context.Background(), b, f.hot(uuid.New(), -1, 8)))
	if f.ledgerHas(t, late) {
		t.Fatal("the movement was written after its caller had been told it failed")
	}
}

// A member's deadline ends its whole batch; the neighbours it took down are replayed under their own contexts.
func TestBatcherReplaysTheNeighboursOfAnExpiredMovement(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	other := deltaFixture{pool: f.pool, repo: f.repo}
	if err := f.pool.QueryRow(context.Background(),
		`INSERT INTO control_plane.customers (name) VALUES ('billing-batch-neighbour') RETURNING id`).Scan(&other.customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	b, sizes := newBatcher(t, f)

	releaseFirst, first := stallFirstBatch(t, f, b)
	releaseOther, waitOther := holdCustomer(t, other)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	short, patient := uuid.New(), uuid.New()
	expired := record(ctx, b, other.hot(short, -1, 9))
	kept := record(context.Background(), b, other.hot(patient, -1, 8))
	for b.Queued() < 2 {
		time.Sleep(time.Millisecond)
	}
	releaseFirst()
	mustApply(t, "stalled movement", <-first)
	waitOther()

	if r := await(t, expired); r.err == nil {
		t.Fatalf("expired movement: %+v, want the deadline's error", r)
	}
	releaseOther()
	mustApply(t, "patient neighbour", await(t, kept))

	if s := sizes.seen(); !slices.Equal(s, []int{1, 2}) {
		t.Fatalf("batch sizes %v, want [1 2]: both must have shared the batch the deadline ended", s)
	}
	if other.ledgerHas(t, short) {
		t.Fatal("the expired movement was written")
	}
}

func TestBatcherDropsAMovementWhoseCallerLeftWhileItQueued(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	release, first := stallFirstBatch(t, f, b)
	gone := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	done := record(ctx, b, f.hot(gone, -1, 9))
	for b.Queued() < 1 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if r := <-done; r.err == nil {
		t.Fatalf("%+v, want the cancellation's error", r)
	}
	release()
	mustApply(t, "stalled movement", <-first)
	mustApply(t, "next movement", <-record(context.Background(), b, f.hot(uuid.New(), -1, 8)))

	if f.ledgerHas(t, gone) {
		t.Fatal("a movement whose caller left before it was taken was written")
	}
	if s := sizes.seen(); !slices.Equal(s, []int{1, 1}) {
		t.Fatalf("batch sizes %v, want [1 1]", s)
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

// A caller that gives up once its movement is in a batch must still learn what happened to it: the batch
// may commit, and the terminal lock and the reserve's lost-commit check act on the answer.
func TestBatcherAnswersACancelledCallerWithItsBatchOutcome(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, _ := newBatcher(t, f)

	release, waitBlocked := holdCustomer(t, f)
	taken := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	done := record(ctx, b, f.hot(taken, -1, 9))
	waitBlocked()
	cancel()
	select {
	case r := <-done:
		t.Fatalf("answered %+v while its batch was still writing", r)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	mustApply(t, "cancelled caller", await(t, done))
	if !f.ledgerHas(t, taken) {
		t.Fatal("the batch outcome reported is not what landed")
	}
}

func TestBatcherDoesNotHandOverAMovementWhoseDeadlinePassed(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, sizes := newBatcher(t, f)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	for range 20 {
		if _, _, err := b.RecordDurable(ctx, f.hot(uuid.New(), -1, 9)); err == nil {
			t.Fatal("a movement past its deadline was recorded")
		}
	}
	if s := sizes.seen(); len(s) != 0 {
		t.Fatalf("batch sizes %v: a movement past its deadline condemns any batch it joins", s)
	}
}

func TestBatcherBoundsAMovementWithoutADeadline(t *testing.T) {
	f := newDeltaFixture(t, "customer")
	b, _ := newBatcher(t, f)

	_, waitBlocked := holdCustomer(t, f)
	unbounded := uuid.New()
	started := time.Now()
	done := record(context.Background(), b, f.hot(unbounded, -1, 9))
	waitBlocked()

	if r := await(t, done); r.err == nil {
		t.Fatalf("%+v, want the batch bound's error while the row stays locked", r)
	}
	if waited := time.Since(started); waited > 6*time.Second {
		t.Fatalf("answered after %v: a movement without a deadline must still be bounded", waited)
	}
}
