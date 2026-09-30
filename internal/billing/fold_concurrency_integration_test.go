package billing_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestStrictPrepaidNeverOverdrawsWhileFolding is the step-284 DoD under load: reserves race the fold and a
// cache that keeps expiring, so every few milliseconds a reserve rehydrates from a durable balance whose
// deltas are half folded. A strict-prepaid customer must never get one credit more than it paid for.
func TestStrictPrepaidNeverOverdrawsWhileFolding(t *testing.T) {
	const funded, workers, perWorker = 50, 20, 10
	h := newBillingHarness(t, funded)
	ctx := context.Background()

	stop := make(chan struct{})
	var background sync.WaitGroup
	tick := func(every time.Duration, fn func()) {
		background.Add(1)
		go func() {
			defer background.Done()
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					fn()
				}
			}
		}()
	}
	tick(time.Millisecond, func() {
		if _, err := h.verify.FoldOnce(ctx, 3); err != nil {
			t.Errorf("FoldOnce: %v", err)
		}
	})
	tick(5*time.Millisecond, func() {
		if err := h.rdb.Del(ctx, "billing:balance:mt:"+h.owner.Type+":"+h.owner.ID.String()).Err(); err != nil {
			t.Errorf("drop cache: %v", err)
		}
	})

	var accepted, failed atomic.Int64
	var reservers sync.WaitGroup
	for range workers {
		reservers.Add(1)
		go func() {
			defer reservers.Done()
			for range perWorker {
				_, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1)
				switch {
				case err == nil:
					accepted.Add(1)
				case !errors.Is(err, errs.ErrInsufficientCredit):
					// A cache dropped between rehydrate and retry refuses the reserve: fail-closed, not an overdraft.
					failed.Add(1)
				}
			}
		}()
	}
	reservers.Wait()
	close(stop)
	background.Wait()

	got := int(accepted.Load())
	if got > funded || got == 0 {
		t.Errorf("accepted %d reserves of 1 credit against %d funded (%d refused on a dropped cache), want 1..%d",
			got, funded, failed.Load(), funded)
	}
	if bal := h.balance(t); bal != funded-got {
		t.Errorf("durable balance = %d, want %d", bal, funded-got)
	}
	var sum int
	if err := pgtest.Pool(t).QueryRow(ctx,
		`SELECT coalesce(sum(credits), 0) FROM control_plane.billing_ledger WHERE owner_type = $1 AND owner_id = $2`,
		h.owner.Type, h.owner.ID).Scan(&sum); err != nil {
		t.Fatalf("sum ledger: %v", err)
	}
	if sum != funded-got {
		t.Errorf("SUM(ledger credits) = %d, want %d (it must equal the balance)", sum, funded-got)
	}
}

// blockingStore holds a reserve's durable commit until released, the way a slow Postgres does: the credit is
// already debited in Redis and not yet in the durable balance.
type blockingStore struct {
	billing.LedgerStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingStore) RecordDurable(ctx context.Context, entry cp.LedgerEntry) (int, bool, error) {
	blocked := false
	b.once.Do(func() { blocked = true })
	if blocked {
		close(b.entered)
		<-b.release
	}
	return b.LedgerStore.RecordDurable(ctx, entry)
}

// TestRehydrationSubtractsInFlightReserves: reserve A has taken the last credit in Redis and is still writing
// it durably when the cache expires. Rehydrating from the durable balance alone would sell that credit twice.
func TestRehydrationSubtractsInFlightReserves(t *testing.T) {
	h := newBillingHarness(t, 1)
	ctx := context.Background()
	store := &blockingStore{LedgerStore: h.repo, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { closeOnce(store.release) })
	acc := billing.New(h.rdb, store, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))

	first := make(chan error, 1)
	go func() {
		_, err := acc.Reserve(ctx, h.owner, uuid.New(), 1)
		first <- err
	}()
	select {
	case <-store.entered:
	case ferr := <-first:
		t.Fatalf("first Reserve returned before its durable write: %v", ferr)
	case <-time.After(10 * time.Second):
		t.Fatal("first Reserve never reached its durable write")
	}
	h.dropCachedBalance(t)

	_, err := acc.Reserve(ctx, h.owner, uuid.New(), 1)
	closeOnce(store.release)
	select {
	case ferr := <-first:
		if ferr != nil {
			t.Fatalf("first Reserve: %v", ferr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first Reserve never returned")
	}
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("second Reserve while the first is in flight = %v, want ErrInsufficientCredit", err)
	}
}

// TestRehydrationForgetsCommittedReserves: once a reserve is durable it leaves the in-flight set, or every
// rehydration would subtract it a second time and refuse credit the customer still has.
func TestRehydrationForgetsCommittedReserves(t *testing.T) {
	h := newBillingHarness(t, 2)
	ctx := context.Background()
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	h.dropCachedBalance(t)
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); err != nil {
		t.Fatalf("second Reserve with 1 credit left = %v, want success", err)
	}
}

// TestRehydrationIgnoresInFlightReservesPastTheHold: a field a crash left behind stops counting once the hold
// it stood for has lapsed, and is dropped.
func TestRehydrationIgnoresInFlightReservesPastTheHold(t *testing.T) {
	h := newBillingHarness(t, 1)
	ctx := context.Background()
	ikey := "billing:inflight:mt:" + h.owner.Type + ":" + h.owner.ID.String()
	crashed := uuid.NewString()
	longAgo := time.Now().Add(-2 * time.Minute).UnixMilli()
	if err := h.rdb.HSet(ctx, ikey, crashed, "1:"+strconv.FormatInt(longAgo, 10)).Err(); err != nil {
		t.Fatalf("seed stale field: %v", err)
	}

	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); err != nil {
		t.Fatalf("Reserve = %v, want success: the stale field must not count", err)
	}
	if left, err := h.rdb.HExists(ctx, ikey, crashed).Result(); err != nil || left {
		t.Errorf("stale field still present = %v (%v), want dropped", left, err)
	}
}

// stuckStore never commits a reserve: it returns only when the write's context ends.
type stuckStore struct{ billing.LedgerStore }

func (stuckStore) RecordDurable(ctx context.Context, _ cp.LedgerEntry) (int, bool, error) {
	<-ctx.Done()
	return 0, false, ctx.Err()
}

// TestReserveBoundsItsDurableWrite: a caller with no deadline must not let a reserve's durable write outlive
// the hold TTL, the age past which a rehydration stops subtracting it.
func TestReserveBoundsItsDurableWrite(t *testing.T) {
	h := newBillingHarness(t, 1)
	acc := billing.New(h.rdb, stuckStore{h.repo}, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))

	done := make(chan error, 1)
	go func() {
		_, err := acc.Reserve(context.Background(), h.owner, uuid.New(), 1)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Reserve on a durable write that never commits succeeded, want an error")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Reserve still waiting on its durable write after 15s: the write is unbounded")
	}
}

// slowRehydrationStore stalls the first balance read AFTER reading it, the way a rehydrating replica
// descheduled between its durable read and its SET NX does.
type slowRehydrationStore struct {
	billing.LedgerStore
	read    chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (s *slowRehydrationStore) Balance(ctx context.Context, ownerType string, ownerID uuid.UUID, direction string) (int, bool, error) {
	bal, found, err := s.LedgerStore.Balance(ctx, ownerType, ownerID, direction)
	first := false
	s.once.Do(func() { first = true })
	if first {
		close(s.read)
		<-s.proceed
	}
	return bal, found, err
}

// TestStaleRehydrationCannotResurrectSpentCredit: a rehydration computed before another replica's reserve
// must not land after that reserve's credit is gone, or the credit is sold twice.
func TestStaleRehydrationCannotResurrectSpentCredit(t *testing.T) {
	h := newBillingHarness(t, 1)
	ctx := context.Background()
	slow := &slowRehydrationStore{LedgerStore: h.repo, read: make(chan struct{}), proceed: make(chan struct{})}
	t.Cleanup(func() { closeOnce(slow.proceed) })
	stale := billing.New(h.rdb, slow, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))

	staleResult := make(chan error, 1)
	go func() {
		_, err := stale.Reserve(ctx, h.owner, uuid.New(), 1)
		staleResult <- err
	}()
	select {
	case <-slow.read:
	case <-time.After(10 * time.Second):
		t.Fatal("the stale replica never reached its durable read")
	}

	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); err != nil {
		t.Fatalf("the other replica's Reserve: %v", err)
	}
	h.dropCachedBalance(t)
	closeOnce(slow.proceed)

	select {
	case err := <-staleResult:
		if !errors.Is(err, errs.ErrInsufficientCredit) {
			t.Fatalf("stale replica's Reserve = %v, want ErrInsufficientCredit: the only credit is already spent", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stale replica's Reserve never returned")
	}
}

// TestStaleRehydrationCannotResurrectAnAdminDebit: the durable balance can also drop outside reserve.lua — an
// admin transfer. Its invalidation must fence a rehydration computed before it, like a debit does.
func TestStaleRehydrationCannotResurrectAnAdminDebit(t *testing.T) {
	h := newBillingHarness(t, 1)
	ctx := context.Background()
	slow := &slowRehydrationStore{LedgerStore: h.repo, read: make(chan struct{}), proceed: make(chan struct{})}
	t.Cleanup(func() { closeOnce(slow.proceed) })
	stale := billing.New(h.rdb, slow, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))

	staleResult := make(chan error, 1)
	go func() {
		_, err := stale.Reserve(ctx, h.owner, uuid.New(), 1)
		staleResult <- err
	}()
	select {
	case <-slow.read:
	case <-time.After(10 * time.Second):
		t.Fatal("the stale replica never reached its durable read")
	}

	if _, _, err := h.verify.Topup(ctx, cp.LedgerEntry{
		OwnerType: h.owner.Type, OwnerID: h.owner.ID, Direction: cp.BillingDirectionMT,
		CustomerID: h.owner.CustomerID, EntryType: cp.EntryAdjustment, Credits: -1,
	}); err != nil {
		t.Fatalf("admin debit: %v", err)
	}
	if err := billing.InvalidateBalanceCaches(ctx, h.rdb,
		billing.BalanceCacheKey(cp.BillingDirectionMT, h.owner.Type, h.owner.ID)); err != nil {
		t.Fatalf("InvalidateBalanceCaches: %v", err)
	}
	closeOnce(slow.proceed)

	select {
	case err := <-staleResult:
		if !errors.Is(err, errs.ErrInsufficientCredit) {
			t.Fatalf("stale replica's Reserve = %v, want ErrInsufficientCredit: the admin took the only credit", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stale replica's Reserve never returned")
	}
}

// closeOnce releases a blocked double from the test body or, if the test failed first, from its cleanup.
func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
