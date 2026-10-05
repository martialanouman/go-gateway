package billing_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestStrictPrepaidNeverOverdrawsWhileFolding is the step-284 DoD under load: reserves race the fold and a
// cache that keeps expiring, so every few milliseconds a reserve rehydrates from a durable balance whose
// deltas are half folded. A strict-prepaid customer must never get one credit more than it paid for.
// Since step-286 workers come in pairs reserving the same messages, and admin transfers drain the same
// balance concurrently.
func TestStrictPrepaidNeverOverdrawsWhileFolding(t *testing.T) {
	const funded, workers, perWorker, transfers = 50, 20, 10, 20
	h := newBillingHarness(t, funded)
	ctx := context.Background()
	sink := seedAccount(t, h.owner.CustomerID)
	messages := make([]uuid.UUID, workers/2*perWorker)
	for i := range messages {
		messages[i] = uuid.New()
	}

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

	var accepted sync.Map
	var failed, moved atomic.Int64
	var reservers sync.WaitGroup
	firstTransfer := make(chan struct{})
	for w := range workers {
		reservers.Add(1)
		go func() {
			defer reservers.Done()
			<-firstTransfer
			for _, messageID := range messages[w/2*perWorker : (w/2+1)*perWorker] {
				_, err := h.acc.Reserve(ctx, h.owner, messageID, 1)
				switch {
				case err == nil:
					accepted.Store(messageID, true)
				case !errors.Is(err, errs.ErrInsufficientCredit):
					// A cache dropped between rehydrate and retry refuses the reserve: fail-closed, not an overdraft.
					failed.Add(1)
				}
			}
		}()
	}
	reservers.Add(1)
	go func() {
		defer reservers.Done()
		for range transfers {
			key := uuid.New()
			debit, credit := transferLegs(h, sink, key, 1)
			err := h.acc.DebitTransfer(ctx, h.owner, key, 1, func(ctx context.Context) (bool, error) {
				_, applied, err := h.repo.Transfer(ctx, debit, credit, key)
				return applied, err
			})
			closeOnce(firstTransfer)
			switch {
			case err == nil:
				moved.Add(1)
			case !errors.Is(err, errs.ErrInsufficientCredit):
				failed.Add(1)
			}
		}
	}()
	reservers.Wait()
	close(stop)
	background.Wait()

	var reserved int
	accepted.Range(func(any, any) bool { reserved++; return true })
	got := reserved + int(moved.Load())
	if got > funded || reserved == 0 || moved.Load() == 0 {
		t.Errorf("accepted %d reserves and %d transfers of 1 credit against %d funded (%d refused on a dropped cache), want both > 0 and at most %d in all",
			reserved, moved.Load(), funded, failed.Load(), funded)
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

// raceDuplicateRepair reserves messageID twice: attempt A debits the cache and is still writing its reserve
// when its duplicate B finds A's hold, sees no durable entry yet, and repairs it; A then loses the claim.
func raceDuplicateRepair(t *testing.T, h *billingHarness, messageID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	store := &blockingStore{LedgerStore: h.repo, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { closeOnce(store.release) })
	original := billing.New(h.rdb, store, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))

	first := make(chan error, 1)
	go func() {
		_, err := original.Reserve(ctx, h.owner, messageID, 1)
		first <- err
	}()
	select {
	case <-store.entered:
	case ferr := <-first:
		t.Fatalf("original Reserve returned before its durable write: %v", ferr)
	case <-time.After(10 * time.Second):
		t.Fatal("original Reserve never reached its durable write")
	}
	if _, err := h.acc.Reserve(ctx, h.owner, messageID, 1); err != nil {
		t.Fatalf("duplicate Reserve: %v", err)
	}
	closeOnce(store.release)
	select {
	case ferr := <-first:
		if ferr != nil {
			t.Fatalf("original Reserve: %v", ferr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("original Reserve never returned")
	}
}

// TestConcurrentDuplicateRepairLeavesNoPhantomCredit: refunding the cache for the original attempt's lost
// claim would hand back a credit the ledger keeps debited.
func TestConcurrentDuplicateRepairLeavesNoPhantomCredit(t *testing.T) {
	h := newBillingHarness(t, 1)
	raceDuplicateRepair(t, h, uuid.New())

	if bal := h.balance(t); bal != 0 {
		t.Fatalf("durable balance = %d, want 0: the one credit is reserved", bal)
	}
	if _, err := h.acc.Reserve(context.Background(), h.owner, uuid.New(), 1); !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve of another message = %v, want ErrInsufficientCredit: the only credit is reserved", err)
	}
}

// TestRepairMarkIsConsumedByTheUndo: the mark spares one undo, not every undo of the message for the hold
// TTL. A replay after the capture debits the cache again and must still be refunded.
func TestRepairMarkIsConsumedByTheUndo(t *testing.T) {
	h := newBillingHarness(t, 2)
	ctx := context.Background()
	messageID := uuid.New()
	raceDuplicateRepair(t, h, messageID)
	if _, err := h.acc.Capture(ctx, h.owner, messageID); err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if _, err := h.acc.Reserve(ctx, h.owner, messageID, 1); err != nil {
		t.Fatalf("Reserve replayed after the capture: %v", err)
	}
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); err != nil {
		t.Fatalf("Reserve of the credit left = %v, want success: the replay's debit must have been refunded", err)
	}
}

// TestRepairCoversItsOwnCommit: the original attempt's in-flight field is past the hold age and purged, while
// its hold still stands and a duplicate is writing the repair. Until that commit lands, only the repair's
// own field keeps a rehydration from selling the credit again.
func TestRepairCoversItsOwnCommit(t *testing.T) {
	h := newBillingHarness(t, 1)
	ctx := context.Background()
	messageID := uuid.New()
	ikey := "billing:inflight:mt:" + h.owner.Type + ":" + h.owner.ID.String()
	longAgo := time.Now().Add(-2 * time.Minute).UnixMilli()
	if err := h.rdb.Set(ctx, "billing:balance:mt:"+h.owner.Type+":"+h.owner.ID.String(), 0, time.Minute).Err(); err != nil {
		t.Fatalf("seed debited cache: %v", err)
	}
	if err := h.rdb.Set(ctx, "billing:reservation:"+messageID.String(), 1, time.Minute).Err(); err != nil {
		t.Fatalf("seed hold: %v", err)
	}
	if err := h.rdb.HSet(ctx, ikey, messageID.String(), "1:"+strconv.FormatInt(longAgo, 10)).Err(); err != nil {
		t.Fatalf("seed stale field: %v", err)
	}

	store := &blockingStore{LedgerStore: h.repo, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { closeOnce(store.release) })
	duplicate := billing.New(h.rdb, store, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))
	repaired := make(chan error, 1)
	go func() {
		_, err := duplicate.Reserve(ctx, h.owner, messageID, 1)
		repaired <- err
	}()
	select {
	case <-store.entered:
	case err := <-repaired:
		t.Fatalf("duplicate Reserve returned before its repair: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("duplicate Reserve never reached its repair")
	}
	h.dropCachedBalance(t)

	_, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1)
	closeOnce(store.release)
	if rerr := <-repaired; rerr != nil {
		t.Fatalf("duplicate Reserve: %v", rerr)
	}
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve while the repair commits = %v, want ErrInsufficientCredit", err)
	}
}

// failingReserveStore holds the first reserve's durable write, then fails it for real (nothing committed).
type failingReserveStore struct {
	billing.LedgerStore
	entered chan struct{}
	fail    chan struct{}
}

func (s *failingReserveStore) RecordDurable(context.Context, cp.LedgerEntry) (int, bool, error) {
	closeOnce(s.entered)
	<-s.fail
	return 0, false, errors.New("postgres unreachable")
}

// beforeCommandHook runs fn once, just before the first command that matches reaches Redis.
type beforeCommandHook struct {
	match func(redis.Cmder) bool
	once  sync.Once
	fn    func()
}

func (*beforeCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *beforeCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.match(cmd) {
			h.once.Do(h.fn)
		}
		return next(ctx, cmd)
	}
}

func (*beforeCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func isRepairScript(cmd redis.Cmder) bool {
	for _, arg := range cmd.Args() {
		if s, ok := arg.(string); ok && strings.Contains(s, ":repair") {
			return true
		}
	}
	return false
}

// TestRepairYieldsToAnUndoneHold: the duplicate saw the original attempt's hold, but before it repairs, that
// attempt's durable write fails for real and its undo refunds the cache. A repair written now would debit
// the ledger for a credit the cache has handed back.
func TestRepairYieldsToAnUndoneHold(t *testing.T) {
	h := newBillingHarness(t, 1)
	ctx := context.Background()
	messageID := uuid.New()
	store := &failingReserveStore{LedgerStore: h.repo, entered: make(chan struct{}), fail: make(chan struct{})}
	t.Cleanup(func() { closeOnce(store.fail) })
	original := billing.New(h.rdb, store, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))

	first := make(chan error, 1)
	go func() {
		_, err := original.Reserve(ctx, h.owner, messageID, 1)
		first <- err
	}()
	select {
	case <-store.entered:
	case err := <-first:
		t.Fatalf("original Reserve returned before its durable write: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("original Reserve never reached its durable write")
	}

	hooked := redis.NewClient(h.rdb.Options())
	t.Cleanup(func() { _ = hooked.Close() })
	var fired atomic.Bool
	hooked.AddHook(&beforeCommandHook{match: isRepairScript, fn: func() {
		fired.Store(true)
		closeOnce(store.fail)
		select {
		case err := <-first:
			if err == nil {
				t.Error("original Reserve on a failed durable write succeeded")
			}
		case <-time.After(10 * time.Second):
			t.Error("original Reserve never undid its hold")
		}
	}})
	duplicate := billing.New(hooked, h.repo, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))
	if _, err := duplicate.Reserve(ctx, h.owner, messageID, 1); err != nil {
		t.Fatalf("duplicate Reserve: %v", err)
	}

	if !fired.Load() {
		t.Fatal("the duplicate never ran its repair script: the undo it must yield to never happened")
	}
	if bal := h.balance(t); bal != 0 {
		t.Fatalf("durable balance = %d, want 0: the duplicate holds the one credit", bal)
	}
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve of another message = %v, want ErrInsufficientCredit", err)
	}
}

// TestUndoneAttemptKeepsItsHandsOffARetry: attempt A failed durably and undid its debit; before A clears its
// in-flight field, a retry B of the same message reserves anew and starts writing. A clearing a field B now
// relies on would let a rehydration sell B's credit again.
func TestUndoneAttemptKeepsItsHandsOffARetry(t *testing.T) {
	h := newBillingHarness(t, 1)
	ctx := context.Background()
	messageID := uuid.New()
	retryStore := &blockingStore{LedgerStore: h.repo, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { closeOnce(retryStore.release) })
	retry := billing.New(h.rdb, retryStore, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))

	retried := make(chan error, 1)
	var fired atomic.Bool
	hooked := redis.NewClient(h.rdb.Options())
	t.Cleanup(func() { _ = hooked.Close() })
	hooked.AddHook(&beforeCommandHook{match: func(cmd redis.Cmder) bool { return cmd.Name() == "hdel" }, fn: func() {
		fired.Store(true)
		go func() {
			_, err := retry.Reserve(ctx, h.owner, messageID, 1)
			retried <- err
		}()
		select {
		case <-retryStore.entered:
		case err := <-retried:
			t.Errorf("retry returned before its durable write: %v", err)
		case <-time.After(10 * time.Second):
			t.Error("retry never reached its durable write")
		}
	}})
	failing := &failingReserveStore{LedgerStore: h.repo, entered: make(chan struct{}), fail: make(chan struct{})}
	closeOnce(failing.fail)
	original := billing.New(hooked, failing, billing.WithHoldTTL(time.Minute), billing.WithConfigSource(h.cfg))
	if _, err := original.Reserve(ctx, h.owner, messageID, 1); err == nil {
		t.Fatal("original Reserve on a failed durable write succeeded")
	}
	if !fired.Load() {
		t.Fatal("the original attempt never cleared its in-flight field")
	}

	h.dropCachedBalance(t)
	_, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1)
	closeOnce(retryStore.release)
	if rerr := <-retried; rerr != nil {
		t.Fatalf("retry Reserve: %v", rerr)
	}
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve while the retry is still writing = %v, want ErrInsufficientCredit", err)
	}
}

// TestDuplicateAfterTheCommitLeavesNoMark: most duplicates arrive once the original reserve is durable and
// repair nothing. Marking the hold anyway would spare a later replay's undo, which must refund.
func TestDuplicateAfterTheCommitLeavesNoMark(t *testing.T) {
	h := newBillingHarness(t, 2)
	ctx := context.Background()
	messageID := uuid.New()
	for range 2 {
		if _, err := h.acc.Reserve(ctx, h.owner, messageID, 1); err != nil {
			t.Fatalf("Reserve: %v", err)
		}
	}
	if _, err := h.acc.Capture(ctx, h.owner, messageID); err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if _, err := h.acc.Reserve(ctx, h.owner, messageID, 1); err != nil {
		t.Fatalf("Reserve replayed after the capture: %v", err)
	}
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); err != nil {
		t.Fatalf("Reserve of the credit left = %v, want success: the replay's debit must have been refunded", err)
	}
}
