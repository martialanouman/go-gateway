package billing_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestConcurrentReservesSameOwner is the concurrency guard for the durable balance model (the bug the
// review caught): N reserves for the SAME owner run at once, each a distinct message. Because the durable
// balance is applied as a signed DELTA (not an absolute set), the final balance is exactly the sum of
// every debit whatever order the transactions commit in, and equals SUM(credits) in the ledger. An
// absolute-write model would let a stale commit clobber a fresher one and lose debits — silent
// under-billing.
func TestConcurrentReservesSameOwner(t *testing.T) {
	const n, each = 20, 5
	h := newBillingHarness(t, 1000)
	ctx := context.Background()

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), each); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent Reserve: %v", err)
	}

	want := 1000 - n*each // 900
	if got := h.balance(t); got != want {
		t.Errorf("durable balance after %d concurrent reserves = %d, want %d (no lost debits)", n, got, want)
	}
	// The ledger's SUM(credits) must equal the balance — the audit invariant, order-independent.
	var sum int
	pool := pgtest.Pool(t)
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(sum(credits), 0) FROM control_plane.billing_ledger WHERE owner_type = 'customer' AND owner_id = $1`,
		h.owner.ID).Scan(&sum); err != nil {
		t.Fatalf("sum ledger: %v", err)
	}
	if sum != want {
		t.Errorf("SUM(credits) = %d, want %d (ledger and balance must agree)", sum, want)
	}
}

// TestConcurrentCaptureReleaseYields is the guard for the terminal-outcome race (the review's TOCTOU bug):
// a single reserved message is captured AND released at the same instant. The terminal lock must let
// exactly ONE win — either the credits are charged (balance stays debited, one capture entry, no release)
// OR they are refunded (balance restored, one release entry, no capture) — never both, which would either
// double-count or leave the balance inconsistent with the ledger.
func TestConcurrentCaptureReleaseYields(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	if _, err := h.acc.Reserve(ctx, h.owner, msg, 7); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if h.balance(t) != 93 {
		t.Fatalf("balance after reserve = %d, want 93", h.balance(t))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		if _, err := h.acc.Capture(ctx, h.owner, msg); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		if err := h.acc.Release(ctx, h.owner, msg); err != nil {
			errCh <- err
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent Capture/Release: %v", err)
	}

	captures := h.ledgerCount(t, cp.EntryCapture, msg)
	releases := h.ledgerCount(t, cp.EntryRelease, msg)
	// Exactly one terminal outcome — captured XOR released.
	if captures+releases != 1 {
		t.Fatalf("terminal entries = %d capture + %d release, want exactly one (capture XOR release)", captures, releases)
	}
	// The balance must agree with which terminal won: captured → still debited (93); released → refunded (100).
	if captures == 1 && h.balance(t) != 93 {
		t.Errorf("capture won but balance = %d, want 93 (charge stands)", h.balance(t))
	}
	if releases == 1 && h.balance(t) != 100 {
		t.Errorf("release won but balance = %d, want 100 (refunded)", h.balance(t))
	}
	// The cache must not diverge from the durable balance (the BLOQUANT-1 guard): if a live-cache refund
	// by release.lua lost the durable race to capture, the cache is dropped so it rehydrates correctly.
	if cached, present := h.cachedBalance(t); present && cached != h.balance(t) {
		t.Errorf("cache = %d diverges from durable balance = %d after the race", cached, h.balance(t))
	}
}

// TestReleaseYieldsToCaptureReconcilesCache is the deterministic BLOQUANT-1 guard: a live-hold release
// whose durable resolution yields to a capture that already won must NOT leave the cache showing a refund
// the durable authority never applied. We force the interleaving: reserve (cache debited, hold live), then
// record the capture durably (capture wins), then Release — release.lua refunds the LIVE cache, but the
// durable release yields; the cache must be reconciled (dropped) so it no longer diverges.
func TestReleaseYieldsToCaptureReconcilesCache(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	if _, err := h.acc.Reserve(ctx, h.owner, msg, 6); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// Capture wins DURABLY first (a late-DLR capture landing before the release resolves), written straight
	// to the ledger rather than through Accountant.Capture. That distinction IS the test: capture.lua deletes
	// the hold, and with the hold gone release.lua answers "no_reservation" and never touches the cached
	// balance — so the phantom refund this guard exists to catch could not arise, and the guard would pass
	// while guarding nothing. Found in step-260b by mutating the reconciliation away and watching this test
	// stay green. The capture entry carries a zero delta: a capture confirms the reserve's debit, it does
	// not repeat it.
	if _, _, err := h.verify.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: h.owner.Type, OwnerID: h.owner.ID, Direction: cp.BillingDirectionMT,
		CustomerID: h.owner.CustomerID, MessageID: &msg, EntryType: cp.EntryCapture, Credits: 0,
	}); err != nil {
		t.Fatalf("record the winning capture durably: %v", err)
	}
	if h.balance(t) != 94 {
		t.Fatalf("durable balance after reserve+capture = %d, want 94", h.balance(t))
	}
	// Now a release arrives on the still-live hold: release.lua refunds the live cache to 100, but the
	// durable release must yield to the capture (no refund entry, balance stays 94).
	if err := h.acc.Release(ctx, h.owner, msg); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if h.ledgerCount(t, cp.EntryRelease, msg) != 0 {
		t.Error("release must have yielded to capture — no release entry")
	}
	if h.balance(t) != 94 {
		t.Errorf("durable balance = %d, want 94 (capture charge stands, release yielded)", h.balance(t))
	}
	// The cache must be reconciled: either dropped (rehydrates to 94) or already equal to durable — never
	// the phantom-credited 100 that release.lua wrote before the yield.
	if cached, present := h.cachedBalance(t); present && cached != 94 {
		t.Errorf("cache = %d after yield, want absent or 94 (durable) — never the phantom refund", cached)
	}
}

// TestReserveReplayAfterHoldExpiryIdempotent is the BLOQUANT-2 guard: a reserve replayed after its Redis
// hold has lapsed (simulated by deleting the hold) must NOT debit twice — the durable idempotency claim
// catches the replay whatever partition it lands in, and the speculative cache re-debit is undone. Without
// the partition-free idempotency table, this double-debits across a day boundary.
func TestReserveReplayAfterHoldExpiryIdempotent(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	if _, err := h.acc.Reserve(ctx, h.owner, msg, 8); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if h.balance(t) != 92 {
		t.Fatalf("durable balance after reserve = %d, want 92", h.balance(t))
	}
	// Simulate the short-TTL hold lapsing, so the replay takes the "reserved" branch (not "held").
	if err := h.rdb.Del(ctx, "billing:reservation:"+msg.String()).Err(); err != nil {
		t.Fatalf("delete hold: %v", err)
	}

	// Replay the same reserve. It must be idempotent: the durable balance stays debited exactly once.
	bal, err := h.acc.Reserve(ctx, h.owner, msg, 8)
	if err != nil {
		t.Fatalf("Reserve replay: %v", err)
	}
	if bal != 92 {
		t.Errorf("replay returned balance %d, want 92 (debited once)", bal)
	}
	if h.balance(t) != 92 {
		t.Errorf("durable balance after replay = %d, want 92 (NO double debit)", h.balance(t))
	}
	// The speculative cache re-debit must have been undone (cache back to 92, not 84).
	if cached, present := h.cachedBalance(t); present && cached != 92 {
		t.Errorf("cache = %d after replay, want 92 — the replay's speculative debit must be undone", cached)
	}
	// Exactly one reserve ledger row for the message.
	var reserves int
	pool := pgtest.Pool(t)
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM control_plane.billing_ledger WHERE message_id = $1 AND entry_type = 'reserve'`,
		msg).Scan(&reserves); err != nil {
		t.Fatalf("count reserves: %v", err)
	}
	if reserves != 1 {
		t.Errorf("reserve ledger rows = %d, want 1 (replay wrote none)", reserves)
	}
}

// expiringStore is a LedgerStore that lets the reserve through, then makes the next durable call behave
// like a caller whose deadline expired mid-write: it cancels the request context and fails. It models the
// production shape exactly — settle.Settler gives a release 200ms while billing's critical section runs to
// 4s (terminalCriticalTimeout), so the request context dying under an in-flight durable write is the
// ORDINARY failure, not the exotic one.
type expiringStore struct {
	balance int
	cancel  context.CancelFunc
	armed   bool
}

func (e *expiringStore) Balance(context.Context, string, uuid.UUID, string) (int, bool, error) {
	return e.balance, true, nil
}

func (e *expiringStore) RecordDurable(_ context.Context, entry cp.LedgerEntry) (int, bool, error) {
	if e.armed {
		e.cancel()
		return 0, false, context.Canceled
	}
	e.balance += entry.Credits
	return e.balance, true, nil
}

func (e *expiringStore) LedgerEntryExists(context.Context, uuid.UUID, cp.EntryType) (bool, error) {
	if e.armed {
		e.cancel()
		return false, context.Canceled
	}
	return false, nil
}

func (e *expiringStore) ReserveEntry(context.Context, uuid.UUID) (int, int, bool, error) {
	return 0, 0, false, nil
}

// TestReleaseRepairsTheCacheEvenWhenTheRequestExpires is the sibling of
// TestReleaseYieldsToCaptureReconcilesCache, for the failure that actually happens in production.
//
// step-260b fixed Release to drop the cache when the durable release does not land, and proved it against
// a severed Postgres. But a severed Postgres is the RARE trigger. The common one is the caller's deadline:
// settle.Settler allows 200ms, billing's terminal critical section runs to 4s, and its own comment warns
// the deadline "must stay above its commit latency". When it does not, release.lua has already refunded
// the live cache — and a repair that reuses the dead context cannot undo it, so the phantom credit the fix
// exists to prevent comes straight back.
//
// The repair must therefore outlive the request, exactly as undoReserveCacheDebit already does one
// function away. Nothing pinned that, and a chaos test could not: cutting the link never cancels a context.
func TestReleaseRepairsTheCacheEvenWhenTheRequestExpires(t *testing.T) {
	rdb := redistest.Client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const initial = 100
	const credits = 6
	store := &expiringStore{balance: initial, cancel: cancel}
	acc := billing.New(rdb, store)
	owner := billing.Owner{Type: cp.OwnerTypeCustomer, ID: uuid.New(), CustomerID: uuid.New()}
	msg := uuid.New()

	// Control: with the request healthy the reserve debits the cache and leaves a live hold.
	if _, err := acc.Reserve(ctx, owner, msg, credits); err != nil {
		t.Fatalf("the control reserve must succeed: %v", err)
	}
	bkey := "billing:balance:" + cp.BillingDirectionMT + ":" + owner.Type + ":" + owner.ID.String()
	if got, err := rdb.Get(ctx, bkey).Int(); err != nil || got != initial-credits {
		t.Fatalf("cached balance after the control reserve = %d (err=%v), want %d", got, err, initial-credits)
	}

	// The release now runs against a request that dies under its durable write: release.lua refunds the
	// live cache to 100, then the terminal resolution fails with the context gone.
	store.armed = true
	if err := acc.Release(ctx, owner, msg); err == nil {
		t.Fatal("a release whose durable side failed must report it")
	}

	// The cache must not be left claiming a refund the ledger never applied. Absent is the expected
	// outcome; equal-to-durable is accepted, because what must hold is that the cache never claims more.
	got, err := rdb.Get(context.Background(), bkey).Int()
	if err == nil && got > initial-credits {
		t.Errorf("balance cache = %d, want absent or at most %d: the durable release never landed, so the "+
			"cache is holding %d credits of phantom refund — the repair ran on the caller's dead context "+
			"and could not reach Redis", got, initial-credits, got-(initial-credits))
	}
}
