package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// staleConnBudget bounds drainStaleConns. MaxConns (4, via pgtest.Config) is the hard ceiling on how many
// connections a pool can be holding at all, so twice that is generous slack. It is a BOUND, not a
// mechanism: in every observed run one round-trip has sufficed, and how pgx reaps corpses was not
// measured. The loop exists so the helper cannot hang — no test should depend on the count.
const staleConnBudget = 8

// drainStaleConns sheds the connections a pool opened BEFORE the link was cut. Measured: without it, the
// first query after Resume fails with "unexpected EOF" on a connection that died during the outage; with
// it, it does not. That is infrastructure recovery, not the behaviour any of these tests is about —
// production wears it as one failed reaper pass, not as a lost refund — so it is drained here,
// deliberately, rather than folded into an assertion that would then be measuring pgxpool.
func drainStaleConns(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for range staleConnBudget {
		if err := pool.Ping(context.Background()); err == nil {
			return
		}
	}
	t.Fatalf("the pool never recovered within %d round-trips after the link was restored", staleConnBudget)
}

// TestReleaseLeavesNoPhantomCreditWhenPostgresIsCut is the step-260b acceptance test for the durable
// authority disappearing mid-release.
//
// release.lua does two things atomically before Postgres is ever consulted: it adds the held credits
// back to the LIVE balance cache and it drops the hold. If the durable release then fails, the cache
// carries a refund the ledger never applied and the hold is gone, so the divergence cannot be undone
// in place. It is bounded only by the balance cache's TTL (10 min) — the reaper cannot arrive first,
// its MIN_AGE being 15 min — and for that whole window a warm-cache Reserve spends credit the durable
// authority does not cover. Losing money quietly is worse than refusing loudly.
//
// The assertion is deliberately "absent OR equal to durable", not "absent": what must hold is that
// the cache never claims more than the ledger, whichever way the code chooses to get there.
func TestReleaseLeavesNoPhantomCreditWhenPostgresIsCut(t *testing.T) {
	pool, proxy := pgtest.Cuttable(t)
	const initial = 100
	const credits = 7
	h := newBillingHarnessOn(t, redistest.Client(t), pool, initial, time.Minute)
	ctx := context.Background()

	// Control, with Postgres up: the reserve debits BOTH sides and leaves a live hold. Without it the
	// outage half could not distinguish a repaired cache from a harness that never worked.
	msg := uuid.New()
	if _, err := h.acc.Reserve(ctx, h.owner, msg, credits); err != nil {
		t.Fatalf("with postgres up the reserve must succeed: %v", err)
	}
	if got := h.balance(t); got != initial-credits {
		t.Fatalf("durable balance after the control reserve = %d, want %d", got, initial-credits)
	}
	if got, present := h.cachedBalance(t); !present || got != initial-credits {
		t.Fatalf("cached balance after the control reserve = (%d, present=%v), want (%d, true)",
			got, present, initial-credits)
	}

	proxy.Cut()

	// The outage: release.lua refunds the live cache and drops the hold, then the durable release
	// cannot land. The failure must be reported — a silent nil would tell the settler the money was
	// resolved when it was not.
	if err := h.acc.Release(ctx, h.owner, msg); err == nil {
		t.Fatal("with postgres cut the release returned nil: a durable failure must be reported, " +
			"otherwise the caller records a settlement that never happened")
	}

	// The heart of it. Read the durable side through the SECOND, uncut pool — reading it through the
	// cut one would make the verification die with the dependency and pass by observing nothing.
	durable := h.balance(t)
	if durable != initial-credits {
		t.Errorf("durable balance = %d, want %d: a failed release must not have moved the ledger",
			durable, initial-credits)
	}
	if cached, present := h.cachedBalance(t); present && cached != durable {
		t.Errorf("balance cache = %d but the durable authority holds %d: release.lua refunded the "+
			"cache and the durable release never landed, so for up to the cache TTL a warm-cache "+
			"reserve spends %d credits the ledger does not cover", cached, durable, cached-durable)
	}

	proxy.Resume()
	drainStaleConns(t, pool)

	// Recovery: the release the outage lost is re-driven — in production by billing.Reaper, since
	// settle.Settler fails open and never propagates. It must now land durably.
	if err := h.acc.Release(ctx, h.owner, msg); err != nil {
		t.Fatalf("after postgres came back the replayed release must succeed: %v", err)
	}
	if got := h.balance(t); got != initial {
		t.Fatalf("durable balance = %d, want %d after the replayed release", got, initial)
	}

	// And once more: the reaper re-drives on every pass until the reservation stops looking orphaned,
	// so a release that refunded twice would hand out free credit on a schedule (invariant c).
	if err := h.acc.Release(ctx, h.owner, msg); err != nil {
		t.Fatalf("a second replay of the same release must be idempotent, not an error: %v", err)
	}
	if got := h.balance(t); got != initial {
		t.Errorf("durable balance = %d, want %d: replaying the release refunded %s twice",
			got, initial, msg)
	}
}

// TestReserveFailsClosedWhenPostgresIsCut is the step-260b acceptance test for the durable authority
// disappearing under a reserve — the mirror of TestReserveFailsClosedWhenRedisIsCut, and the replacement
// for failclosed_test.go's fake: a fake LedgerStore returns a bare errors.New, while a real pgx failure
// travels through postgres.translate and comes back carrying a platform code. Same shape, different
// contract, and only one of the two is what production does.
//
// Reserve touches Postgres on THREE paths, not the two the fiche named, and each fails differently:
// "cold" (rehydrate), "reserved" (the durable mirror of a debit already taken from the cache) and "held"
// (a redelivery landing on a live hold). The middle one is the dangerous one, because reserve.lua has
// ALREADY debited the cache by the time Postgres is consulted.
func TestReserveFailsClosedWhenPostgresIsCut(t *testing.T) {
	pool, proxy := pgtest.Cuttable(t)
	const initial = 100
	h := newBillingHarnessOn(t, redistest.Client(t), pool, initial, time.Minute)
	ctx := context.Background()

	// Control, with Postgres up: the cache starts cold, so this reserve exercises rehydration and then
	// debits both sides. Without it the outage half could not tell fail-closed from a harness that never
	// reached Postgres in the first place.
	first := uuid.New()
	if _, err := h.acc.Reserve(ctx, h.owner, first, 3); err != nil {
		t.Fatalf("with postgres up the reserve must succeed: %v", err)
	}
	if got := h.balance(t); got != initial-3 {
		t.Fatalf("durable balance after the control reserve = %d, want %d", got, initial-3)
	}

	proxy.Cut()

	// Branch "held": a redelivery whose Redis hold is still live. reserve.lua answers from the hold alone
	// and mutates nothing, but the self-repair path still reads the ledger to confirm the durable reserve
	// exists — and that read is now gone. Refusing is right: answering from the hold alone would report a
	// balance nobody verified.
	if _, err := h.acc.Reserve(ctx, h.owner, first, 3); err == nil {
		t.Error("a redelivery during the outage was granted from the Redis hold alone, without the ledger " +
			"confirming the durable reserve behind it")
	}

	// Branch "reserved": a fresh message on the WARM cache. reserve.lua debits the cache and places a hold
	// before Postgres is ever reached, so a refusal here is only half the job — the speculative debit has
	// to be given back, or every refused message during an outage silently shrinks the cached balance.
	during := uuid.New()
	credits, err := h.acc.Reserve(ctx, h.owner, during, 7)
	if err == nil {
		t.Fatalf("with postgres cut the reserve returned (%d, nil): a credit must never be granted while "+
			"the durable authority is unreachable", credits)
	}

	// The assertion that decides the message's fate. billing-svc turns exactly ONE outcome into a terminal
	// rejection — grpcserver.go answers reserved=false for errs.ErrInsufficientCredit and lets the client
	// map it to a rejected CDR; every other error crosses as a gRPC fault, which credit.Reserver returns
	// raw and the router therefore retries. So an outage must never look like a denial of funds: that
	// single confusion would permanently reject every message in flight instead of replaying it.
	if errors.Is(err, errs.ErrInsufficientCredit) {
		t.Errorf("the outage error reads as insufficient credit (%v): billing-svc answers reserved=false "+
			"for exactly that error, which the router turns into a rejected CDR with the offset COMMITTED "+
			"— every message in flight during a postgres blip would be lost instead of retried", err)
	}
	// A tripwire, not a rule. Unlike the Redis path, this error IS coded: postgres.translate wraps an
	// unrecognised pgx failure in errs.ErrInternal (pgerr.go:49), so the symmetric assertion from the Redis
	// chaos test ("must stay uncoded") is false here — and that is safe, because the code does not survive
	// the wire: grpcerr.Status turns it into a gRPC status, and errs.CodeOf of a *status.Error is false.
	// The router never calls the Accountant in-process (cmd/router-svc/wiring.go dials billing over gRPC).
	// If this line ever fails, the reasoning above is what needs re-reading — not this expectation.
	if code, coded := errs.CodeOf(err); !coded || code != errs.ErrInternal {
		t.Errorf("outage error code = (%q, coded=%v), want (%q, true); the comment above explains why this "+
			"is harmless, and a change here means that explanation needs revisiting", code, coded, errs.ErrInternal)
	}

	// Nothing durable moved, and — the part nothing tested before — the cache was put back.
	if got := h.balance(t); got != initial-3 {
		t.Errorf("durable balance = %d, want %d: a refused reserve must not touch the ledger", got, initial-3)
	}
	if exists, err := h.verify.LedgerEntryExists(ctx, during, cp.EntryReserve); err != nil {
		t.Fatalf("LedgerEntryExists: %v", err)
	} else if exists {
		t.Error("a reserve refused during the outage still wrote a ledger entry")
	}
	if cached, present := h.cachedBalance(t); present && cached != initial-3 {
		t.Errorf("balance cache = %d, want %d: reserve.lua debited the cache before Postgres was consulted "+
			"and the compensation did not give it back, so every refusal during an outage shrinks the "+
			"cached balance a little further while the ledger stays put", cached, initial-3)
	}
	if n, err := h.rdb.Exists(ctx, "billing:reservation:"+during.String()).Result(); err != nil || n != 0 {
		t.Errorf("hold for the refused message exists=%d (err=%v), want absent", n, err)
	}

	// Branch "cold": the cache lapses the way its TTL makes it, and rehydration is the only way back.
	h.dropCachedBalance(t)
	cold := uuid.New()
	_, coldErr := h.acc.Reserve(ctx, h.owner, cold, 5)
	if coldErr == nil {
		t.Error("with the cache cold and postgres cut the reserve was granted: the balance behind it was " +
			"never read from anywhere")
	}
	// Refusing is not enough, and this branch is where it is easiest to refuse for the WRONG reason. A
	// rehydration that swallowed its error would leave the cache at zero and the next attempt would read
	// "insufficient credit" — an outage wearing the mask of a denial of funds, which billing-svc answers
	// with reserved=false and the router turns into a permanent rejection. The refusal must stay a fault.
	if errors.Is(coldErr, errs.ErrInsufficientCredit) {
		t.Errorf("the cold-cache outage error reads as insufficient credit (%v): a balance that could not "+
			"be read is not a balance of zero, and the difference is a message rejected forever instead of "+
			"retried", coldErr)
	}
	if _, present := h.cachedBalance(t); present {
		t.Error("a failed rehydration left a balance in the cache: it can only have been invented, since " +
			"the durable authority was unreachable")
	}

	proxy.Resume()
	drainStaleConns(t, pool)

	// Recovery: the redelivery that the refusal bought now goes through.
	if _, err := h.acc.Reserve(ctx, h.owner, during, 7); err != nil {
		t.Fatalf("after postgres came back the redelivered reserve must succeed: %v", err)
	}
	if got := h.balance(t); got != initial-3-7 {
		t.Fatalf("durable balance = %d, want %d after the redelivery", got, initial-3-7)
	}

	// And once more, because ONE redelivery proves nothing about idempotence: the outage reserve wrote
	// nothing, so the call above was this message's first successful one and could not have doubled
	// anything. Kafka's guarantee is at-least-once — a commit lost twice redelivers twice. This is the call
	// that can double-charge, so this is the one that has to be asserted (invariant c).
	if _, err := h.acc.Reserve(ctx, h.owner, during, 7); err != nil {
		t.Fatalf("a second redelivery of the same message_id must be idempotent, not an error: %v", err)
	}
	if got := h.balance(t); got != initial-3-7 {
		t.Errorf("durable balance = %d, want %d: redelivering %s charged it twice — billing must be "+
			"idempotent by message_id across the Kafka hop (invariant c)", got, initial-3-7, during)
	}
}

// TestCaptureRecoversTheHoldFromTheLedgerAfterAnOutage pins the safety of capture.lua's DEL-first order.
//
// The script drops the Redis hold as its FIRST mutation, before Postgres is consulted at all, and there is
// no undoCaptureCacheDelete to put it back — unlike reserve, which compensates. That looks like a leak, and
// it is deliberately not one: the hold is a cache, the ledger is the authority, and a replay recovers the
// amount from the durable reserve entry. Adding a compensation here would reopen the double-capture race
// the atomic DEL exists to close, so this test DOCUMENTS the behaviour rather than changing it — and it
// exists because "the ledger will cover it" is an assertion, not an argument, until something proves it.
func TestCaptureRecoversTheHoldFromTheLedgerAfterAnOutage(t *testing.T) {
	pool, proxy := pgtest.Cuttable(t)
	const initial = 100
	const credits = 9
	h := newBillingHarnessOn(t, redistest.Client(t), pool, initial, time.Minute)
	ctx := context.Background()

	// Control, with Postgres up: the reserve debits the ledger and leaves a live hold to capture.
	msg := uuid.New()
	if _, err := h.acc.Reserve(ctx, h.owner, msg, credits); err != nil {
		t.Fatalf("with postgres up the reserve must succeed: %v", err)
	}
	if n, err := h.rdb.Exists(ctx, "billing:reservation:"+msg.String()).Result(); err != nil || n != 1 {
		t.Fatalf("hold after the control reserve exists=%d (err=%v), want 1", n, err)
	}

	proxy.Cut()

	if charged, err := h.acc.Capture(ctx, h.owner, msg); err == nil {
		t.Fatalf("with postgres cut the capture returned (%d, nil): a capture is not committed until the "+
			"ledger says so", charged)
	}
	// The hold is gone even though the capture failed. This is the behaviour under test, not a bug — but it
	// is the reason the recovery below cannot lean on Redis for the amount.
	if n, err := h.rdb.Exists(ctx, "billing:reservation:"+msg.String()).Result(); err != nil || n != 0 {
		t.Errorf("hold exists=%d (err=%v) after a failed capture, want 0: capture.lua deletes it as its "+
			"first mutation, so a change here means the recovery path below is no longer the one that runs",
			n, err)
	}
	if h.ledgerCount(t, cp.EntryCapture, msg) != 0 {
		t.Error("a capture that failed on the durable side still wrote its ledger entry")
	}
	// capture.lua only READS the balance key, so an outage must leave the cached balance exactly as the
	// reserve left it — no phantom credit in either direction.
	if cached, present := h.cachedBalance(t); !present || cached != initial-credits {
		t.Errorf("cached balance = (%d, present=%v) after a failed capture, want (%d, true): a capture "+
			"never moves the balance, the reserve already did", cached, present, initial-credits)
	}

	proxy.Resume()
	drainStaleConns(t, pool)

	// The recovery, and the whole point: the hold no longer exists, so the amount charged can ONLY have
	// come from the durable reserve entry. A wrong amount here would put a false figure on the CDR.
	charged, err := h.acc.Capture(ctx, h.owner, msg)
	if err != nil {
		t.Fatalf("after postgres came back the replayed capture must succeed: %v", err)
	}
	if charged != credits {
		t.Errorf("credits charged = %d, want %d: with the hold gone the amount is recovered from the "+
			"ledger's reserve entry, and a wrong one is billed to the customer on the CDR", charged, credits)
	}
	if got := h.balance(t); got != initial-credits {
		t.Errorf("durable balance = %d, want %d: a capture confirms the reserve's debit, it does not "+
			"repeat it", got, initial-credits)
	}
	if h.ledgerCount(t, cp.EntryCapture, msg) != 1 {
		t.Error("the replayed capture did not record its ledger entry")
	}

	// Idempotent: connector-pool re-drives a capture on every redelivery, and the reaper on every sweep.
	charged, err = h.acc.Capture(ctx, h.owner, msg)
	if err != nil {
		t.Fatalf("a second replay of the same capture must be idempotent, not an error: %v", err)
	}
	if charged != credits {
		t.Errorf("credits charged on the second replay = %d, want %d", charged, credits)
	}
	if got := h.balance(t); got != initial-credits {
		t.Errorf("durable balance = %d, want %d: replaying the capture moved money twice", got, initial-credits)
	}
}
