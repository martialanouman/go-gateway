package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestReserveFailsClosedWhenRedisIsCut is the step-250 acceptance test for the third row of the
// failure-policy matrix (guide de codage §16): "Redis (cache de solde) -> fail-closed pendant la
// réhydratation depuis Postgres".
//
// TestReserveFailsClosedWhenAuthorityDown covers the mirror image — Postgres down, Redis up. Nothing
// covered this direction, and it is the one the matrix row is actually about: the cache is the thing
// that disappears, and the question is whether a credit can slip through unverified while it is gone.
//
// The subtle assertion is the last one, and it is what makes this a ZERO-LOSS test rather than merely
// a fail-closed one. Refusing is not enough: HOW the reserve refuses decides the message's fate. The
// error must carry NO platform code, because router.handle branches on exactly that — a coded error
// becomes a `rejected` CDR row and COMMITS the offset (the message is deliberately turned away), while
// an uncoded one is treated as a transient fault, leaves the offset uncommitted and is redelivered.
// Give this failure a code and every message in flight during a Redis blip is permanently rejected
// instead of retried. That is the loss the criterion forbids, and it would be invisible to any
// assertion that only checked "err != nil".
func TestReserveFailsClosedWhenRedisIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	const initial = 100
	h := newBillingHarnessOn(t, rdb, initial, time.Minute)
	ctx := context.Background()

	// Control, with Redis up: a reserve succeeds and debits the durable ledger. Without it the outage
	// half could not distinguish fail-closed from a harness that never worked.
	first := uuid.New()
	if _, err := h.acc.Reserve(ctx, h.owner, first, 3); err != nil {
		t.Fatalf("with redis up the reserve must succeed: %v", err)
	}
	if got := h.balance(t); got != initial-3 {
		t.Fatalf("durable balance after the control reserve = %d, want %d", got, initial-3)
	}

	proxy.Cut()

	// The outage: a reserve must REFUSE, and refuse loudly. A silent (0, nil) would be the worst
	// outcome of all — the router would read "not billable" and send an unpaid message.
	during := uuid.New()
	credits, err := h.acc.Reserve(ctx, h.owner, during, 7)
	if err == nil {
		t.Fatalf("with redis cut the reserve returned (%d, nil): a credit must never be granted "+
			"unverified while the balance cache is gone", credits)
	}
	if code, coded := errs.CodeOf(err); coded {
		t.Errorf("the outage error carries the platform code %q: router.handle turns a coded error into "+
			"a rejected CDR and COMMITS the offset, so every message in flight during a redis blip "+
			"would be permanently rejected instead of retried — the error must stay uncoded", code)
	}

	// Nothing was written durably: no hold, no ledger entry. A fail-closed reserve that still debited
	// would leak credit on every outage.
	if got := h.balance(t); got != initial-3 {
		t.Errorf("durable balance = %d, want %d: a refused reserve must not touch the ledger", got, initial-3)
	}
	if exists, err := h.repo.LedgerEntryExists(ctx, during, cp.EntryReserve); err != nil {
		t.Fatalf("LedgerEntryExists: %v", err)
	} else if exists {
		t.Error("a reserve refused during the outage still wrote a ledger entry")
	}

	proxy.Resume()

	// Recovery: the redelivery the uncoded error bought now goes through.
	if _, err := h.acc.Reserve(ctx, h.owner, during, 7); err != nil {
		t.Fatalf("after redis came back the redelivered reserve must succeed: %v", err)
	}
	if got := h.balance(t); got != initial-3-7 {
		t.Fatalf("durable balance = %d, want %d after the redelivery", got, initial-3-7)
	}

	// And once more, because ONE redelivery proves nothing about idempotence: the outage reserve wrote
	// nothing, so the call above was this message's first successful one and could not have doubled
	// anything. Kafka's guarantee is at-least-once, not exactly-twice — a commit lost a second time
	// redelivers a second time. This is the call that can double-charge, so this is the one that has to
	// be asserted (invariant c).
	if _, err := h.acc.Reserve(ctx, h.owner, during, 7); err != nil {
		t.Fatalf("a second redelivery of the same message_id must be idempotent, not an error: %v", err)
	}
	if got := h.balance(t); got != initial-3-7 {
		t.Errorf("durable balance = %d, want %d: redelivering %s charged it twice — billing must be "+
			"idempotent by message_id across the Kafka hop (invariant c)", got, initial-3-7, during)
	}
}
