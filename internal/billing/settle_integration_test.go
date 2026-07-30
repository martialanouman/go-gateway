package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/connectorpool/settle"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// countLedger counts billing_ledger rows of an entry_type for a message — the authoritative idempotency
// check (a double capture/release must add exactly one entry of its type).
func countLedger(t *testing.T, messageID uuid.UUID, entryType cp.EntryType) int {
	t.Helper()
	var n int
	if err := pgtest.Pool(t).QueryRow(context.Background(),
		`SELECT count(*) FROM control_plane.billing_ledger WHERE message_id = $1 AND entry_type = $2`,
		messageID, string(entryType)).Scan(&n); err != nil {
		t.Fatalf("count ledger %s: %v", entryType, err)
	}
	return n
}

// settledRouted builds the mt.routed the settler reads: a customer-scoped billable message keyed to the
// harness owner and the seeded SMPP account (the ledger's account_id FK).
func settledRouted(h *billingHarness, accountID, messageID uuid.UUID) pipeline.RoutedMT {
	return pipeline.RoutedMT{
		MessageID:  messageID,
		CustomerID: h.owner.CustomerID,
		AccountID:  accountID,
		Billable:   true,
		OwnerType:  cp.OwnerTypeCustomer,
	}
}

// reserveFor establishes a reservation for messageID against the harness owner, attributed to accountID (so
// the reserve and settle ledger rows share the same account_id).
func reserveFor(t *testing.T, h *billingHarness, accountID, messageID uuid.UUID, credits int) {
	t.Helper()
	owner := billing.Owner{Type: cp.OwnerTypeCustomer, ID: h.owner.CustomerID, CustomerID: h.owner.CustomerID, AccountID: &accountID}
	if _, err := h.acc.Reserve(context.Background(), owner, messageID, credits); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
}

// TestSettlerCaptureIdempotentUnderDoubleDelivery is invariant (c) for capture: a redelivered sent message
// captures against the same message_id and adds EXACTLY ONE capture ledger entry, with a stable
// credits_charged. The balance is deliberately NOT the assertion here — capture entries are credits=0, so a
// buggy double-capture would move the balance zero times either way; only the ledger entry count catches it.
func TestSettlerCaptureIdempotentUnderDoubleDelivery(t *testing.T) {
	h := newBillingHarness(t, 100)
	// A generous deadline: these settle over bufconn + a testcontainer Postgres durable write, so the tight
	// production default could fail-open on a slow CI commit and turn an idempotency assertion flaky.
	settler := settle.NewSettler(newBillingGRPCClient(t, h), settle.WithTimeout(5*time.Second))
	ctx := context.Background()
	account := seedAccount(t, h.owner.CustomerID)
	msg := uuid.New()
	reserveFor(t, h, account, msg, 3)
	r := settledRouted(h, account, msg)

	billed, charged := settler.Capture(ctx, r)
	if !billed || charged == nil || *charged != 3 {
		t.Fatalf("first Capture = (%v, %v), want (true, &3)", billed, charged)
	}
	// Redelivery of the same message_id: still reports charged=3, but adds no second entry.
	billed2, charged2 := settler.Capture(ctx, r)
	if !billed2 || charged2 == nil || *charged2 != 3 {
		t.Fatalf("redelivered Capture = (%v, %v), want (true, &3) — stable", billed2, charged2)
	}
	if n := countLedger(t, msg, cp.EntryCapture); n != 1 {
		t.Errorf("capture ledger entries = %d, want exactly 1 (idempotent under double delivery)", n)
	}
}

// TestSettlerReleaseIdempotentUnderDoubleDelivery is invariant (c) for release, where the balance assertion
// has teeth: release REFUNDS (credits>0), so a double release would refund twice — minting free credit. A
// redelivered terminal failure must refund exactly once: one release entry, and the balance back to its
// pre-reserve value (one debit + one refund), never above it.
func TestSettlerReleaseIdempotentUnderDoubleDelivery(t *testing.T) {
	h := newBillingHarness(t, 100)
	settler := settle.NewSettler(newBillingGRPCClient(t, h), settle.WithTimeout(5*time.Second))
	ctx := context.Background()
	account := seedAccount(t, h.owner.CustomerID)
	msg := uuid.New()
	reserveFor(t, h, account, msg, 3) // balance 100 -> 97
	if got := h.balance(t); got != 97 {
		t.Fatalf("balance after reserve = %d, want 97", got)
	}
	r := settledRouted(h, account, msg)

	settler.Release(ctx, r)
	settler.Release(ctx, r) // redelivery — must not refund twice

	if got := h.balance(t); got != 100 {
		t.Errorf("balance after double release = %d, want 100 (refund exactly once, never 103)", got)
	}
	if n := countLedger(t, msg, cp.EntryRelease); n != 1 {
		t.Errorf("release ledger entries = %d, want exactly 1 (a double release mints free credit)", n)
	}
}
