package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/connectorpool/settle"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
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

// captureRequest is the gRPC capture the pool used to send for a customer-scoped reservation.
func captureRequest(h *billingHarness, accountID, messageID uuid.UUID) *pb.CaptureRequest {
	return &pb.CaptureRequest{MessageId: messageID.String(), Owner: &pb.Owner{
		OwnerType: pb.OwnerType_OWNER_TYPE_CUSTOMER, OwnerId: h.owner.CustomerID.String(),
		CustomerId: h.owner.CustomerID.String(), AccountId: accountID.String(),
	}}
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
	client := newBillingGRPCClient(t, h)
	ctx := context.Background()
	account := seedAccount(t, h.owner.CustomerID)
	msg := uuid.New()
	reserveFor(t, h, account, msg, 3)
	req := captureRequest(h, account, msg)

	first, err := client.Capture(ctx, req)
	if err != nil || first.GetCreditsCharged() != 3 {
		t.Fatalf("first Capture = (%v, %v), want 3 credits charged", first, err)
	}
	// Redelivery of the same message_id: still reports 3 charged, but adds no second entry.
	second, err := client.Capture(ctx, req)
	if err != nil || second.GetCreditsCharged() != 3 {
		t.Fatalf("redelivered Capture = (%v, %v), want 3 credits charged — stable", second, err)
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

// TestPoolAndConsumerSettleOnce: a capture over gRPC (the pool before step-287d, or the reaper) and the
// mt.outcome consumer settle the same message. The ledger must keep exactly one capture, and the consumer
// must settle the balance the reservation was made on.
func TestPoolAndConsumerSettleOnce(t *testing.T) {
	h := newBillingHarness(t, 100)
	account := seedAccount(t, h.owner.CustomerID)
	msg := uuid.New()
	reserveFor(t, h, account, msg, 3)

	if resp, err := newBillingGRPCClient(t, h).Capture(context.Background(), captureRequest(h, account, msg)); err != nil ||
		resp.GetCreditsCharged() != 3 {
		t.Fatalf("the gRPC capture = (%v, %v), want 3 credits charged", resp, err)
	}
	rec, err := pipeline.EncodeOutcome(pipeline.OutcomeMT{
		MessageID: msg, CustomerID: h.owner.CustomerID, AccountID: account, Status: "enroute",
		Billable: true, OwnerType: cp.OwnerTypeCustomer,
	})
	if err != nil {
		t.Fatalf("encode outcome: %v", err)
	}
	cons := &batchOnce{recs: []kafka.Record{rec}}
	if err := billing.NewSettleConsumer(cons, h.acc, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cons.results[0] != nil {
		t.Fatalf("the consumer's capture failed: %v", cons.results[0])
	}
	if n := countLedger(t, msg, cp.EntryCapture); n != 1 {
		t.Errorf("capture ledger entries = %d, want exactly 1 (pool and consumer settle the same message)", n)
	}
	if got := h.balance(t); got != 97 {
		t.Errorf("balance = %d, want 97 (one debit, captured once)", got)
	}
}
