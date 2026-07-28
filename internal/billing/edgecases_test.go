package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// TestCaptureAfterReleaseYields is the capture↔release conflict (§6.9): once a message has been released
// (failed before send), a late capture (e.g. an out-of-order DLR) must yield — the release wins, the
// capture is a no-op, and the balance stays refunded.
func TestCaptureAfterReleaseYields(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	if _, err := h.acc.Reserve(ctx, h.owner, msg, 3); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := h.acc.Release(ctx, h.owner, msg); err != nil {
		t.Fatalf("Release: %v", err)
	}
	charged, err := h.acc.Capture(ctx, h.owner, msg) // the hold is gone; capture must yield to the release
	if err != nil {
		t.Fatalf("Capture after release: %v", err)
	}
	if charged != 0 {
		t.Errorf("credits charged after release = %d, want 0 (release wins)", charged)
	}
	if h.balance(t) != 100 {
		t.Errorf("balance = %d, want 100 (stayed refunded)", h.balance(t))
	}
	if h.ledgerCount(t, cp.EntryCapture, msg) != 0 {
		t.Error("no capture entry must be written when the release already won")
	}
}

// TestReleaseIdempotent: a repeated release for the same message is a no-op — the balance is refunded once.
func TestReleaseIdempotent(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	if _, err := h.acc.Reserve(ctx, h.owner, msg, 7); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := h.acc.Release(ctx, h.owner, msg); err != nil {
		t.Fatalf("Release 1: %v", err)
	}
	if err := h.acc.Release(ctx, h.owner, msg); err != nil {
		t.Fatalf("Release 2 (replay): %v", err)
	}
	if h.balance(t) != 100 {
		t.Errorf("balance after double release = %d, want 100 (refunded exactly once)", h.balance(t))
	}
}

// TestCaptureAfterHoldExpiry: when the SMSC round-trip outlives the reservation hold, the capture finds
// no live hold and recovers the reserved amount from the durable ledger — the charge still lands, exactly
// once, and the balance stays debited (the reserve already debited it).
func TestCaptureAfterHoldExpiry(t *testing.T) {
	h := newBillingHarnessTTL(t, 100, 100*time.Millisecond)
	ctx := context.Background()
	msg := uuid.New()

	if _, err := h.acc.Reserve(ctx, h.owner, msg, 6); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	time.Sleep(250 * time.Millisecond) // let the hold's TTL lapse

	charged, err := h.acc.Capture(ctx, h.owner, msg)
	if err != nil {
		t.Fatalf("Capture after hold expiry: %v", err)
	}
	if charged != 6 {
		t.Errorf("credits charged = %d, want 6 (recovered from the ledger)", charged)
	}
	if h.balance(t) != 94 {
		t.Errorf("balance = %d, want 94 (reserve debit stands, capture recovered)", h.balance(t))
	}
	if h.ledgerCount(t, cp.EntryCapture, msg) != 1 {
		t.Error("exactly one capture entry must be written after a lapsed hold")
	}
}
