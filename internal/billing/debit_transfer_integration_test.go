package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestDebitTransferThatDidNotApplyFollowsTheDurableSide: the source was debited in Redis, then the write did
// not report an applied transfer. Whether or not something committed — a lost ack, a transfer already applied
// under the key — the next reserve must see exactly the durable truth, which a refund in place cannot give.
func TestDebitTransferThatDidNotApplyFollowsTheDurableSide(t *testing.T) {
	cases := []struct {
		name      string
		commits   bool
		applied   bool
		writeErr  error
		wantSpend bool
	}{
		{name: "failed before committing", writeErr: errors.New("postgres unreachable"), wantSpend: true},
		{name: "replay that moved nothing", wantSpend: true},
		{name: "committed, ack lost", commits: true, writeErr: errors.New("connection reset")},
		{name: "already applied under the key", commits: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBillingHarness(t, 10)
			ctx := context.Background()
			sink := seedAccount(t, h.owner.CustomerID)
			key := uuid.New()
			err := h.acc.DebitTransfer(ctx, h.owner, key, 10, func(ctx context.Context) (bool, error) {
				if tc.commits {
					debit, credit := transferLegs(h, sink, key, 10)
					if _, _, err := h.repo.Transfer(ctx, debit, credit, key); err != nil {
						t.Fatalf("durable transfer: %v", err)
					}
				}
				return tc.applied, tc.writeErr
			})
			if !errors.Is(err, tc.writeErr) {
				t.Fatalf("DebitTransfer = %v, want the write's own outcome %v", err, tc.writeErr)
			}
			_, err = h.acc.Reserve(ctx, h.owner, uuid.New(), 10)
			if tc.wantSpend && err != nil {
				t.Fatalf("Reserve of the 10 credits still durable = %v, want success", err)
			}
			if !tc.wantSpend && !errors.Is(err, errs.ErrInsufficientCredit) {
				t.Fatalf("Reserve of the 10 credits transferred durably = %v, want ErrInsufficientCredit", err)
			}
		})
	}
}

// TestDebitTransferForgetsItsDebitOnceApplied: an applied transfer is in the durable balance; left in the
// in-flight set it would be subtracted a second time by every rehydration. The write here moves nothing
// durably, so a rehydration that still subtracted it would show 6 instead of 10.
func TestDebitTransferForgetsItsDebitOnceApplied(t *testing.T) {
	h := newBillingHarness(t, 10)
	ctx := context.Background()
	if err := h.acc.DebitTransfer(ctx, h.owner, uuid.New(), 4, func(context.Context) (bool, error) {
		return true, nil
	}); err != nil {
		t.Fatalf("DebitTransfer: %v", err)
	}
	if got, ok := h.cachedBalance(t); !ok || got != 6 {
		t.Fatalf("cached balance = %d (present %v), want 6", got, ok)
	}
	h.dropCachedBalance(t)
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 10); err != nil {
		t.Fatalf("Reserve of the durable 10 after the cache dropped = %v, want success", err)
	}
}

// TestDebitTransferRefusesTheSameKeyInFlight: a second attempt under the key of a transfer still writing is
// refused without touching anything — cleaning up would remove the first attempt's debit before its commit.
func TestDebitTransferRefusesTheSameKeyInFlight(t *testing.T) {
	h := newBillingHarness(t, 10)
	ctx := context.Background()
	key := uuid.New()
	var second error
	if err := h.acc.DebitTransfer(ctx, h.owner, key, 10, func(context.Context) (bool, error) {
		second = h.acc.DebitTransfer(ctx, h.owner, key, 10, func(context.Context) (bool, error) {
			t.Error("the second attempt reached the durable write")
			return true, nil
		})
		if err := h.rdb.Del(ctx, billing.BalanceCacheKey(cp.BillingDirectionMT, h.owner.Type, h.owner.ID)).Err(); err != nil {
			t.Errorf("drop cache: %v", err)
		}
		if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); !errors.Is(err, errs.ErrInsufficientCredit) {
			t.Errorf("Reserve while the first transfer is still writing = %v, want ErrInsufficientCredit", err)
		}
		return false, errors.New("abandoned")
	}); err == nil {
		t.Fatal("first DebitTransfer = nil, want its write's error")
	}
	if !errors.Is(second, errs.ErrIdempotencyConflict) {
		t.Fatalf("second DebitTransfer under the same key = %v, want ErrIdempotencyConflict", second)
	}
}

// transferLegs builds the ledger pair Transfer expects: the harness owner pays, sink receives.
func transferLegs(h *billingHarness, sink, key uuid.UUID, credits int) (debit, credit cp.LedgerEntry) {
	debit = cp.LedgerEntry{OwnerType: h.owner.Type, OwnerID: h.owner.ID, Direction: cp.BillingDirectionMT,
		CustomerID: h.owner.CustomerID, MessageID: &key, EntryType: cp.EntryTransfer, Credits: -credits}
	credit = cp.LedgerEntry{OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: sink, Direction: cp.BillingDirectionMT,
		CustomerID: h.owner.CustomerID, AccountID: &sink, MessageID: &key, EntryType: cp.EntryTransfer, Credits: credits}
	return debit, credit
}

// TestDebitTransferBoundsItsWrite: an admin request has no deadline of its own, and a transfer committing
// after its in-flight field ages out would no longer be subtracted by a rehydration.
func TestDebitTransferBoundsItsWrite(t *testing.T) {
	h := newBillingHarness(t, 10)
	done := make(chan error, 1)
	go func() {
		done <- h.acc.DebitTransfer(context.Background(), h.owner, uuid.New(), 1, func(ctx context.Context) (bool, error) {
			<-ctx.Done()
			return false, ctx.Err()
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("DebitTransfer over a write that never commits succeeded, want an error")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("DebitTransfer still waiting on its write after 15s: the write is unbounded")
	}
}
