package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestDebitTransferThatDidNotApplyGivesTheCreditBack: the source was debited in Redis, then the transfer
// failed or turned out to be a replay. Whatever committed, the next reserve must see the durable truth.
func TestDebitTransferThatDidNotApplyGivesTheCreditBack(t *testing.T) {
	for name, write := range map[string]func(context.Context) (bool, error){
		"failed":      func(context.Context) (bool, error) { return false, errors.New("postgres unreachable") },
		"not applied": func(context.Context) (bool, error) { return false, nil },
	} {
		t.Run(name, func(t *testing.T) {
			h := newBillingHarness(t, 10)
			ctx := context.Background()
			err := h.acc.DebitTransfer(ctx, h.owner, uuid.New(), 10, write)
			if name == "failed" && err == nil {
				t.Fatal("DebitTransfer over a failed write = nil, want its error")
			}
			if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 10); err != nil {
				t.Fatalf("Reserve of the 10 credits never transferred = %v, want success", err)
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
