package main

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestBalanceCacheInvalidationFencesRehydration: an admin money op lowers the durable balance outside
// reserve.lua, so the invalidation it wires must bump the debit counter, not just delete the key (ADR-0022).
func TestBalanceCacheInvalidationFencesRehydration(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	owner := uuid.New()
	key := billing.BalanceCacheKey(cp.BillingDirectionMT, cp.OwnerTypeCustomer, owner)
	seqKey := "billing:seq:" + cp.BillingDirectionMT + ":" + cp.OwnerTypeCustomer + ":" + owner.String()
	if err := rdb.Set(ctx, key, 5, 0).Err(); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	moKey := billing.BalanceCacheKey(cp.BillingDirectionMO, cp.OwnerTypeCustomer, owner)
	if err := (redisBalanceCache{rdb: rdb}).Del(ctx, key, moKey); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if n, _ := rdb.Exists(ctx, key).Result(); n != 0 {
		t.Error("balance cache still present after invalidation")
	}
	if seq, err := rdb.Get(ctx, seqKey).Int(); err != nil || seq != 1 {
		t.Errorf("debit counter = %d (%v), want 1: the invalidation must fence a stale rehydration", seq, err)
	}
	moSeq := "billing:seq:" + cp.BillingDirectionMO + ":" + cp.OwnerTypeCustomer + ":" + owner.String()
	if n, _ := rdb.Exists(ctx, moSeq).Result(); n != 0 {
		t.Error("an MO debit counter was created: nothing reads it, it would only accumulate")
	}
}
