package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestOverdraftFloorAllowsThenRefuses proves the overdraft floor is wired end-to-end: a prepaid customer
// with an overdraft limit may reserve BELOW zero down to -limit, but a reserve that would cross -limit is
// refused with insufficient_credit and leaves the balance untouched.
func TestOverdraftFloorAllowsThenRefuses(t *testing.T) {
	h := newBillingHarness(t, 100)
	h.setConfig(cp.BillingCustomer{BillingMode: cp.BillingPrepaid, OverdraftEnabled: true, OverdraftLimit: intptr(50)})
	ctx := context.Background()

	// 100 - 120 = -20, within the -50 floor → allowed.
	bal, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 120)
	if err != nil {
		t.Fatalf("Reserve within overdraft: %v", err)
	}
	if bal != -20 {
		t.Errorf("balance after overdraft reserve = %d, want -20", bal)
	}
	// -20 - 40 = -60, below the -50 floor → refused, balance unchanged.
	_, err = h.acc.Reserve(ctx, h.owner, uuid.New(), 40)
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve past overdraft = %v, want ErrInsufficientCredit", err)
	}
	if h.balance(t) != -20 {
		t.Errorf("balance after refused reserve = %d, want -20 (unchanged)", h.balance(t))
	}
}

// TestPostpaidSoftLimitNeverBlocks proves a postpaid customer with a SOFT credit limit has no reserve
// floor: a reserve far past the limit still succeeds (the soft limit is advisory, enforced by alerting,
// never by blocking a send).
func TestPostpaidSoftLimitNeverBlocks(t *testing.T) {
	h := newBillingHarness(t, 100)
	h.setConfig(cp.BillingCustomer{BillingMode: cp.BillingPostpaid, CreditLimit: intptr(50), CreditLimitIsHard: false})
	ctx := context.Background()

	bal, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1000)
	if err != nil {
		t.Fatalf("Reserve on soft postpaid: %v", err)
	}
	if bal != -900 {
		t.Errorf("balance = %d, want -900 (soft limit does not block)", bal)
	}
}

// TestPostpaidHardLimitFloor proves a postpaid HARD credit limit is a real floor: reserves are allowed
// down to -limit and refused beyond.
func TestPostpaidHardLimitFloor(t *testing.T) {
	h := newBillingHarness(t, 100)
	h.setConfig(cp.BillingCustomer{BillingMode: cp.BillingPostpaid, CreditLimit: intptr(200), CreditLimitIsHard: true})
	ctx := context.Background()

	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 250); err != nil { // 100-250=-150 ≥ -200
		t.Fatalf("Reserve within hard limit: %v", err)
	}
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 100); !errors.Is(err, errs.ErrInsufficientCredit) { // -150-100=-250 < -200
		t.Fatalf("Reserve past hard limit = %v, want ErrInsufficientCredit", err)
	}
	if h.balance(t) != -150 {
		t.Errorf("balance = %d, want -150", h.balance(t))
	}
}

// TestAccountScopedSoftPostpaidDoesNotBlock proves the step-142c override removal is correct: an
// account-scoped balance is floor-driven by its config (the DB forbids overdraft/hard-limit for
// account-scoped, so only strict-prepaid or soft-postpaid can reach here). A SOFT postpaid customer has no
// floor, so an account-scoped balance still reserves past zero — it must NOT be forced to strict prepaid.
func TestAccountScopedSoftPostpaidDoesNotBlock(t *testing.T) {
	h := newBillingHarness(t, 100)
	// Soft postpaid (advisory limit, no reserve floor) — a config the DB allows for account-scoped balances.
	h.setConfig(cp.BillingCustomer{BillingMode: cp.BillingPostpaid, CreditLimit: intptr(50), CreditLimitIsHard: false})
	ctx := context.Background()

	// An account-scoped balance (distinct owner key) with its own durable balance of 100.
	accountID := uuid.New()
	h.owner.Type = cp.OwnerTypeSMPPAccount
	h.owner.ID = accountID
	if _, _, err := h.repo.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: accountID, Direction: cp.BillingDirectionMT,
		CustomerID: h.owner.CustomerID, EntryType: cp.EntryTopup, Credits: 100,
	}); err != nil {
		t.Fatalf("seed account balance: %v", err)
	}

	// Soft postpaid → no floor → a reserve past zero succeeds even for an account-scoped balance.
	bal, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 300)
	if err != nil {
		t.Fatalf("account-scoped soft-postpaid Reserve = %v, want success (soft limit never blocks)", err)
	}
	if bal != -200 {
		t.Errorf("balance = %d, want -200 (soft postpaid does not block, no strict-prepaid override)", bal)
	}
}

// TestConfigLoadedFromRepoAndReRebuild proves the config-sync path end to end: a billing_customers row is
// loaded from Postgres into the snapshot and drives the reserve floor; and a later rebuild (a config
// change) is picked up on the next Store — the "invalidation" the DoD requires.
func TestConfigLoadedFromRepoAndReRebuild(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()

	// Configure the customer: billing-enabled prepaid with a 40-credit overdraft (billing config lives on
	// the customer row, step-142d; only billing-enabled customers enter the snapshot).
	if _, err := pgtest.Pool(t).Exec(ctx,
		`UPDATE control_plane.customers
		   SET billing_enabled = true, billing_mode = 'prepaid', overdraft_enabled = true, overdraft_limit = 40
		 WHERE id = $1`, h.owner.CustomerID); err != nil {
		t.Fatalf("configure customer overdraft: %v", err)
	}
	// config-sync's rebuild: load from the repo, Store in the provider.
	snap, err := billing.LoadConfigSnapshot(ctx, h.repo)
	if err != nil {
		t.Fatalf("LoadConfigSnapshot: %v", err)
	}
	h.cfg.Store(snap)

	// The loaded overdraft floor (-40) is in force: 100-130 = -30 ≥ -40 → allowed.
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 130); err != nil {
		t.Fatalf("Reserve within loaded overdraft: %v", err)
	}
	if h.balance(t) != -30 {
		t.Fatalf("balance = %d, want -30", h.balance(t))
	}

	// A config change tightens the customer back to strict prepaid; rebuild + Store must take effect.
	if _, err := pgtest.Pool(t).Exec(ctx,
		`UPDATE control_plane.customers SET overdraft_enabled = false, overdraft_limit = NULL WHERE id = $1`,
		h.owner.CustomerID); err != nil {
		t.Fatalf("update customer config: %v", err)
	}
	snap2, err := billing.LoadConfigSnapshot(ctx, h.repo)
	if err != nil {
		t.Fatalf("LoadConfigSnapshot (rebuild): %v", err)
	}
	h.cfg.Store(snap2)

	// Now strict prepaid (floor 0): the balance is already -30, so ANY further reserve is refused.
	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1); !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve after tightening to strict = %v, want ErrInsufficientCredit", err)
	}
}

// TestBalanceCacheHasBoundedTTL proves rehydration now writes the balance cache with a bounded TTL (not
// the old TTL-less key), so any cache/durable drift cannot persist indefinitely.
func TestBalanceCacheHasBoundedTTL(t *testing.T) {
	ttl := 30 * time.Second
	h := newBillingHarnessTTL(t, 100, time.Minute, billing.WithBalanceCacheTTL(ttl))
	ctx := context.Background()

	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 5); err != nil { // rehydrates the cold cache
		t.Fatalf("Reserve: %v", err)
	}
	bkey := "billing:balance:" + cp.BillingDirectionMT + ":" + h.owner.Type + ":" + h.owner.ID.String()
	pttl, err := h.rdb.PTTL(ctx, bkey).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if pttl <= 0 || pttl > ttl {
		t.Errorf("balance cache PTTL = %v, want in (0, %v] — a bounded TTL, never persistent", pttl, ttl)
	}
}

// TestBalanceCacheDriftHealsOnExpiry proves the drift bound in action: a corrupted balance cache is
// discarded when its TTL lapses, and the next reserve rehydrates the correct value from the durable
// authority — the durable balance, not the phantom cache, wins.
func TestBalanceCacheDriftHealsOnExpiry(t *testing.T) {
	h := newBillingHarnessTTL(t, 100, time.Minute, billing.WithBalanceCacheTTL(400*time.Millisecond))
	ctx := context.Background()
	bkey := "billing:balance:" + cp.BillingDirectionMT + ":" + h.owner.Type + ":" + h.owner.ID.String()

	if _, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 10); err != nil { // durable 90, cache 90 (TTL ~400ms)
		t.Fatalf("Reserve: %v", err)
	}
	// Corrupt the cache to a phantom value, preserving its bounded TTL so it will lapse on its own.
	if err := h.rdb.Set(ctx, bkey, 999, redis.KeepTTL).Err(); err != nil {
		t.Fatalf("corrupt cache: %v", err)
	}
	time.Sleep(600 * time.Millisecond) // let the bounded TTL lapse

	bal, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 5) // cold → rehydrate from durable 90 → 85
	if err != nil {
		t.Fatalf("Reserve after expiry: %v", err)
	}
	if bal != 85 {
		t.Errorf("balance after drift-heal = %d, want 85 (durable 90 - 5, NOT the phantom 999)", bal)
	}
	if h.balance(t) != 85 {
		t.Errorf("durable balance = %d, want 85", h.balance(t))
	}
}
