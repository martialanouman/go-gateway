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
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// billingHarness wires the Accountant against real Redis + Postgres and seeds a customer with an initial
// durable balance. The Redis balance cache starts COLD, so the first reserve exercises rehydration.
type billingHarness struct {
	acc   *billing.Accountant
	repo  *postgres.BillingRepo
	rdb   *redis.Client
	cfg   *billing.ConfigProvider
	owner billing.Owner
}

// setConfig publishes a billing configuration for the harness's customer (overdraft/postpaid floor). An
// empty provider (the default) fails closed to strict prepaid, so tests that never call this are unchanged.
func (h *billingHarness) setConfig(c cp.BillingCustomer) {
	c.CustomerID = h.owner.CustomerID
	h.cfg.Store(billing.BuildConfigSnapshot([]cp.BillingCustomer{c}, nil))
}

func newBillingHarness(t *testing.T, initialBalance int) *billingHarness {
	return newBillingHarnessTTL(t, initialBalance, time.Minute)
}

func newBillingHarnessTTL(t *testing.T, initialBalance int, holdTTL time.Duration, extra ...billing.Option) *billingHarness {
	t.Helper()
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	ctx := context.Background()

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-core-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	repo := postgres.NewBillingRepo(pool)
	// Establish the durable balance with a topup entry; the Redis cache stays absent (cold).
	if _, _, err := repo.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
		CustomerID: customerID, EntryType: cp.EntryTopup, Credits: initialBalance,
	}); err != nil {
		t.Fatalf("seed balance: %v", err)
	}

	cfg := &billing.ConfigProvider{} // empty → strict prepaid until a test calls setConfig
	opts := append([]billing.Option{billing.WithHoldTTL(holdTTL), billing.WithConfigSource(cfg)}, extra...)
	acc := billing.New(rdb, repo, opts...)
	if err := acc.EnsureNonClustered(ctx); err != nil {
		t.Fatalf("EnsureNonClustered on a single Redis: %v", err)
	}
	return &billingHarness{acc: acc, repo: repo, rdb: rdb, cfg: cfg, owner: billing.Owner{
		Type: cp.OwnerTypeCustomer, ID: customerID, CustomerID: customerID,
	}}
}

func (h *billingHarness) balance(t *testing.T) int {
	t.Helper()
	credits, _, err := h.repo.Balance(context.Background(), h.owner.Type, h.owner.ID, cp.BillingDirectionMT)
	if err != nil {
		t.Fatalf("read balance: %v", err)
	}
	return credits
}

// cachedBalance reads the Redis balance cache for the owner. present=false means the key is absent (cold),
// which is consistent with any durable value (the next reserve rehydrates it).
func (h *billingHarness) cachedBalance(t *testing.T) (value int, present bool) {
	t.Helper()
	key := "billing:balance:" + cp.BillingDirectionMT + ":" + h.owner.Type + ":" + h.owner.ID.String()
	v, err := h.rdb.Get(context.Background(), key).Int()
	if errors.Is(err, redis.Nil) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read cached balance: %v", err)
	}
	return v, true
}

// moBalance reads the durable MO meter for the owner.
func (h *billingHarness) moBalance(t *testing.T) int {
	t.Helper()
	credits, _, err := h.repo.Balance(context.Background(), h.owner.Type, h.owner.ID, cp.BillingDirectionMO)
	if err != nil {
		t.Fatalf("read MO balance: %v", err)
	}
	return credits
}

func (h *billingHarness) ledgerCount(t *testing.T, entryType cp.EntryType, messageID uuid.UUID) int {
	t.Helper()
	ok, err := h.repo.LedgerEntryExists(context.Background(), messageID, entryType)
	if err != nil {
		t.Fatalf("ledger exists: %v", err)
	}
	if ok {
		return 1
	}
	return 0
}

// TestReserveCaptureSuccess: a reserve debits the balance (rehydrating the cold cache from Postgres),
// and a capture confirms without further change; the ledger records both.
func TestReserveCaptureSuccess(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	bal, err := h.acc.Reserve(ctx, h.owner, msg, 3)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if bal != 97 {
		t.Errorf("balance after reserve = %d, want 97", bal)
	}
	if h.balance(t) != 97 {
		t.Errorf("durable balance = %d, want 97", h.balance(t))
	}

	charged, err := h.acc.Capture(ctx, h.owner, msg)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if charged != 3 {
		t.Errorf("credits charged = %d, want 3", charged)
	}
	if h.balance(t) != 97 {
		t.Errorf("durable balance after capture = %d, want 97 (capture must not re-debit)", h.balance(t))
	}
	if h.ledgerCount(t, cp.EntryReserve, msg) != 1 || h.ledgerCount(t, cp.EntryCapture, msg) != 1 {
		t.Error("ledger must have one reserve and one capture entry")
	}
}

// TestReserveReleaseFailure: a reserve holds credits, then a release (message failed before send) refunds
// them; the balance returns to its start and the ledger records the reserve and the release.
func TestReserveReleaseFailure(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	if _, err := h.acc.Reserve(ctx, h.owner, msg, 5); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if h.balance(t) != 95 {
		t.Fatalf("balance after reserve = %d, want 95", h.balance(t))
	}
	if err := h.acc.Release(ctx, h.owner, msg); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if h.balance(t) != 100 {
		t.Errorf("balance after release = %d, want 100 (refunded)", h.balance(t))
	}
	if h.ledgerCount(t, cp.EntryReserve, msg) != 1 || h.ledgerCount(t, cp.EntryRelease, msg) != 1 {
		t.Error("ledger must have one reserve and one release entry")
	}
}

// TestIdempotencyInvariantC is the BLOCKING invariant-c test (§6.9): a double message_id — reserve twice,
// capture twice — debits the balance EXACTLY ONCE and writes exactly one reserve and one capture entry.
func TestIdempotencyInvariantC(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	bal1, err := h.acc.Reserve(ctx, h.owner, msg, 4)
	if err != nil {
		t.Fatalf("Reserve 1: %v", err)
	}
	bal2, err := h.acc.Reserve(ctx, h.owner, msg, 4) // replay — must be idempotent (a hold already exists)
	if err != nil {
		t.Fatalf("Reserve 2 (replay): %v", err)
	}
	if bal1 != 96 || bal2 != 96 {
		t.Fatalf("double reserve balances = %d, %d, want 96, 96 (debited once)", bal1, bal2)
	}

	if _, err := h.acc.Capture(ctx, h.owner, msg); err != nil {
		t.Fatalf("Capture 1: %v", err)
	}
	if _, err := h.acc.Capture(ctx, h.owner, msg); err != nil { // duplicate delivery — no-op
		t.Fatalf("Capture 2 (duplicate): %v", err)
	}

	if got := h.balance(t); got != 96 {
		t.Errorf("balance after double reserve+capture = %d, want 96 (debited EXACTLY once)", got)
	}
	// Exactly one reserve and one capture entry — no double accounting.
	var reserves, captures int
	if err := poolCount(t, h, msg, "reserve", &reserves); err != nil {
		t.Fatal(err)
	}
	if err := poolCount(t, h, msg, "capture", &captures); err != nil {
		t.Fatal(err)
	}
	if reserves != 1 || captures != 1 {
		t.Errorf("ledger rows = %d reserve / %d capture, want 1 / 1 (no double accounting)", reserves, captures)
	}
}

// TestInsufficientNoLedger: a reserve beyond the balance is denied with insufficient_credit and writes NO
// ledger entry, leaving the balance untouched (§6.9).
func TestInsufficientNoLedger(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	_, err := h.acc.Reserve(ctx, h.owner, msg, 150)
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve(150) error = %v, want ErrInsufficientCredit", err)
	}
	if h.balance(t) != 100 {
		t.Errorf("balance = %d, want 100 (unchanged on denial)", h.balance(t))
	}
	if h.ledgerCount(t, cp.EntryReserve, msg) != 0 {
		t.Error("a denied reserve must write NO ledger entry")
	}
}

// poolCount counts ledger rows of an entry type for a message via the harness's pool.
func poolCount(t *testing.T, h *billingHarness, messageID uuid.UUID, entryType string, out *int) error {
	t.Helper()
	pool := pgtest.Pool(t)
	return pool.QueryRow(context.Background(),
		`SELECT count(*) FROM control_plane.billing_ledger WHERE message_id = $1 AND entry_type = $2`,
		messageID, entryType).Scan(out)
}
