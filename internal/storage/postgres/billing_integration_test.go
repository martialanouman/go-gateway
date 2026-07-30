package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestBillingRepoBalanceAndConfig proves the read side against real Postgres: an unseeded balance reads
// as absent (a valid zero), a configured customer maps every billing column (including the nullable
// overdraft/credit limits, which now live on the customer row, step-142d), and a non-existent customer
// reads as not-found rather than an error.
func TestBillingRepoBalanceAndConfig(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers
		   (name, billing_enabled, billing_mode, overdraft_enabled, overdraft_limit, credit_limit, credit_limit_is_hard)
		 VALUES ('billing-repo-test', true, 'prepaid', true, 500, 1000, true) RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	// Balance not yet recorded → absent (0, false), not an error.
	if credits, found, err := repo.Balance(ctx, cp.OwnerTypeCustomer, customerID, cp.BillingDirectionMT); err != nil || found || credits != 0 {
		t.Fatalf("Balance(unseeded) = (%d, %v, %v), want (0, false, nil)", credits, found, err)
	}

	// Billing config maps every column, nullable limits included.
	bc, found, err := repo.BillingCustomer(ctx, customerID)
	if err != nil || !found {
		t.Fatalf("BillingCustomer = (found=%v, err=%v), want found", found, err)
	}
	if bc.BillingMode != cp.BillingPrepaid || !bc.OverdraftEnabled || !bc.CreditLimitIsHard {
		t.Errorf("config flags = %+v, want prepaid/overdraft/hard-limit", bc)
	}
	if bc.OverdraftLimit == nil || *bc.OverdraftLimit != 500 || bc.CreditLimit == nil || *bc.CreditLimit != 1000 {
		t.Errorf("limits = overdraft %v / credit %v, want 500 / 1000", bc.OverdraftLimit, bc.CreditLimit)
	}

	// A non-existent customer reads as not-found, not an error.
	if _, found, err := repo.BillingCustomer(ctx, uuid.New()); err != nil || found {
		t.Fatalf("BillingCustomer(missing) = (found=%v, err=%v), want (false, nil)", found, err)
	}
}

// TestBillingRepoListBillingScopes proves the router-facing gate/owner snapshot source: only billing-enabled
// customers appear, each carries its balance_scope, and a billing-disabled customer is absent (so the router
// makes no billing call for it). The DB is shared across tests, so the assertions are keyed by the two
// customers this test seeds rather than the total count.
func TestBillingRepoListBillingScopes(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var enabledAccount, disabled uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name, billing_enabled, balance_scope)
		 VALUES ('scope-enabled-account', true, 'smpp_account') RETURNING id`).Scan(&enabledAccount); err != nil {
		t.Fatalf("seed enabled customer: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name, billing_enabled) VALUES ('scope-disabled', false) RETURNING id`).
		Scan(&disabled); err != nil {
		t.Fatalf("seed disabled customer: %v", err)
	}

	scopes, err := repo.ListBillingScopes(ctx)
	if err != nil {
		t.Fatalf("ListBillingScopes: %v", err)
	}
	byID := make(map[uuid.UUID]cp.BalanceScope, len(scopes))
	for _, s := range scopes {
		byID[s.CustomerID] = s.Scope
	}
	if got, ok := byID[enabledAccount]; !ok || got != cp.BalanceScopeSMPPAccount {
		t.Errorf("enabled account-scope customer = (%q, present=%v), want smpp_account present", got, ok)
	}
	if _, ok := byID[disabled]; ok {
		t.Error("billing-disabled customer must be absent from ListBillingScopes (no billing call is made for it)")
	}
}

// TestBillingRepoListExternalBillingConfigs proves the §6.10 customers→providers join: a customer referencing
// an ACTIVE provider appears with its mode/timeout/policy; one referencing a DISABLED provider is absent
// (kill switch); one with no provider is absent (pure internal). Keyed by the seeded customers (shared DB).
func TestBillingRepoListExternalBillingConfigs(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var activeProvider, disabledProvider uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.external_billing_providers (name, base_url, mode, sync_call_timeout_ms, failure_policy)
		 VALUES ('active-prov', 'https://ext.example', 'consume_delegate_sync', 120, 'fail_closed') RETURNING id`).
		Scan(&activeProvider); err != nil {
		t.Fatalf("seed active provider: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.external_billing_providers (name, base_url, mode, status)
		 VALUES ('disabled-prov', 'https://ext.example', 'balance_check', 'disabled') RETURNING id`).
		Scan(&disabledProvider); err != nil {
		t.Fatalf("seed disabled provider: %v", err)
	}

	var withActive, withDisabled, plain uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name, billing_enabled, external_billing_provider_id)
		 VALUES ('ext-active', true, $1) RETURNING id`, activeProvider).Scan(&withActive); err != nil {
		t.Fatalf("seed customer(active): %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name, billing_enabled, external_billing_provider_id)
		 VALUES ('ext-disabled', true, $1) RETURNING id`, disabledProvider).Scan(&withDisabled); err != nil {
		t.Fatalf("seed customer(disabled): %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name, billing_enabled) VALUES ('ext-none', true) RETURNING id`).
		Scan(&plain); err != nil {
		t.Fatalf("seed customer(none): %v", err)
	}

	configs, err := repo.ListExternalBillingConfigs(ctx)
	if err != nil {
		t.Fatalf("ListExternalBillingConfigs: %v", err)
	}
	byID := make(map[uuid.UUID]cp.CustomerExternalBilling, len(configs))
	for _, c := range configs {
		byID[c.CustomerID] = c
	}
	got, ok := byID[withActive]
	if !ok {
		t.Fatal("customer with an active provider must appear")
	}
	if got.ProviderID != activeProvider || got.Mode != cp.ExternalModeConsumeSync ||
		got.SyncTimeoutMs == nil || *got.SyncTimeoutMs != 120 || got.FailurePolicy != cp.FailClosed {
		t.Errorf("config = %+v, want active provider sync/120/fail_closed", got)
	}
	if _, ok := byID[withDisabled]; ok {
		t.Error("a customer whose provider is disabled must be absent (kill switch)")
	}
	if _, ok := byID[plain]; ok {
		t.Error("a customer with no provider must be absent (pure internal billing)")
	}
}

// TestBillingRepoTransfer proves the admin transfer (step-148): it moves MT credit between two accounts of a
// customer as two zero-sum EntryTransfer rows, refuses to overdraw the source, and is idempotent by key.
func TestBillingRepoTransfer(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID, acct1, acct2 uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name, balance_scope) VALUES ('xfer', 'smpp_account') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.smpp_accounts (customer_id, name) VALUES ($1,'a1') RETURNING id`, customerID).Scan(&acct1); err != nil {
		t.Fatalf("seed acct1: %v", err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.smpp_accounts (customer_id, name) VALUES ($1,'a2') RETURNING id`, customerID).Scan(&acct2); err != nil {
		t.Fatalf("seed acct2: %v", err)
	}
	// Fund acct1 with 100 (topup).
	if _, _, err := repo.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: acct1, Direction: cp.BillingDirectionMT,
		CustomerID: customerID, AccountID: &acct1, EntryType: cp.EntryTopup, Credits: 100,
	}); err != nil {
		t.Fatalf("fund acct1: %v", err)
	}

	// The handler passes MessageID on both legs; the repo must strip it before the ledger insert, else the
	// two legs collide on the partial unique index (message_id, entry_type, created_at) with created_at
	// constant in the tx. Passing it here proves the repo neutralises it.
	sharedMsg := uuid.New()
	mkPair := func(amount int) (cp.LedgerEntry, cp.LedgerEntry) {
		debit := cp.LedgerEntry{OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: acct1, Direction: cp.BillingDirectionMT, CustomerID: customerID, AccountID: &acct1, MessageID: &sharedMsg, EntryType: cp.EntryTransfer, Credits: -amount}
		credit := cp.LedgerEntry{OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: acct2, Direction: cp.BillingDirectionMT, CustomerID: customerID, AccountID: &acct2, MessageID: &sharedMsg, EntryType: cp.EntryTransfer, Credits: amount}
		return debit, credit
	}

	// Overdraw is refused.
	bigD, bigC := mkPair(500)
	if _, _, err := repo.Transfer(ctx, bigD, bigC, uuid.New()); !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Errorf("Transfer(overdraw) must fail with insufficient_credit, got %v", err)
	}

	// A valid transfer moves 30, returning two mirrored ledger rows.
	idem := uuid.New()
	d, c := mkPair(30)
	rows, applied, err := repo.Transfer(ctx, d, c, idem)
	if err != nil || !applied {
		t.Fatalf("Transfer = (%v, %v), want (true, nil)", applied, err)
	}
	if len(rows) != 2 || rows[0].Credits != -30 || rows[1].Credits != 30 {
		t.Errorf("transfer rows = %+v, want a -30/+30 pair", rows)
	}
	// Both legs actually landed in the ledger (the B1 unique-index collision would leave 0/1).
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.billing_ledger WHERE customer_id=$1 AND entry_type='transfer'`, customerID).Scan(&n); err != nil {
		t.Fatalf("count transfer entries: %v", err)
	}
	if n != 2 {
		t.Errorf("transfer ledger entries = %d, want 2 (both legs committed)", n)
	}
	if b, _, _ := repo.Balance(ctx, cp.OwnerTypeSMPPAccount, acct1, cp.BillingDirectionMT); b != 70 {
		t.Errorf("acct1 = %d, want 70", b)
	}
	if b, _, _ := repo.Balance(ctx, cp.OwnerTypeSMPPAccount, acct2, cp.BillingDirectionMT); b != 30 {
		t.Errorf("acct2 = %d, want 30", b)
	}
	// A replay with the same key is a no-op (no double-move).
	d2, c2 := mkPair(30)
	if _, applied, err := repo.Transfer(ctx, d2, c2, idem); err != nil || applied {
		t.Fatalf("replay Transfer applied=%v err=%v, want (false, nil)", applied, err)
	}
	if b, _, _ := repo.Balance(ctx, cp.OwnerTypeSMPPAccount, acct1, cp.BillingDirectionMT); b != 70 {
		t.Errorf("acct1 after replay = %d, want 70 (idempotent)", b)
	}
}

// TestBillingRepoChangeBalanceScope proves the guarded scope flip (step-148): it flips when all balances are
// zero and refuses (conflict) when any is non-zero.
func TestBillingRepoChangeBalanceScope(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('scope-flip') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	owner := cp.BalanceOwner{OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID}

	// All balances zero → flip succeeds.
	if err := repo.ChangeBalanceScope(ctx, customerID, []cp.BalanceOwner{owner}, cp.OwnerTypeSMPPAccount); err != nil {
		t.Fatalf("ChangeBalanceScope(zero balances) = %v, want nil", err)
	}
	var scope string
	if err := pool.QueryRow(ctx, `SELECT balance_scope FROM control_plane.customers WHERE id=$1`, customerID).Scan(&scope); err != nil || scope != "smpp_account" {
		t.Fatalf("balance_scope = %q (err %v), want smpp_account", scope, err)
	}

	// Fund the owner, then a flip back must be refused (409) because the balance is non-zero.
	if _, _, err := repo.RecordDurable(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
		CustomerID: customerID, EntryType: cp.EntryTopup, Credits: 50,
	}); err != nil {
		t.Fatalf("fund owner: %v", err)
	}
	if err := repo.ChangeBalanceScope(ctx, customerID, []cp.BalanceOwner{owner}, string(cp.BalanceScopeCustomer)); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("ChangeBalanceScope(non-zero balance) = %v, want conflict", err)
	}
}

// TestBillingRepoConsumedCredits proves the §6.10 reconciliation read: a captured message counts its reserve
// debit as settled consumption, a released message counts nothing, and an in-flight (reserved-only) message
// counts nothing. Uses a fresh customer so the shared DB does not perturb the total.
func TestBillingRepoConsumedCredits(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('consumed-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	captured, released, inflight := uuid.New(), uuid.New(), uuid.New()
	reserve := func(msg uuid.UUID, credits int) {
		if _, _, err := repo.RecordDurable(ctx, cp.LedgerEntry{
			OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, MessageID: &msg, EntryType: cp.EntryReserve, Credits: -credits,
		}); err != nil {
			t.Fatalf("reserve %s: %v", msg, err)
		}
	}
	terminal := func(msg uuid.UUID, et cp.EntryType, credits int) {
		if _, _, err := repo.RecordDurable(ctx, cp.LedgerEntry{
			OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, MessageID: &msg, EntryType: et, Credits: credits,
		}); err != nil {
			t.Fatalf("%s %s: %v", et, msg, err)
		}
	}

	reserve(captured, 3)
	terminal(captured, cp.EntryCapture, 0) // captured → counts 3
	reserve(released, 5)
	terminal(released, cp.EntryRelease, 5) // released → counts 0
	reserve(inflight, 7)                   // reserved, not yet captured → counts 0

	consumed, err := repo.ConsumedCredits(ctx, customerID)
	if err != nil {
		t.Fatalf("ConsumedCredits: %v", err)
	}
	if consumed != 3 {
		t.Errorf("ConsumedCredits = %d, want 3 (only the captured message's reserve debit)", consumed)
	}
}

// TestBillingRepoRecordAndIdempotency proves the durable write path: RecordDurable appends the ledger AND
// reconciles the balance in one transaction, the ledger is append-only (every entry accumulates, none is
// updated), and LedgerEntryExists is the cross-partition idempotency guard the capture path reads.
func TestBillingRepoRecordAndIdempotency(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewBillingRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx,
		`INSERT INTO control_plane.customers (name) VALUES ('billing-ledger-test') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	messageID := uuid.New()

	entry := func(et cp.EntryType, mid *uuid.UUID, credits int) cp.LedgerEntry {
		return cp.LedgerEntry{
			OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID, Direction: cp.BillingDirectionMT,
			CustomerID: customerID, MessageID: mid, EntryType: et, Credits: credits,
		}
	}

	// topup +100 → the returned balance is the atomic post-adjustment value (applied, no message_id).
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryTopup, nil, 100)); err != nil || bal != 100 || !applied {
		t.Fatalf("record topup = (%d, %v, %v), want (100, true, nil)", bal, applied, err)
	}
	// reserve -3 → 97.
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryReserve, &messageID, -3)); err != nil || bal != 97 || !applied {
		t.Fatalf("record reserve = (%d, %v, %v), want (97, true, nil)", bal, applied, err)
	}

	// Balance reflects the running sum of deltas.
	if credits, found, err := repo.Balance(ctx, cp.OwnerTypeCustomer, customerID, cp.BillingDirectionMT); err != nil || !found || credits != 97 {
		t.Fatalf("Balance after reserve = (%d, %v, %v), want (97, true, nil)", credits, found, err)
	}

	// Pre-capture idempotency guard: reserve exists, capture does not yet.
	if ok, err := repo.LedgerEntryExists(ctx, messageID, cp.EntryReserve); err != nil || !ok {
		t.Fatalf("LedgerEntryExists(reserve) = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := repo.LedgerEntryExists(ctx, messageID, cp.EntryCapture); err != nil || ok {
		t.Fatalf("LedgerEntryExists(capture, pre) = (%v, %v), want (false, nil)", ok, err)
	}

	// Replaying the SAME reserve is idempotent: no second debit, no second row, applied=false, balance held.
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryReserve, &messageID, -3)); err != nil || bal != 97 || applied {
		t.Fatalf("replay reserve = (%d, %v, %v), want (97, false, nil) — idempotent, no double debit", bal, applied, err)
	}

	// Capture confirms the reservation with credits=0: the reserve already debited, so the balance is
	// unchanged (delta 0 → balance stays 97) and the ledger stays self-consistent.
	if bal, applied, err := repo.RecordDurable(ctx, entry(cp.EntryCapture, &messageID, 0)); err != nil || bal != 97 || !applied {
		t.Fatalf("record capture = (%d, %v, %v), want (97, true, nil)", bal, applied, err)
	}
	if ok, err := repo.LedgerEntryExists(ctx, messageID, cp.EntryCapture); err != nil || !ok {
		t.Fatalf("LedgerEntryExists(capture, post) = (%v, %v), want (true, nil)", ok, err)
	}
	if credits, _, _ := repo.Balance(ctx, cp.OwnerTypeCustomer, customerID, cp.BillingDirectionMT); credits != 97 {
		t.Errorf("Balance after capture = %d, want 97 (capture must not double-debit)", credits)
	}

	// Append-only, and the idempotent replay added NOTHING: 3 entries for this owner (topup, reserve,
	// capture) — the second reserve was a no-op, not a fourth row.
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM control_plane.billing_ledger WHERE owner_type = 'customer' AND owner_id = $1`,
		customerID).Scan(&rows); err != nil {
		t.Fatalf("count ledger: %v", err)
	}
	if rows != 3 {
		t.Errorf("ledger rows = %d, want 3 (topup, reserve, capture — replay added no row)", rows)
	}

	// Audit invariant (§6.14): the balance can be reconstructed by summing the signed credits, so
	// SUM(credits) must equal the current balance (100 - 3 + 0 = 97).
	var sum int
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(sum(credits), 0) FROM control_plane.billing_ledger WHERE owner_type = 'customer' AND owner_id = $1`,
		customerID).Scan(&sum); err != nil {
		t.Fatalf("sum ledger: %v", err)
	}
	if sum != 97 {
		t.Errorf("SUM(credits) = %d, want 97 (must equal the balance — ledger self-consistency)", sum)
	}
}
