package billing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/credit"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// seedAccount inserts an SMPP account for the harness customer and returns its id. The credit reserver always
// carries account_id to attribute the ledger charge (§6.9), and billing_ledger.account_id has an FK to
// smpp_accounts — so a reserve that touches the durable ledger needs a real account, exactly as in production.
func seedAccount(t *testing.T, customerID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pgtest.Pool(t).QueryRow(context.Background(),
		`INSERT INTO control_plane.smpp_accounts (customer_id, name) VALUES ($1, 'credit-reserve-test') RETURNING id`,
		customerID).Scan(&id); err != nil {
		t.Fatalf("seed smpp account: %v", err)
	}
	return id
}

// TestCreditReserverIdempotentUnderDoubleDelivery drives the router's credit stage (credit.Reserver) over the
// REAL billing gRPC server and accountant (Redis + Postgres) and proves invariant (c) end-to-end: a
// redelivered mt.inbound reserves against the same message_id and never debits the balance twice. This is the
// path that matters for step-145 — the reserver forwards the message_id, and billing-svc's idempotency does
// the rest — so a Kafka at-least-once redelivery (or a retry after a billing deadline that actually
// committed) heals without over-charging the customer.
func TestCreditReserverIdempotentUnderDoubleDelivery(t *testing.T) {
	h := newBillingHarness(t, 100)
	client := newBillingGRPCClient(t, h)

	var holder credit.Holder
	holder.Store(credit.BuildSnapshot([]cp.CustomerBillingScope{
		{CustomerID: h.owner.CustomerID, Scope: cp.BalanceScopeCustomer},
	}))
	reserver := credit.NewReserver(&holder, client)

	ctx := context.Background()
	accountID := seedAccount(t, h.owner.CustomerID)
	messageID := uuid.New()

	reserved, ownerType, err := reserver.Reserve(ctx, accountID, h.owner.CustomerID, messageID, 3)
	if err != nil || !reserved || ownerType != cp.OwnerTypeCustomer {
		t.Fatalf("first Reserve = (%v, %q, %v), want (true, customer, nil)", reserved, ownerType, err)
	}
	if got := h.balance(t); got != 97 {
		t.Fatalf("balance after first reserve = %d, want 97", got)
	}

	// Redelivery of the SAME message_id: idempotent — still reserved, but no second debit (invariant c).
	reserved2, _, err := reserver.Reserve(ctx, accountID, h.owner.CustomerID, messageID, 3)
	if err != nil || !reserved2 {
		t.Fatalf("redelivered Reserve = (%v, %v), want (true, nil)", reserved2, err)
	}
	if got := h.balance(t); got != 97 {
		t.Errorf("balance after redelivered reserve = %d, want 97 (reserve is idempotent by message_id)", got)
	}
}

// TestCreditReserverSkipsDisabledCustomer proves the zero-network-call gate against the real gRPC client: a
// customer absent from the snapshot (billing disabled) is never billed — the balance is untouched.
func TestCreditReserverSkipsDisabledCustomer(t *testing.T) {
	h := newBillingHarness(t, 100)
	client := newBillingGRPCClient(t, h)

	var holder credit.Holder // empty snapshot: the customer is not billing-enabled
	holder.Store(credit.BuildSnapshot(nil))
	reserver := credit.NewReserver(&holder, client)

	reserved, _, err := reserver.Reserve(context.Background(), uuid.New(), h.owner.CustomerID, uuid.New(), 3)
	if err != nil {
		t.Fatalf("Reserve(disabled) err = %v, want nil", err)
	}
	if reserved {
		t.Error("a billing-disabled customer must not be reserved")
	}
	if got := h.balance(t); got != 100 {
		t.Errorf("balance = %d, want 100 (a disabled customer is never debited)", got)
	}
}
