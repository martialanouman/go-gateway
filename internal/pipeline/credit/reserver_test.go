package credit_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/credit"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// fakeBilling is a counting stub of the billing Reserve RPC. calls proves the zero-network-call invariant;
// got captures the last request so a test can assert the resolved owner and cost.
type fakeBilling struct {
	calls int
	got   *pb.ReserveRequest
	resp  *pb.ReserveResponse
	err   error
}

func (f *fakeBilling) Reserve(_ context.Context, in *pb.ReserveRequest, _ ...grpc.CallOption) (*pb.ReserveResponse, error) {
	f.calls++
	f.got = in
	return f.resp, f.err
}

func newReserver(t *testing.T, fake *fakeBilling, scopes ...cp.CustomerBillingScope) *credit.Reserver {
	t.Helper()
	var h credit.Holder
	h.Store(credit.BuildSnapshot(scopes))
	return credit.NewReserver(&h, fake)
}

// TestReserveSkipsWhenDisabled is the load-bearing acceptance test: a customer absent from the snapshot
// (billing disabled) yields no reservation and makes ZERO billing calls (§6.9, step-145).
func TestReserveSkipsWhenDisabled(t *testing.T) {
	fake := &fakeBilling{}
	r := newReserver(t, fake) // empty snapshot: nobody is billed

	reserved, _, err := r.Reserve(context.Background(), uuid.New(), uuid.New(), uuid.New(), 1)
	if err != nil {
		t.Fatalf("Reserve(disabled) err = %v, want nil", err)
	}
	if reserved {
		t.Errorf("Reserve(disabled) reserved = true, want false")
	}
	if fake.calls != 0 {
		t.Errorf("Reserve(disabled) made %d billing calls, want 0", fake.calls)
	}
}

// TestReserveCustomerScope resolves the owner to the customer balance and forwards message_id + segment cost.
func TestReserveCustomerScope(t *testing.T) {
	cust, acct, msg := uuid.New(), uuid.New(), uuid.New()
	fake := &fakeBilling{resp: &pb.ReserveResponse{Reserved: true, BalanceAfter: 97}}
	r := newReserver(t, fake, cp.CustomerBillingScope{CustomerID: cust, Scope: cp.BalanceScopeCustomer})

	reserved, ownerType, err := r.Reserve(context.Background(), acct, cust, msg, 3)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if !reserved || ownerType != cp.OwnerTypeCustomer {
		t.Errorf("(reserved, ownerType) = (%v, %q), want (true, customer)", reserved, ownerType)
	}
	if fake.calls != 1 {
		t.Fatalf("billing calls = %d, want 1", fake.calls)
	}
	if got := fake.got.GetOwner(); got.GetOwnerType() != pb.OwnerType_OWNER_TYPE_CUSTOMER ||
		got.GetOwnerId() != cust.String() || got.GetCustomerId() != cust.String() || got.GetAccountId() != acct.String() {
		t.Errorf("owner = %+v, want type=customer owner_id=customer customer_id=customer account_id=account", got)
	}
	if fake.got.GetMessageId() != msg.String() || fake.got.GetCredits() != 3 {
		t.Errorf("req = (msg %q, credits %d), want (%q, 3)", fake.got.GetMessageId(), fake.got.GetCredits(), msg)
	}
}

// TestReserveAccountScope resolves the owner to the SMPP account balance when balance_scope=smpp_account.
func TestReserveAccountScope(t *testing.T) {
	cust, acct := uuid.New(), uuid.New()
	fake := &fakeBilling{resp: &pb.ReserveResponse{Reserved: true, BalanceAfter: 5}}
	r := newReserver(t, fake, cp.CustomerBillingScope{CustomerID: cust, Scope: cp.BalanceScopeSMPPAccount})

	_, ownerType, err := r.Reserve(context.Background(), acct, cust, uuid.New(), 1)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ownerType != cp.OwnerTypeSMPPAccount {
		t.Errorf("ownerType = %q, want smpp_account", ownerType)
	}
	if got := fake.got.GetOwner(); got.GetOwnerType() != pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT || got.GetOwnerId() != acct.String() {
		t.Errorf("owner = %+v, want type=smpp_account owner_id=account", got)
	}
}

// TestReserveInsufficientCredit maps a business denial (reserved=false) to the coded reject the router turns
// into a rejected CDR / HTTP 402 — never a transient retry.
func TestReserveInsufficientCredit(t *testing.T) {
	cust := uuid.New()
	fake := &fakeBilling{resp: &pb.ReserveResponse{Reserved: false, Code: "insufficient_credit"}}
	r := newReserver(t, fake, cp.CustomerBillingScope{CustomerID: cust, Scope: cp.BalanceScopeCustomer})

	_, _, err := r.Reserve(context.Background(), uuid.New(), cust, uuid.New(), 2)
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve(denied) err = %v, want ErrInsufficientCredit", err)
	}
	if code, ok := errs.CodeOf(err); !ok || code != errs.ErrInsufficientCredit {
		t.Errorf("CodeOf = (%q, %v), want (insufficient_credit, true)", code, ok)
	}
}

// TestReserveTransportErrorIsRaw proves a billing-svc transport fault is returned WITHOUT a platform code, so
// the router treats it as transient and retries (fail-closed: a billed message is not sent until billing answers).
func TestReserveTransportErrorIsRaw(t *testing.T) {
	cust := uuid.New()
	fake := &fakeBilling{err: errors.New("connection refused")}
	r := newReserver(t, fake, cp.CustomerBillingScope{CustomerID: cust, Scope: cp.BalanceScopeCustomer})

	_, _, err := r.Reserve(context.Background(), uuid.New(), cust, uuid.New(), 1)
	if err == nil {
		t.Fatal("Reserve(transport error) err = nil, want the raw error")
	}
	if _, ok := errs.CodeOf(err); ok {
		t.Errorf("transport error carries a platform code %v, want none (so the router retries)", err)
	}
}
