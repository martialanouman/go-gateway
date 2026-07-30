package billing_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/billing/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// newExternalBillerGRPCClient serves the gRPC Server over an ExternalBiller wrapping the harness accountant,
// with the harness customer configured for balance_check against the given stub provider — so a test drives
// the real Server → ExternalBiller → provider → accountant path end to end (the main.go wiring shape).
func newExternalBillerGRPCClient(t *testing.T, h *billingHarness, provider *billing.StubProvider, policy cp.BillingFailurePolicy) pb.BillingClient {
	t.Helper()
	h.cfg.Store(billing.BuildConfigSnapshot(
		[]cp.BillingCustomer{{CustomerID: h.owner.CustomerID, BillingMode: cp.BillingPrepaid}},
		[]cp.CustomerExternalBilling{{
			CustomerID: h.owner.CustomerID, ProviderID: uuid.New(),
			Mode: cp.ExternalModeBalanceCheck, FailurePolicy: policy,
		}},
	))
	biller := billing.NewExternalBiller(h.acc, h.cfg, provider)
	return serveBillingCore(t, biller, h.repo)
}

// TestBillingGRPCExternalDenyIsInsufficientCredit proves the decorator is wired into the server: an external
// balance_check denial surfaces as a normal Reserved=false + insufficient_credit response (not a gRPC error),
// and the internal balance is untouched (the external gate ran before the internal reserve).
func TestBillingGRPCExternalDenyIsInsufficientCredit(t *testing.T) {
	h := newBillingHarness(t, 100)
	provider := billing.NewStubProvider()
	provider.SetAllowed(false)
	client := newExternalBillerGRPCClient(t, h, provider, cp.FailClosed)

	rr, err := client.Reserve(context.Background(),
		&pb.ReserveRequest{MessageId: uuid.NewString(), Owner: pbCustomerOwner(h), Credits: 3})
	if err != nil {
		t.Fatalf("Reserve(external deny) returned a gRPC error, want a normal response: %v", err)
	}
	if rr.GetReserved() || rr.GetCode() != "insufficient_credit" {
		t.Errorf("Reserve resp = %+v, want reserved=false code=insufficient_credit", rr)
	}
	if got := h.balance(t); got != 100 {
		t.Errorf("balance = %d, want 100 (an external denial must not reach the internal reserve)", got)
	}
}

// TestBillingGRPCExternalUnavailableIsGRPCError proves the fail_closed path: a provider fault surfaces as a
// gRPC Unavailable status (external_billing_unavailable), which the router retries — a billed message is held,
// never sent unconfirmed.
func TestBillingGRPCExternalUnavailableIsGRPCError(t *testing.T) {
	h := newBillingHarness(t, 100)
	provider := billing.NewStubProvider()
	provider.SetError(errors.New("provider down"))
	client := newExternalBillerGRPCClient(t, h, provider, cp.FailClosed)

	_, err := client.Reserve(context.Background(),
		&pb.ReserveRequest{MessageId: uuid.NewString(), Owner: pbCustomerOwner(h), Credits: 3})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Reserve(provider fault, fail_closed) code = %v, want Unavailable", status.Code(err))
	}
	if status.Convert(err).Message() != "external_billing_unavailable" {
		t.Errorf("status message = %q, want external_billing_unavailable", status.Convert(err).Message())
	}
}

// newBillingGRPCClient wires the real Billing Server (over the harness's Redis+Postgres accountant) to an
// in-memory bufconn transport, so the test exercises the actual gRPC codec, status machinery and handler
// mapping — not a direct method call.
func newBillingGRPCClient(t *testing.T, h *billingHarness) pb.BillingClient {
	t.Helper()
	return serveBillingCore(t, h.acc, h.repo)
}

// serveBillingCore serves a Billing Server over the given core (the raw accountant, or the ExternalBiller
// decorator) on an in-memory bufconn transport, so a test exercises the real gRPC codec and handler mapping.
func serveBillingCore(t *testing.T, core billing.Core, balances billing.BalanceReader) pb.BillingClient {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterBillingServer(srv, billing.NewServer(core, balances))

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewBillingClient(conn)
}

func pbCustomerOwner(h *billingHarness) *pb.Owner {
	return &pb.Owner{
		OwnerType:  pb.OwnerType_OWNER_TYPE_CUSTOMER,
		OwnerId:    h.owner.ID.String(),
		CustomerId: h.owner.CustomerID.String(),
	}
}

// TestBillingGRPCLifecycle drives reserve → capture → get-balances → record-mo over the wire and checks the
// responses and the durable effects.
func TestBillingGRPCLifecycle(t *testing.T) {
	h := newBillingHarness(t, 100)
	client := newBillingGRPCClient(t, h)
	ctx := context.Background()
	owner := pbCustomerOwner(h)
	msg := uuid.NewString()

	rr, err := client.Reserve(ctx, &pb.ReserveRequest{MessageId: msg, Owner: owner, Credits: 3})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if !rr.GetReserved() || rr.GetBalanceAfter() != 97 {
		t.Errorf("Reserve resp = %+v, want reserved=true balance=97", rr)
	}

	cr, err := client.Capture(ctx, &pb.CaptureRequest{MessageId: msg, Owner: owner})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !cr.GetCaptured() || cr.GetCreditsCharged() != 3 || cr.GetBalanceAfter() != 97 {
		t.Errorf("Capture resp = %+v, want captured=true charged=3 balance=97", cr)
	}

	gb, err := client.GetBalances(ctx, &pb.GetBalancesRequest{Owner: owner})
	if err != nil {
		t.Fatalf("GetBalances: %v", err)
	}
	if got := mtCredits(gb); got != 97 {
		t.Errorf("GetBalances MT = %d, want 97", got)
	}

	mo, err := client.RecordMO(ctx, &pb.RecordMORequest{MessageId: uuid.NewString(), Owner: owner, Credits: 4})
	if err != nil {
		t.Fatalf("RecordMO: %v", err)
	}
	if mo.GetBalanceAfter() != -4 || mo.GetFloored() {
		t.Errorf("RecordMO resp = %+v, want balance=-4 floored=false", mo)
	}
}

// TestBillingGRPCInsufficientCredit proves a denied reserve is a normal response (reserved=false + machine
// code), not a gRPC error.
func TestBillingGRPCInsufficientCredit(t *testing.T) {
	h := newBillingHarness(t, 10)
	client := newBillingGRPCClient(t, h)

	rr, err := client.Reserve(context.Background(),
		&pb.ReserveRequest{MessageId: uuid.NewString(), Owner: pbCustomerOwner(h), Credits: 50})
	if err != nil {
		t.Fatalf("Reserve (insufficient) returned a gRPC error, want a normal response: %v", err)
	}
	if rr.GetReserved() || rr.GetCode() != "insufficient_credit" {
		t.Errorf("Reserve resp = %+v, want reserved=false code=insufficient_credit", rr)
	}
}

// TestBillingGRPCInvalidArgument proves a malformed request is rejected with InvalidArgument carrying the
// flat validation code.
func TestBillingGRPCInvalidArgument(t *testing.T) {
	h := newBillingHarness(t, 100)
	client := newBillingGRPCClient(t, h)

	_, err := client.Reserve(context.Background(),
		&pb.ReserveRequest{MessageId: "not-a-uuid", Owner: pbCustomerOwner(h), Credits: 3})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Reserve(bad message_id) code = %v, want InvalidArgument", status.Code(err))
	}
	if status.Convert(err).Message() != "validation_error" {
		t.Errorf("status message = %q, want validation_error", status.Convert(err).Message())
	}

	// A non-positive credits is a client error (InvalidArgument), not a server-side Internal.
	_, err = client.Reserve(context.Background(),
		&pb.ReserveRequest{MessageId: uuid.NewString(), Owner: pbCustomerOwner(h), Credits: 0})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Reserve(credits=0) code = %v, want InvalidArgument", status.Code(err))
	}
}

func mtCredits(resp *pb.GetBalancesResponse) int32 {
	for _, b := range resp.GetBalances() {
		if b.GetDirection() == pb.Direction_DIRECTION_MT {
			return b.GetCredits()
		}
	}
	return 0
}
