package credit

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// defaultReserveTimeout bounds a single Reserve RPC. It is short on purpose: a hung billing-svc must not
// stall the consumer past its session timeout (which would trigger a rebalance storm) — the deadline turns a
// hang into a retryable error the router redelivers, not an indefinite block on the hot path.
const defaultReserveTimeout = 200 * time.Millisecond

// BillingClient is the slice of the billing gRPC client the credit stage uses. The generated pb.BillingClient
// satisfies it; declared consumer-side (convention §2) so a test can supply a counting fake.
type BillingClient interface {
	Reserve(ctx context.Context, in *pb.ReserveRequest, opts ...grpc.CallOption) (*pb.ReserveResponse, error)
}

// Reserver is the credit stage: it gates on the cached snapshot and, for a billing-enabled customer, reserves
// credit through billing-svc. It holds no balance itself — billing-svc owns the atomic accounting.
type Reserver struct {
	holder  *Holder
	client  BillingClient
	timeout time.Duration
}

// Option configures a Reserver.
type Option func(*Reserver)

// WithTimeout overrides the per-call Reserve deadline (default 200ms).
func WithTimeout(d time.Duration) Option {
	return func(r *Reserver) { r.timeout = d }
}

// NewReserver builds the credit stage over a snapshot holder and the billing client.
func NewReserver(holder *Holder, client BillingClient, opts ...Option) *Reserver {
	r := &Reserver{holder: holder, client: client, timeout: defaultReserveTimeout}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Reserve holds credit for a message. It gates on the cached snapshot: a customer with billing disabled
// (absent from the snapshot) returns reserved=false WITHOUT any network call (§6.9). Otherwise it resolves
// the balance owner from the customer's scope and calls billing Reserve under a short deadline. On success it
// returns reserved=true and the ownerType the reservation was made against (customer | smpp_account), which
// the caller pins onto mt.routed so connector-pool captures the identical key (step-146). A business denial
// (reserved=false on the RPC) is returned as errs.ErrInsufficientCredit — the coded reject the router turns
// into a rejected CDR / HTTP 402; a transport fault is returned raw (no platform code) so the router treats
// it as transient and retries — fail-closed: a billed message is not sent until billing answers. The reserve
// is idempotent by message_id, so a redelivery after a deadline that actually committed heals without double-charging.
func (r *Reserver) Reserve(ctx context.Context, accountID, customerID, messageID uuid.UUID, segments int) (reserved bool, ownerType string, err error) {
	scope, enabled := r.holder.Lookup(customerID)
	if !enabled {
		return false, "", nil
	}

	owner, ownerType := ownerFor(scope, accountID, customerID)

	cctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	resp, err := r.client.Reserve(cctx, &pb.ReserveRequest{
		MessageId: messageID.String(),
		Owner:     owner,
		Credits:   i32(segments),
	})
	if err != nil {
		// Transport fault (billing-svc unreachable, deadline): raw error, no platform code → the router
		// retries. Never send a billed message on an unconfirmed reserve.
		return false, "", err
	}
	if !resp.GetReserved() {
		// Any denial (insufficient funds) is a terminal, coded reject — never a transient retry.
		return false, "", errs.ErrInsufficientCredit
	}
	return true, ownerType, nil
}

// ownerFor resolves the reserve owner from the customer's balance scope. account_id is always carried so the
// ledger attributes the charge to the originating SMPP account even when the balance is customer-scoped
// (§6.9). It returns the pb owner for the RPC and the domain owner_type string pinned onto mt.routed.
func ownerFor(scope cp.BalanceScope, accountID, customerID uuid.UUID) (*pb.Owner, string) {
	if scope == cp.BalanceScopeSMPPAccount {
		return &pb.Owner{
			OwnerType:  pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT,
			OwnerId:    accountID.String(),
			CustomerId: customerID.String(),
			AccountId:  accountID.String(),
		}, cp.OwnerTypeSMPPAccount
	}
	return &pb.Owner{
		OwnerType:  pb.OwnerType_OWNER_TYPE_CUSTOMER,
		OwnerId:    customerID.String(),
		CustomerId: customerID.String(),
		AccountId:  accountID.String(),
	}, cp.OwnerTypeCustomer
}

// i32 narrows a segment count to the wire credit type. A segment count is a small integer, never near the
// int32 bound.
//
//nolint:gosec // G115: segment counts are small integers, never near the int32 bound.
func i32(v int) int32 { return int32(v) }
