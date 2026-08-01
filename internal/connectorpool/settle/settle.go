// Package settle closes the MT billing loop in the connector pool (step-146): it CAPTURES the reserved
// credit when a message is sent and RELEASES it when a message terminally fails, through the billing gRPC
// service. It gates on the reservation flag pinned on mt.routed (billing disabled → ZERO billing call) and
// resolves the balance owner from the owner_type the router pinned, so a capture hits the identical key the
// reserve used (step-145). Every billing fault FAILS OPEN: the error is logged and counted, never returned —
// a propagated error would redeliver the record and re-submit the SMS (a duplicate), and the reserve debit
// already made the money correct, so a missed settlement is an audit gap the reaper reconciles, not a
// billing error. That reaper is billing.Reaper (step-190), a supervised sweep in billing-svc: it finds
// reservations left open here and settles each against the message's recorded CDR outcome. The message body never enters this package (invariant a).
package settle

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
)

// defaultSettleTimeout bounds a single capture/release RPC. It is short so a slow billing-svc degrades to a
// fast fail-open rather than stalling the send pipeline (a down billing-svc fails even faster — connection
// refused). billing-svc does a synchronous durable write, so the deadline must stay above its commit
// latency; it is configurable (BILLING_SETTLE_TIMEOUT) so ops can widen it without a redeploy.
const defaultSettleTimeout = 200 * time.Millisecond

// BillingClient is the slice of the billing gRPC client the settler uses. The generated pb.BillingClient
// satisfies it; declared consumer-side (convention §2) so a test can supply a counting fake.
type BillingClient interface {
	Capture(ctx context.Context, in *pb.CaptureRequest, opts ...grpc.CallOption) (*pb.CaptureResponse, error)
	Release(ctx context.Context, in *pb.ReleaseRequest, opts ...grpc.CallOption) (*pb.ReleaseResponse, error)
}

// Metric counts fail-open settle failures so an alert can fire — fail-open without an alarm is silent audit
// rot. The labels are bounded (none): a pod binds one connector, never a message id or MSISDN.
type Metric interface {
	CaptureFailed()
	ReleaseFailed()
}

type nopMetric struct{}

func (nopMetric) CaptureFailed() {}
func (nopMetric) ReleaseFailed() {}

// Settler captures/releases MT reservations through billing-svc. It holds no balance itself — billing-svc
// owns the atomic, idempotent accounting keyed by message_id.
type Settler struct {
	client  BillingClient
	timeout time.Duration
	metric  Metric
	logger  *slog.Logger
}

// Option configures a Settler.
type Option func(*Settler)

// WithTimeout overrides the per-call capture/release deadline (default 200ms).
func WithTimeout(d time.Duration) Option {
	return func(s *Settler) {
		if d > 0 {
			s.timeout = d
		}
	}
}

// WithMetric wires the fail-open failure counters.
func WithMetric(m Metric) Option {
	return func(s *Settler) {
		if m != nil {
			s.metric = m
		}
	}
}

// WithLogger sets the logger (defaults to slog.Default).
func WithLogger(l *slog.Logger) Option {
	return func(s *Settler) {
		if l != nil {
			s.logger = l
		}
	}
}

// NewSettler builds the settler over the billing client.
func NewSettler(client BillingClient, opts ...Option) *Settler {
	s := &Settler{client: client, timeout: defaultSettleTimeout, metric: nopMetric{}, logger: slog.Default()}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Capture confirms the reservation for a successfully-sent message. It gates on Billable: a message with no
// reservation (billing disabled) makes ZERO billing call and returns (false, nil). On success it returns
// (billed, &creditsCharged), where billed is creditsCharged > 0 — a capture that yielded to a winning release
// returns credits_charged=0, hence billed=false and &0. A billing fault FAILS OPEN: the reserve debit already
// charged the customer, so a missed capture is an audit gap billing.Reaper reconciles; it is logged and counted,
// NEVER returned as an error (a propagated error would redeliver → duplicate SMS). credits_charged=nil means
// "no settlement recorded" (disabled or fail-open) — distinct from &0 ("settled at zero"), so the CDR stays
// reconcilable.
func (s *Settler) Capture(ctx context.Context, r pipeline.RoutedMT) (billed bool, creditsCharged *int32) {
	if !r.Billable {
		return false, nil
	}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	resp, err := s.client.Capture(cctx, &pb.CaptureRequest{
		MessageId: r.MessageID.String(),
		Owner:     ownerFromType(r.OwnerType, r.CustomerID, r.AccountID),
	})
	if err != nil {
		s.metric.CaptureFailed()
		s.logger.WarnContext(ctx, "billing capture failed (fail-open); billing.Reaper will reconcile",
			"message_id", r.MessageID, "err", err)
		return false, nil
	}
	// credits_charged is the sole source of truth for billed: the server sets it to the amount actually moved
	// (0 when this capture yielded to a winning release). We deliberately do not read resp.Captured — it means
	// only "the RPC resolved", not "credits moved" — so billed is exactly "credits were charged".
	charged := resp.GetCreditsCharged()
	return charged > 0, &charged
}

// Release refunds the reservation for a message that terminally failed or was cancelled (never delivered).
// It gates on Billable (zero call when no reservation). A billing fault FAILS OPEN: failing to release leaves
// the customer over-charged until billing.Reaper reconciles; it is logged and counted, NEVER returned (a
// propagated error would redeliver and re-submit a known-bad message, burning SMSC TPS — and a "permanent"
// reject is not guaranteed deterministic on retry).
func (s *Settler) Release(ctx context.Context, r pipeline.RoutedMT) {
	if !r.Billable {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if _, err := s.client.Release(cctx, &pb.ReleaseRequest{
		MessageId: r.MessageID.String(),
		Owner:     ownerFromType(r.OwnerType, r.CustomerID, r.AccountID),
	}); err != nil {
		s.metric.ReleaseFailed()
		s.logger.WarnContext(ctx, "billing release failed (fail-open); billing.Reaper will reconcile",
			"message_id", r.MessageID, "err", err)
	}
}

// ownerFromType rebuilds the balance owner from the owner_type the router pinned onto mt.routed and the
// customer/account ids already on the record — the identical key the reserve used (step-145). account_id is
// always carried so the ledger attributes the charge to the originating SMPP account.
func ownerFromType(ownerType string, customerID, accountID uuid.UUID) *pb.Owner {
	if ownerType == cp.OwnerTypeSMPPAccount {
		return &pb.Owner{
			OwnerType:  pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT,
			OwnerId:    accountID.String(),
			CustomerId: customerID.String(),
			AccountId:  accountID.String(),
		}
	}
	return &pb.Owner{
		OwnerType:  pb.OwnerType_OWNER_TYPE_CUSTOMER,
		OwnerId:    customerID.String(),
		CustomerId: customerID.String(),
		AccountId:  accountID.String(),
	}
}
