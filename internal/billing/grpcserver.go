package billing

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// BalanceReader reads durable balances for GetBalances and for the balance_after fields of
// capture/release (which do not change the balance). *postgres.BillingRepo satisfies it, declared
// consumer-side (convention §2).
type BalanceReader interface {
	Balance(ctx context.Context, ownerType string, ownerID uuid.UUID, direction string) (int, bool, error)
}

// Core is the billing surface the gRPC server drives: reserve/capture/release plus the MO meter. Both the raw
// *Accountant and the *ExternalBiller decorator (§6.10, step-147) satisfy it, so the server is oblivious to
// whether an external provider is in play. Declared consumer-side (convention §2).
type Core interface {
	Reserve(ctx context.Context, owner Owner, messageID uuid.UUID, credits int) (balanceAfter int, err error)
	Capture(ctx context.Context, owner Owner, messageID uuid.UUID) (creditsCharged int, err error)
	Release(ctx context.Context, owner Owner, messageID uuid.UUID) error
	RecordMO(ctx context.Context, owner Owner, messageID uuid.UUID, credits int) (MOResult, error)
}

// Server implements pb.BillingServer over the billing Core (step-142/143/147). It maps the flat wire messages
// to domain calls and the flat error code model onto gRPC status. The message body never reaches billing
// (invariant a): it sees only owner identifiers, a message_id and integer credits.
type Server struct {
	pb.UnimplementedBillingServer
	core     Core
	balances BalanceReader
}

// NewServer builds the Billing gRPC server over the billing core (the Accountant, or the ExternalBiller
// decorator wrapping it) and a durable balance reader.
func NewServer(core Core, balances BalanceReader) *Server {
	return &Server{core: core, balances: balances}
}

// Reserve holds credits for a message before the SMSC send. Insufficient credit is a normal, non-error
// outcome (reserved=false + machine code), not a gRPC failure.
func (s *Server) Reserve(ctx context.Context, req *pb.ReserveRequest) (*pb.ReserveResponse, error) {
	owner, messageID, err := ownerAndMessage(req.GetOwner(), req.GetMessageId())
	if err != nil {
		return nil, err
	}
	if req.GetCredits() <= 0 {
		return nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	bal, err := s.core.Reserve(ctx, owner, messageID, int(req.GetCredits()))
	if errors.Is(err, errs.ErrInsufficientCredit) {
		return &pb.ReserveResponse{Reserved: false, BalanceAfter: i32(bal), Code: string(errs.ErrInsufficientCredit)}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.ReserveResponse{Reserved: true, BalanceAfter: i32(bal)}, nil
}

// Capture commits a reservation; the balance is unchanged (the reserve already debited it), so
// balance_after is the current MT balance and credits_charged is the amount of record. captured=true means
// the RPC resolved, not that credits moved: on a capture that yields to a winning release the amount of
// record is credits_charged=0 — the caller (connector-pool CDR, step-146) keys off credits_charged.
func (s *Server) Capture(ctx context.Context, req *pb.CaptureRequest) (*pb.CaptureResponse, error) {
	owner, messageID, err := ownerAndMessage(req.GetOwner(), req.GetMessageId())
	if err != nil {
		return nil, err
	}
	charged, err := s.core.Capture(ctx, owner, messageID)
	if err != nil {
		return nil, toStatus(err)
	}
	bal, err := s.mtBalance(ctx, owner)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.CaptureResponse{Captured: true, BalanceAfter: i32(bal), CreditsCharged: i32(charged)}, nil
}

// Release refunds a reservation for a message that failed before it was sent.
func (s *Server) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	owner, messageID, err := ownerAndMessage(req.GetOwner(), req.GetMessageId())
	if err != nil {
		return nil, err
	}
	if err := s.core.Release(ctx, owner, messageID); err != nil {
		return nil, toStatus(err)
	}
	bal, err := s.mtBalance(ctx, owner)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.ReleaseResponse{Released: true, BalanceAfter: i32(bal)}, nil
}

// GetBalances returns the owner's balances, one entry per direction that has a durable row.
func (s *Server) GetBalances(ctx context.Context, req *pb.GetBalancesRequest) (*pb.GetBalancesResponse, error) {
	owner, err := ownerFromPB(req.GetOwner())
	if err != nil {
		return nil, err
	}
	out := make([]*pb.Balance, 0, 2)
	for _, d := range []struct {
		dir string
		pb  pb.Direction
	}{{cp.BillingDirectionMT, pb.Direction_DIRECTION_MT}, {cp.BillingDirectionMO, pb.Direction_DIRECTION_MO}} {
		credits, found, err := s.balances.Balance(ctx, owner.Type, owner.ID, d.dir)
		if err != nil {
			return nil, toStatus(err)
		}
		if found {
			out = append(out, &pb.Balance{Direction: d.pb, Credits: i32(credits)})
		}
	}
	return &pb.GetBalancesResponse{Balances: out}, nil
}

// RecordMO accrues a received message on the separate MO meter. It never blocks: a reached floor is
// reported (floored=true), not enforced.
func (s *Server) RecordMO(ctx context.Context, req *pb.RecordMORequest) (*pb.RecordMOResponse, error) {
	owner, messageID, err := ownerAndMessage(req.GetOwner(), req.GetMessageId())
	if err != nil {
		return nil, err
	}
	if req.GetCredits() <= 0 {
		return nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	r, err := s.core.RecordMO(ctx, owner, messageID, int(req.GetCredits()))
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.RecordMOResponse{BalanceAfter: i32(r.Balance), Floored: r.FloorReached || r.Suppressed}, nil
}

func (s *Server) mtBalance(ctx context.Context, owner Owner) (int, error) {
	bal, _, err := s.balances.Balance(ctx, owner.Type, owner.ID, cp.BillingDirectionMT)
	return bal, err
}

// ownerAndMessage parses the shared owner + message_id of the write RPCs, returning an InvalidArgument
// status on a malformed field.
func ownerAndMessage(o *pb.Owner, messageID string) (Owner, uuid.UUID, error) {
	owner, err := ownerFromPB(o)
	if err != nil {
		return Owner{}, uuid.Nil, err
	}
	mid, err := uuid.Parse(messageID)
	if err != nil {
		return Owner{}, uuid.Nil, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	return owner, mid, nil
}

func ownerFromPB(o *pb.Owner) (Owner, error) {
	if o == nil {
		return Owner{}, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	ownerType := ownerTypeString(o.GetOwnerType())
	if ownerType == "" {
		return Owner{}, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	ownerID, err := uuid.Parse(o.GetOwnerId())
	if err != nil {
		return Owner{}, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	customerID, err := uuid.Parse(o.GetCustomerId())
	if err != nil {
		return Owner{}, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
	}
	owner := Owner{Type: ownerType, ID: ownerID, CustomerID: customerID}
	if a := o.GetAccountId(); a != "" {
		accountID, err := uuid.Parse(a)
		if err != nil {
			return Owner{}, status.Error(codes.InvalidArgument, string(errs.ErrValidation))
		}
		owner.AccountID = &accountID
	}
	return owner, nil
}

func ownerTypeString(t pb.OwnerType) string {
	switch t {
	case pb.OwnerType_OWNER_TYPE_CUSTOMER:
		return cp.OwnerTypeCustomer
	case pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT:
		return cp.OwnerTypeSMPPAccount
	default:
		return ""
	}
}

// toStatus maps a domain error onto a gRPC status whose message is the flat wire code (§11.3) — the shared
// contract clients branch on. An unrecognised error is Internal, never leaking its text.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	code, ok := errs.CodeOf(err)
	if !ok {
		return status.Error(codes.Internal, string(errs.ErrInternal))
	}
	return status.Error(grpcCodeFor(code), string(code))
}

func grpcCodeFor(c errs.Code) codes.Code {
	switch c {
	case errs.ErrValidation:
		return codes.InvalidArgument
	case errs.ErrConflict:
		return codes.Aborted
	case errs.ErrNotFound:
		return codes.NotFound
	case errs.ErrInsufficientCredit:
		return codes.FailedPrecondition
	case errs.ErrExternalBillingUnavailable:
		// A provider outage under fail_closed is transient: gRPC Unavailable tells the router to retry
		// (the reserver returns any gRPC error raw → the pipeline treats it as a retryable fault), so a
		// billed message is held, never sent unconfirmed nor permanently rejected.
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

// i32 narrows an integer credit count to the wire type. Credit counts are bounded well within int32.
//
//nolint:gosec // G115: credit counts (segments/meter) are small integers, never near the int32 bound.
func i32(v int) int32 { return int32(v) }
