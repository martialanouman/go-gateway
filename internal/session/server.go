package session

import (
	"context"
	"errors"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/session/pb"
)

// errorDomain namespaces the ErrorInfo carried on a rejection, per the google.rpc.ErrorInfo
// convention. The Reason is the gateway's shared Code; the caller (step-024) reads it to retranslate
// a rejection into an SMPP command_status without parsing a human message.
const errorDomain = "session-manager-svc"

// Server adapts the Redis-backed Registry to the SessionRegistry gRPC contract (api/proto/session.proto).
// It is a pure translator: it maps the wire messages to Registry calls and back, and turns the
// registry's sentinel errors into gRPC statuses that carry the shared error Code. It holds no state of
// its own beyond the registry, so a single instance serves every connection.
type Server struct {
	pb.UnimplementedSessionRegistryServer

	reg *Registry
}

// NewServer returns a SessionRegistry server backed by reg.
func NewServer(reg *Registry) *Server {
	return &Server{reg: reg}
}

// Bind registers the session carried by req against its account's max_sessions ceiling. The pod_id is
// supplied by the caller (the pod that owns the SMPP connection) through req.Session, so the registry
// never has to know which pod is calling. A bind beyond the ceiling is refused with a
// ResourceExhausted status carrying the max_sessions_exceeded code (invariant d, across the wire).
func (s *Server) Bind(ctx context.Context, req *pb.BindRequest) (*pb.BindResponse, error) {
	sess := req.GetSession()
	if sess == nil {
		return nil, status.Error(codes.InvalidArgument, "bind: session is required")
	}

	b := Bind{
		AccountID: sess.GetAccountId(),
		PodID:     sess.GetPodId(),
		BindID:    sess.GetBindId(),
	}
	active, err := s.reg.Bind(ctx, b, int(req.GetMaxSessions()))
	if err != nil {
		if errors.Is(err, errs.ErrMaxSessionsExceeded) {
			return nil, quotaExceeded(active)
		}
		return nil, status.Errorf(codes.Internal, "bind: %v", err)
	}
	//nolint:gosec // G115: active is bounded by max_sessions (a small, operator-set ceiling), never near int32.
	return &pb.BindResponse{Accepted: true, ActiveSessions: int32(active)}, nil
}

// Unbind removes the session named by req. The contract's UnbindRequest carries only account_id and
// bind_id, but the registry keys a session by pod_id:bind_id, so the owning pod_id is resolved first
// with a Lookup. This is a read-then-write rather than one atomic call, which is safe because unbind is
// idempotent: a session already gone (a concurrent unbind, a lapsed TTL) simply reports removed=false.
func (s *Server) Unbind(ctx context.Context, req *pb.UnbindRequest) (*pb.UnbindResponse, error) {
	binds, err := s.reg.Lookup(ctx, req.GetAccountId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unbind: lookup: %v", err)
	}

	for _, b := range binds {
		if b.BindID != req.GetBindId() {
			continue
		}
		removed, err := s.reg.Unbind(ctx, b)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "unbind: %v", err)
		}
		return &pb.UnbindResponse{Removed: removed}, nil
	}
	return &pb.UnbindResponse{Removed: false}, nil
}

// Lookup returns the account's live sessions, used to route return traffic to the pod owning a bind.
// The Redis registry persists only account/pod/bind, so the returned Sessions carry those three
// fields; system_id and bind_type are left unset (the registry does not store them, and pod_id +
// bind_id are all that return-routing needs).
func (s *Server) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	binds, err := s.reg.Lookup(ctx, req.GetAccountId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lookup: %v", err)
	}

	sessions := make([]*pb.Session, 0, len(binds))
	for _, b := range binds {
		sessions = append(sessions, &pb.Session{
			AccountId: b.AccountID,
			PodId:     b.PodID,
			BindId:    b.BindID,
		})
	}
	return &pb.LookupResponse{Sessions: sessions}, nil
}

// Deliver forwards an inbound deliver_sm to the pod owning the target bind. Real forwarding is the MO
// return path, which lands in step-046 (delivery) and step-048 (inter-pod routing): this service holds
// no SMPP connection to deliver to, and DeliverRequest carries no account_id to resolve the owning pod.
// Until then the method is explicitly Unimplemented rather than silently reporting a delivery that
// never happened.
func (s *Server) Deliver(_ context.Context, _ *pb.DeliverRequest) (*pb.DeliverResponse, error) {
	return nil, status.Error(codes.Unimplemented, "deliver: MO forwarding lands in step-046/048")
}

// quotaExceeded builds the ResourceExhausted status for a bind refused at the max_sessions ceiling. The
// shared code travels in a machine-readable google.rpc.ErrorInfo.Reason so the SMPP caller can map it
// to ESME_RBINDFAIL without parsing the human message. If attaching the detail fails (it never should),
// the bare status still carries the correct code and message.
func quotaExceeded(active int) error {
	st := status.New(codes.ResourceExhausted, errs.ErrMaxSessionsExceeded.String())
	detail := &errdetails.ErrorInfo{
		Reason:   errs.ErrMaxSessionsExceeded.String(),
		Domain:   errorDomain,
		Metadata: map[string]string{"active_sessions": strconv.Itoa(active)},
	}
	withDetail, err := st.WithDetails(detail)
	if err != nil {
		return st.Err()
	}
	return withDetail.Err()
}
