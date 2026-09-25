package session

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	"github.com/martialanouman/go-gateway/internal/session/pb"
)

// errorDomain namespaces the ErrorInfo carried on a rejection, per the google.rpc.ErrorInfo
// convention. The Reason is the gateway's shared Code; the caller (step-024) reads it to retranslate
// a rejection into an SMPP command_status without parsing a human message.
const errorDomain = "session-manager-svc"

// Publisher fans a force-disconnect order out to the smpp-server pods. It is the Redis pub/sub
// PUBLISH abstracted behind a payload-oriented method so the Disconnect path is testable without
// Redis. *redisstore.PubSubPublisher satisfies it in production.
type Publisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

// Server adapts the Redis-backed Registry to the SessionRegistry gRPC contract (api/proto/session.proto).
// It is a pure translator: it maps the wire messages to Registry calls and back, and turns the
// registry's sentinel errors into gRPC statuses that carry the shared error Code. It holds no state of
// its own beyond the registry and the disconnect publisher, so a single instance serves every
// connection.
type Server struct {
	pb.UnimplementedSessionRegistryServer

	reg *Registry
	pub Publisher
}

// NewServer returns a SessionRegistry server backed by reg for the bind registry and pub for the
// force-disconnect fan-out.
func NewServer(reg *Registry, pub Publisher) *Server {
	return &Server{reg: reg, pub: pub}
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
		Addr:      sess.GetPodAddr(),

		SystemID:   sess.GetSystemId(),
		BindType:   BindTypeName(sess.GetBindType()),
		RemoteAddr: sess.GetRemoteAddr(),
		WindowSize: int(sess.GetWindowSize()),
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
	if err := s.reg.Unindex(ctx, req.GetBindId()); err != nil {
		return nil, status.Errorf(codes.Internal, "unbind: %v", err)
	}
	return &pb.UnbindResponse{Removed: false}, nil
}

// Lookup returns the account's live sessions, used to route return traffic to the pod owning a bind.
// The Redis registry persists account/pod/bind plus the pod's published address, so the returned
// Sessions carry those four fields; system_id and bind_type are left unset (the registry does not
// store them, and the other four are all that return-routing needs — pod_addr being the one it
// actually dials, step-302).
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
			PodAddr:   b.Addr,
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

// Disconnect fans a force-close order out to every owning pod by publishing a disconnect.Event on
// the shared Redis channel; the pods do the actual socket close asynchronously (step-032). The
// registry itself is never mutated here — an unbind on the closed socket frees the max_sessions slot
// on the pod's own path. Publishing is idempotent: a repeated order simply targets sessions that are
// already gone. The scope must be a concrete account, customer or session; an unspecified scope or empty id
// is rejected rather than fanned out to an over-broad set of sessions.
// A session scope is resolved first: an order for a bind that is not live answers NotFound, never a
// silent success (step-360).
func (s *Server) Disconnect(ctx context.Context, req *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
	scope, err := disconnectScope(req.GetScope())
	if err != nil {
		return nil, err
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "disconnect: id is required")
	}
	if scope == disconnect.ScopeSession {
		found, err := s.reg.Resolve(ctx, req.GetId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "disconnect: resolve: %v", err)
		}
		if !found {
			return nil, status.Errorf(codes.NotFound, "disconnect: no live session %s", req.GetId())
		}
	}

	// Reason is a machine label, never a secret; it is safe to carry and later log (§1.9).
	payload := disconnect.Encode(disconnect.Event{Scope: scope, ID: req.GetId(), Reason: req.GetReason()})
	if err := s.pub.Publish(ctx, disconnect.Channel, payload); err != nil {
		return nil, status.Errorf(codes.Internal, "disconnect: publish: %v", err)
	}
	return &pb.DisconnectResponse{Published: true}, nil
}

// ListSessions serves the operator's reads (step-360): every live session of one account with the count
// its quota sees, or a page of all live sessions.
func (s *Server) ListSessions(ctx context.Context, req *pb.ListSessionsRequest) (*pb.ListSessionsResponse, error) {
	if req.GetAccountId() != "" {
		sessions, active, err := s.reg.ListAccount(ctx, req.GetAccountId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list sessions: %v", err)
		}
		//nolint:gosec // G115: active is bounded by max_sessions, a small operator-set ceiling.
		return &pb.ListSessionsResponse{Sessions: toPB(sessions), Active: int32(active)}, nil
	}
	if req.GetLimit() < 1 || req.GetLimit() > maxListLimit {
		return nil, status.Errorf(codes.InvalidArgument, "list sessions: limit must be within 1..%d", maxListLimit)
	}
	sessions, next, err := s.reg.List(ctx, req.GetCursor(), int(req.GetLimit()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list sessions: %v", err)
	}
	return &pb.ListSessionsResponse{Sessions: toPB(sessions), NextCursor: next}, nil
}

func toPB(sessions []Session) []*pb.Session {
	out := make([]*pb.Session, len(sessions))
	for i, s := range sessions {
		out[i] = &pb.Session{
			AccountId: s.AccountID, SystemId: s.SystemID, PodId: s.PodID, BindId: s.BindID,
			BindType: pb.BindType(pb.BindType_value["BIND_TYPE_"+strings.ToUpper(s.BindType)]), RemoteAddr: s.RemoteAddr,
			//nolint:gosec // G115: a window is a small per-session setting.
			WindowSize: int32(s.WindowSize), ConnectedAtUnixMs: s.ConnectedAt.UnixMilli(),
		}
	}
	return out
}

// maxListLimit is the Admin contract's page ceiling, held here too so a direct gRPC caller cannot pipeline
// the whole index in one call.
const maxListLimit = 500

// BindTypeName is the contract's name for a bind type (tx, rx, trx), derived from the generated enum so
// no hand-kept table can drift from it.
func BindTypeName(t pb.BindType) string {
	return strings.ToLower(strings.TrimPrefix(t.String(), "BIND_TYPE_"))
}

// disconnectScope maps the wire scope to the domain scope, rejecting the unspecified (zero) value so
// a malformed request can never fan out to every session.
func disconnectScope(s pb.DisconnectScope) (disconnect.Scope, error) {
	switch s {
	case pb.DisconnectScope_DISCONNECT_SCOPE_ACCOUNT:
		return disconnect.ScopeAccount, nil
	case pb.DisconnectScope_DISCONNECT_SCOPE_CUSTOMER:
		return disconnect.ScopeCustomer, nil
	case pb.DisconnectScope_DISCONNECT_SCOPE_SESSION:
		return disconnect.ScopeSession, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "disconnect: scope must be account, customer or session, got %v", s)
	}
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
