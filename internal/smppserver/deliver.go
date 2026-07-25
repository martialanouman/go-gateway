package smppserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// The Deliver outcomes the caller (step-048) branches on.
var (
	// ErrBindNotLive means the target bind is not on this pod (unbound, or moved). The caller
	// re-resolves the owning pod via Lookup and retries there.
	ErrBindNotLive = errors.New("smppserver: bind not live on this pod")
	// ErrBindCannotReceive means the bind is a transmitter, which never receives deliver_sm.
	ErrBindCannotReceive = errors.New("smppserver: bind cannot receive deliver_sm")
	// ErrNotDeliverSM means the PDU bytes did not decode to a deliver_sm.
	ErrNotDeliverSM = errors.New("smppserver: pdu is not a deliver_sm")
)

// Deliver pushes an already-encoded deliver_sm to the ESME on the named local bind and waits for its
// deliver_sm_resp. It decodes the opaque PDU, refuses a transmitter bind (which cannot receive), then
// hands the body to the session's send window (bounded, correlated). The message body never touches a
// log or span (invariant a): only the bind id and outcome are observable.
func (l *Listener) Deliver(ctx context.Context, bindID string, pdu []byte) error {
	decoded, err := smpp.Unmarshal(pdu)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNotDeliverSM, err)
	}
	body, ok := decoded.Body.(*smpp.DeliverSM)
	if !ok {
		return fmt.Errorf("%w: got command 0x%08x", ErrNotDeliverSM, uint32(decoded.CommandID()))
	}

	l.sessMu.Lock()
	live := l.sessions[bindID]
	l.sessMu.Unlock()
	if live == nil {
		return ErrBindNotLive
	}
	if live.mode != session.BindReceiver && live.mode != session.BindTransceiver {
		return ErrBindCannotReceive
	}

	if _, err := live.sess.Send(ctx, body); err != nil {
		return fmt.Errorf("smppserver: deliver to bind %s: %w", bindID, err)
	}
	return nil
}

// DeliverServer is the pod-local gRPC surface of SessionRegistry: it serves only Deliver, forwarding
// a deliver_sm to the local bind it targets. The other SessionRegistry RPCs are served by
// session-manager (the cross-pod registry); here they stay Unimplemented. step-048 dials a pod
// directly (after a Lookup) to reach this method.
type DeliverServer struct {
	pb.UnimplementedSessionRegistryServer
	listener *Listener
	logger   *slog.Logger
}

// NewDeliverServer builds the Deliver gRPC surface over the pod's live-session registry.
func NewDeliverServer(l *Listener, logger *slog.Logger) *DeliverServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &DeliverServer{listener: l, logger: logger}
}

// Deliver implements pb.SessionRegistryServer.Deliver. It maps the outcome to a gRPC status the caller
// can act on: NotFound (re-resolve the pod), FailedPrecondition (a transmitter — never retryable),
// InvalidArgument (malformed PDU), or Unavailable (the bind died mid-send — retryable elsewhere).
func (s *DeliverServer) Deliver(ctx context.Context, req *pb.DeliverRequest) (*pb.DeliverResponse, error) {
	err := s.listener.Deliver(ctx, req.GetBindId(), req.GetPdu())
	switch {
	case err == nil:
		return &pb.DeliverResponse{Delivered: true}, nil
	case errors.Is(err, ErrBindNotLive):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrBindCannotReceive):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrNotDeliverSM):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	default:
		// The bind died mid-send or timed out. Log the id and outcome only — never the body.
		s.logger.WarnContext(ctx, "smppserver: deliver failed", "bind_id", req.GetBindId(), "err", err)
		return nil, status.Error(codes.Unavailable, err.Error())
	}
}
