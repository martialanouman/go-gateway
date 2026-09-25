package adminapi

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
)

// GRPCDisconnector adapts the SessionRegistry gRPC client to the Disconnector the Admin handlers use,
// mapping an account or customer scope to a DisconnectRequest. session-manager fans the order out to
// the owning pods (step-032).
type GRPCDisconnector struct {
	client registrypb.SessionRegistryClient
}

// NewGRPCDisconnector returns a Disconnector backed by the SessionRegistry client.
func NewGRPCDisconnector(client registrypb.SessionRegistryClient) *GRPCDisconnector {
	return &GRPCDisconnector{client: client}
}

// DisconnectAccount force-closes the live sessions of one account.
func (d *GRPCDisconnector) DisconnectAccount(ctx context.Context, accountID uuid.UUID, reason string) error {
	return d.disconnect(ctx, registrypb.DisconnectScope_DISCONNECT_SCOPE_ACCOUNT, accountID, reason)
}

// DisconnectCustomer force-closes the live sessions of every account of one customer.
func (d *GRPCDisconnector) DisconnectCustomer(ctx context.Context, customerID uuid.UUID, reason string) error {
	return d.disconnect(ctx, registrypb.DisconnectScope_DISCONNECT_SCOPE_CUSTOMER, customerID, reason)
}

func (d *GRPCDisconnector) disconnect(ctx context.Context, scope registrypb.DisconnectScope, id uuid.UUID, reason string) error {
	if _, err := d.client.Disconnect(ctx, &registrypb.DisconnectRequest{
		Scope:  scope,
		Id:     id.String(),
		Reason: reason,
	}); err != nil {
		return fmt.Errorf("session disconnect: %w", err)
	}
	return nil
}

// disconnectAccount and disconnectCustomer are the best-effort call sites the handlers use after a
// control-plane mutation: the mutation is authoritative, so a fan-out failure (or a nil Disconnector)
// is logged, never surfaced as a request error. Identifiers and the reason are logged, never a secret.
func disconnectAccount(ctx context.Context, disc Disconnector, logger *slog.Logger, accountID uuid.UUID, reason string) {
	if disc == nil {
		return
	}
	if err := disc.DisconnectAccount(ctx, accountID, reason); err != nil {
		logDisconnectFailure(ctx, logger, "account_id", accountID, reason, err)
	}
}

func disconnectCustomer(ctx context.Context, disc Disconnector, logger *slog.Logger, customerID uuid.UUID, reason string) {
	if disc == nil {
		return
	}
	if err := disc.DisconnectCustomer(ctx, customerID, reason); err != nil {
		logDisconnectFailure(ctx, logger, "customer_id", customerID, reason, err)
	}
}

func logDisconnectFailure(ctx context.Context, logger *slog.Logger, idKey string, id uuid.UUID, reason string, err error) {
	if logger == nil {
		return
	}
	logger.WarnContext(ctx, "admin: force-disconnect failed; control-plane change stands",
		idKey, id, "reason", reason, "err", err)
}

// GRPCSessions adapts the SessionRegistry gRPC client to the SessionDirectory the session handlers use.
type GRPCSessions struct {
	client registrypb.SessionRegistryClient
}

// NewGRPCSessions returns a SessionDirectory backed by the SessionRegistry client.
func NewGRPCSessions(client registrypb.SessionRegistryClient) *GRPCSessions {
	return &GRPCSessions{client: client}
}

// ListAccountSessions returns every live session of one account and the count its quota sees.
func (g *GRPCSessions) ListAccountSessions(ctx context.Context, accountID uuid.UUID) ([]LiveSession, int, error) {
	resp, err := g.client.ListSessions(ctx, &registrypb.ListSessionsRequest{AccountId: accountID.String()})
	if err != nil {
		return nil, 0, fmt.Errorf("list account sessions: %w", err)
	}
	sessions, err := fromPB(resp.GetSessions())
	return sessions, int(resp.GetActive()), err
}

// ListSessions returns the page of live sessions strictly after the bind id after.
func (g *GRPCSessions) ListSessions(ctx context.Context, after string, limit int) ([]LiveSession, string, error) {
	//nolint:gosec // G115: limit is bounded to 500 by the contract.
	resp, err := g.client.ListSessions(ctx, &registrypb.ListSessionsRequest{Cursor: after, Limit: int32(limit)})
	if err != nil {
		return nil, "", fmt.Errorf("list sessions: %w", err)
	}
	sessions, err := fromPB(resp.GetSessions())
	return sessions, resp.GetNextCursor(), err
}

// DisconnectSession orders the pod holding the bind to close it.
func (g *GRPCSessions) DisconnectSession(ctx context.Context, bindID, reason string) error {
	_, err := g.client.Disconnect(ctx, &registrypb.DisconnectRequest{
		Scope: registrypb.DisconnectScope_DISCONNECT_SCOPE_SESSION, Id: bindID, Reason: reason,
	})
	if status.Code(err) == codes.NotFound {
		return ErrSessionNotFound
	}
	if err != nil {
		return fmt.Errorf("disconnect session: %w", err)
	}
	return nil
}

var bindTypeNames = map[registrypb.BindType]string{
	registrypb.BindType_BIND_TYPE_TX: "tx", registrypb.BindType_BIND_TYPE_RX: "rx", registrypb.BindType_BIND_TYPE_TRX: "trx",
}

func fromPB(in []*registrypb.Session) ([]LiveSession, error) {
	out := make([]LiveSession, 0, len(in))
	for _, s := range in {
		account, err := uuid.Parse(s.GetAccountId())
		if err != nil {
			return nil, fmt.Errorf("session %s: account id: %w", s.GetBindId(), err)
		}
		out = append(out, LiveSession{
			AccountID: account, BindID: s.GetBindId(), SystemID: s.GetSystemId(), PodID: s.GetPodId(),
			BindType: bindTypeNames[s.GetBindType()], RemoteAddr: s.GetRemoteAddr(), WindowSize: int(s.GetWindowSize()),
			ConnectedAt: time.UnixMilli(s.GetConnectedAtUnixMs()).UTC(),
		})
	}
	return out, nil
}
