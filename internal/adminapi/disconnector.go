package adminapi

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

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
