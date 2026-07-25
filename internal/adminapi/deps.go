package adminapi

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// CustomerStore is the persistence the customer handlers need. It is declared here, on the consumer
// side (guide-codage-go §6): the storage package must not know which handler calls it.
type CustomerStore interface {
	Create(ctx context.Context, in cp.NewCustomer) (cp.Customer, error)
	Get(ctx context.Context, id uuid.UUID) (cp.Customer, error)
	List(ctx context.Context, f cp.CustomerFilter) (cp.Page[cp.Customer], error)
	Update(ctx context.Context, id uuid.UUID, p cp.CustomerPatch) (cp.Customer, error)
	Delete(ctx context.Context, id uuid.UUID) error
	// Suspend sets the customer and every one of its accounts to suspended, in one transaction.
	Suspend(ctx context.Context, id uuid.UUID) (cp.Customer, error)
}

// AccountStore is the persistence the SMPP-account handlers need.
type AccountStore interface {
	Create(ctx context.Context, in cp.NewAccount) (cp.Account, error)
	Get(ctx context.Context, id uuid.UUID) (cp.Account, error)
	List(ctx context.Context, f cp.AccountFilter) (cp.Page[cp.Account], error)
	Update(ctx context.Context, id uuid.UUID, p cp.AccountPatch) (cp.Account, error)
	Delete(ctx context.Context, id uuid.UUID) error
	SetChannels(ctx context.Context, id uuid.UUID, smpp, rest bool) (cp.Account, error)
	SetSessionLimits(ctx context.Context, id uuid.UUID, maxSessions int, bind cp.BindType) (cp.Account, error)
	Suspend(ctx context.Context, id uuid.UUID) (cp.Account, error)
}

// CredentialStore is the persistence the credential handlers need. It never carries a secret: the
// hashes are written on create and rotate, and the returned Credential is always the masked view.
type CredentialStore interface {
	Create(ctx context.Context, in cp.NewCredential) (cp.Credential, error)
	ListByAccount(ctx context.Context, accountID uuid.UUID) ([]cp.Credential, error)
	Get(ctx context.Context, accountID, credID uuid.UUID) (cp.Credential, error)
	SetStatus(ctx context.Context, accountID, credID uuid.UUID, s cp.CredentialStatus) (cp.Credential, error)
	Rotate(ctx context.Context, accountID, credID uuid.UUID, rot cp.CredentialRotation) (cp.Credential, error)
}

// ConnectorStore is the persistence the connector handlers need. List returns a bare slice: the
// contract does not paginate connectors.
type ConnectorStore interface {
	Create(ctx context.Context, in cp.NewConnector) (cp.Connector, error)
	Get(ctx context.Context, id uuid.UUID) (cp.Connector, error)
	List(ctx context.Context) ([]cp.Connector, error)
	Update(ctx context.Context, id uuid.UUID, p cp.ConnectorPatch) (cp.Connector, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

// RouteStore is the persistence the route handlers need. Create and Update persist the route and
// its targets together. List returns a bare slice; the contract does not paginate routes.
type RouteStore interface {
	Create(ctx context.Context, in cp.NewRoute) (cp.Route, error)
	Get(ctx context.Context, id uuid.UUID) (cp.Route, error)
	List(ctx context.Context) ([]cp.Route, error)
	Update(ctx context.Context, id uuid.UUID, p cp.RoutePatch) (cp.Route, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

// SenderIDStore is the persistence the sender-ID handlers need. Sender IDs are nested under a
// customer, so update and delete are scoped by the customer id.
type SenderIDStore interface {
	Create(ctx context.Context, in cp.NewSenderID) (cp.SenderID, error)
	ListByCustomer(ctx context.Context, customerID uuid.UUID) ([]cp.SenderID, error)
	Update(ctx context.Context, customerID, senderID uuid.UUID, p cp.SenderIDPatch) (cp.SenderID, error)
	Delete(ctx context.Context, customerID, senderID uuid.UUID) error
}

// Disconnector force-closes the live SMPP sessions whose authorization has ceased (a revoked or
// disabled credential, a suspended account or customer), so a control-plane change reaches sessions
// already bound (step-032). It is best-effort: the control-plane mutation is authoritative and must
// not fail because the fan-out did, so a handler logs a Disconnect error and still returns success.
// session-manager's SessionRegistry.Disconnect satisfies it (see GRPCDisconnector). A nil Disconnector
// disables the fan-out (the contract test wires none).
type Disconnector interface {
	DisconnectAccount(ctx context.Context, accountID uuid.UUID, reason string) error
	DisconnectCustomer(ctx context.Context, customerID uuid.UUID, reason string) error
}

// Deps are the collaborators the Admin API needs. Later milestones add a store field per resource;
// New tolerates a nil store (the contract test builds the API without any), but a running server
// wires them all.
type Deps struct {
	Customers    CustomerStore
	Accounts     AccountStore
	Credentials  CredentialStore
	Connectors   ConnectorStore
	Routes       RouteStore
	SenderIDs    SenderIDStore
	Disconnector Disconnector
	Verifier     auth.TokenVerifier
	Logger       *slog.Logger
}
