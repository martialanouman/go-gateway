package adminapi

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/connector/status"
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
	// RateLimit returns the connector's operational rate_limit (found=false when none), so an update
	// lowering throughput_limit_per_sec below it can be rejected (spec §6.4 NOTE).
	RateLimit(ctx context.Context, connectorID uuid.UUID) (cp.RateLimit, bool, error)
	// UpdateReconnectPolicy and UpdateBindPool are the dedicated partial updates the connector-piloting
	// endpoints use (step-128), so bind_pool_size and the reconnect knobs are not part of the general
	// ConnectorUpdate surface.
	UpdateReconnectPolicy(ctx context.Context, id uuid.UUID, p cp.ReconnectPolicy) (cp.Connector, error)
	UpdateBindPool(ctx context.Context, id uuid.UUID, size int) (cp.Connector, error)
}

// ConnectorControl reads a connector's live runtime status and signals a reconfigure (step-128). The
// Admin API runs in a separate process from the connector pool, so it drives the pool through Redis:
// Read assembles the per-bind link_status + breaker_state; SignalReconfigure bumps the generation the
// pool polls to re-dial (rebind / resize / policy change). *status.Reader satisfies it.
type ConnectorControl interface {
	Read(ctx context.Context, connectorID uuid.UUID) (status.Connector, error)
	SignalReconfigure(ctx context.Context, connectorID uuid.UUID) error
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

// InboundNumberStore is the persistence the inbound-number handlers need. List returns a bare slice
// (the contract does not paginate). There is no Get: the contract has no get-inbound-number. Assign
// dedicates the number to an account, or clears it (nil = shared) — a dedicated method because a nil
// account_id means "set NULL", not "leave unchanged".
type InboundNumberStore interface {
	Create(ctx context.Context, in cp.NewInboundNumber) (cp.InboundNumber, error)
	List(ctx context.Context) ([]cp.InboundNumber, error)
	Update(ctx context.Context, id uuid.UUID, p cp.InboundNumberPatch) (cp.InboundNumber, error)
	Delete(ctx context.Context, id uuid.UUID) error
	Assign(ctx context.Context, id uuid.UUID, accountID *uuid.UUID) (cp.InboundNumber, error)
}

// InboundKeywordStore is the persistence the inbound-keyword handlers need. Every operation is scoped
// by inbound_number_id (the path id): keywords are nested under a shared inbound number, so update and
// delete address a keyword only within its number. List returns a bare slice; the contract does not
// paginate keywords.
type InboundKeywordStore interface {
	Create(ctx context.Context, in cp.NewInboundKeyword) (cp.InboundKeyword, error)
	ListByInboundNumber(ctx context.Context, inboundNumberID uuid.UUID) ([]cp.InboundKeyword, error)
	Update(ctx context.Context, inboundNumberID, keywordID uuid.UUID, p cp.InboundKeywordPatch) (cp.InboundKeyword, error)
	Delete(ctx context.Context, inboundNumberID, keywordID uuid.UUID) error
}

// UnroutedMOStore is the read side of the unrouted-MO operator queue (list-unrouted-mo). It is
// keyset-paginated by (received_at, id): List returns up to limit rows newest-first, starting strictly
// after the cursor position (nil = the first page). *postgres.UnroutedMORepo satisfies it.
type UnroutedMOStore interface {
	List(ctx context.Context, limit int, after *cp.UnroutedMOKey) ([]cp.UnroutedMO, error)
	// DeleteByMSISDN removes the records carrying a phone number, for an RGPD erasure (step-166).
	DeleteByMSISDN(ctx context.Context, msisdn string) (int, error)
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
	// StreamHub fans out the realtime feeds (step-183). Nil refuses the stream endpoints at request time.
	StreamHub StreamHub
	// Quit closes live WebSocket handlers on shutdown; see the Quit type.
	Quit Quit
	// Trace reads a message lifecycle for get-message-trace (step-185).
	Trace TraceStore

	// MessageSearch reads the CDR for search-messages (step-186) and feeds the export worker.
	MessageSearch SearchStore

	// ExportJobs and ExportSink back the asynchronous export (step-187). A nil sink means the
	// deployment has no export storage: create-message-export then answers 503 rather than queueing a
	// job nothing can fulfil.
	ExportJobs ExportJobStore
	ExportSink ExportSink

	Customers        CustomerStore
	Accounts         AccountStore
	Credentials      CredentialStore
	Connectors       ConnectorStore
	ConnectorControl ConnectorControl
	Routes           RouteStore
	SenderIDs        SenderIDStore
	InboundNumbers   InboundNumberStore
	InboundKeywords  InboundKeywordStore
	UnroutedMO       UnroutedMOStore
	Suppressions     SuppressionAdminStore
	OptOutKeywords   OptOutKeywordStore
	AntispamRules    AntispamRuleStore
	ExactRoutes      ExactRouteAdminStore
	ExactRouteCache  ExactRouteCacheInvalidator
	// ConfigChanges and ConfigChannel let a mutation whose durable write lands AFTER its HTTP response
	// — today, the background bulk import of exact routes (step-250e) — publish its own config-change
	// announcement once committed. Synchronous handlers are covered by the PublishConfigChanges
	// middleware and must not use them: the middleware announces when the HANDLER returns, which for
	// an import is the 202 while BulkUpsert is still running, so the fleet would rebuild its Bloom
	// from a table that does not hold the rows yet and nothing would republish after the commit.
	// Small imports won that race and large ones lost it, the inverse of the use case. Best-effort,
	// like the middleware: a lost announcement only defers the rebuild to the next admin mutation.
	ConfigChanges    ConfigChangePublisher
	ConfigChannel    string
	RoutingScripts   RoutingScriptAdminStore
	Imports          ImportRunner
	Disconnector     Disconnector
	Billing          BillingStore
	BalanceCache     BalanceCacheInvalidator
	RatePlans        RatePlanStore
	BillingProviders BillingProviderStore
	ContentKeys      ContentKeyRotator
	ContentKeyReader ContentKeyReader
	ContentKeyEraser ContentKeyEraser
	Messages         MessageContentReader
	ContentAudit     ContentAuditor
	GDPRJobs         GDPRJobStore
	CDREraser        CDREraser
	// GDPRRunner runs erasure jobs. It is SEPARATE from Imports on purpose: a legally-mandated erasure must
	// not be refused because bulk MNP imports filled the shared pool.
	GDPRRunner ImportRunner
	Verifier   auth.TokenVerifier
	Logger     *slog.Logger
}

// BillingStore is the persistence the admin billing handlers need (step-148): the reserve-floor config view,
// balance reads, the durable top-up write, the atomic transfer, and the guarded balance-scope flip. All
// writes are durable (Postgres ledger); the Redis balance cache is invalidated separately (BalanceCacheInvalidator).
// *postgres.BillingRepo satisfies it; declared consumer-side.
type BillingStore interface {
	Balances(ctx context.Context, owners []cp.BalanceOwner) ([]cp.BalanceRow, error)
	Topup(ctx context.Context, entry cp.LedgerEntry) (row cp.LedgerRow, applied bool, err error)
	Transfer(ctx context.Context, debit, credit cp.LedgerEntry, idemKey uuid.UUID) (rows []cp.LedgerRow, applied bool, err error)
	ChangeBalanceScope(ctx context.Context, customerID uuid.UUID, currentOwners []cp.BalanceOwner, newScope string) error
	// Ledger returns one keyset page of a customer's ledger plus whether a further page exists (step-149).
	Ledger(ctx context.Context, f cp.LedgerFilter) (rows []cp.LedgerRow, hasMore bool, err error)
}

// RatePlanStore is the persistence the admin rate-plan handlers need (§3.1, step-149). *postgres.RatePlanRepo
// satisfies it; declared consumer-side.
type RatePlanStore interface {
	List(ctx context.Context) ([]cp.RatePlan, error)
	Create(ctx context.Context, in cp.NewRatePlan) (cp.RatePlan, error)
	Update(ctx context.Context, id uuid.UUID, p cp.RatePlanPatch) (cp.RatePlan, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

// BillingProviderStore is the persistence the admin external-provider handlers need (§6.10, step-149).
// *postgres.ExternalBillingProviderRepo satisfies it; declared consumer-side.
type BillingProviderStore interface {
	List(ctx context.Context) ([]cp.ExternalBillingProvider, error)
	Get(ctx context.Context, id uuid.UUID) (cp.ExternalBillingProvider, error)
	Create(ctx context.Context, in cp.NewExternalBillingProvider) (cp.ExternalBillingProvider, error)
	Update(ctx context.Context, id uuid.UUID, p cp.ExternalBillingProviderPatch) (cp.ExternalBillingProvider, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

// ExactRouteCacheInvalidator drops the cached exact routes of the numbers an admin mutation just changed
// durably, so the next Bloom possible-hit repopulates exactroute:{msisdn} from Postgres (step-250e). It is
// the control plane's ONLY write to that cache: it never says what the new target is, which is what keeps
// the key rebuildable rather than a second source of truth. Best-effort — a failure is logged, not fatal,
// because the commit already happened and the resolver's TTL bounds the staleness. *exact.Invalidator
// satisfies it; registerExactRoutes defaults a nil one to a no-op. Declared consumer-side.
type ExactRouteCacheInvalidator interface {
	Invalidate(ctx context.Context, msisdns ...string) error
}

// BalanceCacheInvalidator deletes the Redis balance-cache keys of the owners an admin money op just changed
// durably, so the next reserve rehydrates the fresh Postgres balance instead of serving a stale cached one
// (step-148). Best-effort: a delete failure is logged, not fatal (the cache TTL still self-heals). A go-redis
// adapter satisfies it; New defaults a nil one to a no-op. Declared consumer-side.
type BalanceCacheInvalidator interface {
	Del(ctx context.Context, keys ...string) error
}
