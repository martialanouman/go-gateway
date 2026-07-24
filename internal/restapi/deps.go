// Package restapi is the public REST surface (rest-api-svc): submit-messages, get-message,
// list-messages, get-account and the public health check, wired chi + huma to conform to
// api/openapi-public.yaml. submit-messages honors the Idempotency-Key header (step-031); the SMPP
// cancel surface stays protocol-only (ADR-0009).
package restapi

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/idempotency"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"

	"github.com/google/uuid"
)

// PrincipalStore resolves a presented API key hash to its principal. *postgres.APIKeyRepo satisfies
// it.
type PrincipalStore interface {
	PrincipalByAPIKeyHash(ctx context.Context, hash string) (cp.APIKeyPrincipal, bool, error)
}

// CDRReader reads messages from the CDR: a single message's current status for get-message, and a
// cursor-paginated page for list-messages. Both are scoped to (customer_id, account_id).
// *clickhouse.CDRReader satisfies it.
type CDRReader interface {
	Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (clickhouse.CDRRow, bool, error)
	List(ctx context.Context, customerID, accountID uuid.UUID, filter clickhouse.CDRListFilter, limit int) ([]clickhouse.CDRRow, error)
}

// AccountReader reads the caller's own SMPP account for get-account. *postgres.AccountRepo satisfies
// it.
type AccountReader interface {
	Get(ctx context.Context, id uuid.UUID) (cp.Account, error)
}

// SenderIDReader lists a customer's sender IDs for get-account. *postgres.SenderIDRepo satisfies it.
type SenderIDReader interface {
	ListByCustomer(ctx context.Context, customerID uuid.UUID) ([]cp.SenderID, error)
}

// RateLimitReader reads the account's throughput limit for get-account. The bool is false when no
// limit row is configured. *postgres.RateLimitRepo satisfies it.
type RateLimitReader interface {
	RateLimit(ctx context.Context, accountID uuid.UUID) (cp.RateLimit, bool, error)
}

// IdempotencyStore backs the Idempotency-Key window on submit-messages: it reserves a slot atomically,
// finalizes it once the message is durably published, releases it on a publish failure, and lets a
// concurrent submit await the winner's result. *idempotency.Store satisfies it. A nil store disables
// idempotency — the header is then ignored, which is how the surface behaved before step-031.
type IdempotencyStore interface {
	Reserve(ctx context.Context, accountID, idemKey, bodyHash string, response []byte) (idempotency.Result, error)
	Finalize(ctx context.Context, accountID, idemKey string) error
	Release(ctx context.Context, accountID, idemKey string) error
	Await(ctx context.Context, accountID, idemKey string, timeout time.Duration) ([]byte, error)
}

// Deps are the REST service's collaborators. A nil Principals disables the auth middleware, which is
// how the contract test builds the API purely to read its spec.
type Deps struct {
	Principals PrincipalStore
	// Ingestor runs the shared MT ingestion sequence (encode → durable produce → accepted CDR row).
	// It is the same helper the SMPP submit_sm path uses, so both surfaces reach the pipeline
	// identically.
	Ingestor  *ingest.Ingestor
	CDRReader CDRReader
	// Accounts, SenderIDs and RateLimits back the read-only get-account projection.
	Accounts   AccountReader
	SenderIDs  SenderIDReader
	RateLimits RateLimitReader
	// Idempotency backs the Idempotency-Key header on submit-messages. Nil disables it.
	Idempotency IdempotencyStore
	Tracer      trace.Tracer
	Logger      *slog.Logger
	// Now supplies the accept/submit timestamp; defaults to time.Now. Injectable for tests.
	Now func() time.Time
	// Version is reported by the health endpoint.
	Version string
}
