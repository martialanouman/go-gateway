// Package restapi is the public REST surface (rest-api-svc): submit-messages, get-message and the
// public health check, wired chi + huma to conform to api/openapi-public.yaml. M2 serves the single
// submit, the status read and health; the list/cancel/account operations and the Idempotency-Key
// header arrive at M3.
package restapi

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"

	"github.com/google/uuid"
)

// PrincipalStore resolves a presented API key hash to its principal. *postgres.APIKeyRepo satisfies
// it.
type PrincipalStore interface {
	PrincipalByAPIKeyHash(ctx context.Context, hash string) (cp.APIKeyPrincipal, bool, error)
}

// CDRReader reads a message's current status for get-message. *clickhouse.CDRReader satisfies it.
type CDRReader interface {
	Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (clickhouse.CDRRow, bool, error)
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
	Tracer    trace.Tracer
	Logger    *slog.Logger
	// Now supplies the accept/submit timestamp; defaults to time.Now. Injectable for tests.
	Now func() time.Time
	// Version is reported by the health endpoint.
	Version string
}
