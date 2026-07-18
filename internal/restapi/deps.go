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
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"

	"github.com/google/uuid"
)

// PrincipalStore resolves a presented API key hash to its principal. *postgres.APIKeyRepo satisfies
// it.
type PrincipalStore interface {
	PrincipalByAPIKeyHash(ctx context.Context, hash string) (cp.APIKeyPrincipal, bool, error)
}

// Producer publishes to mt.inbound. *kafka.Producer satisfies it. The produce is the durability
// boundary that earns the 202.
type Producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// CDRReader reads a message's current status for get-message. *clickhouse.CDRReader satisfies it.
type CDRReader interface {
	Current(ctx context.Context, customerID, accountID, messageID uuid.UUID) (clickhouse.CDRRow, bool, error)
}

// Deps are the REST service's collaborators. A nil Principals disables the auth middleware, which is
// how the contract test builds the API purely to read its spec.
type Deps struct {
	Principals PrincipalStore
	Producer   Producer
	CDRReader  CDRReader
	// Accepted is the bounded worker pool that writes the accepted CDR row off the request path. The
	// cmd runs it as a supervised goroutine; handlers only Enqueue.
	Accepted *AcceptedWriter
	Tracer   trace.Tracer
	Logger   *slog.Logger
	// Now supplies the accept/submit timestamp; defaults to time.Now. Injectable for tests.
	Now func() time.Time
	// Version is reported by the health endpoint.
	Version string
}
