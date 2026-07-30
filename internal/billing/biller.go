package billing

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// defaultAuthorizeTimeout bounds a synchronous external authorize when the provider set no
// sync_call_timeout_ms, so the hot-path call is never unbounded even if the caller's context has no deadline.
// It is deliberately short (below a typical router reserve deadline) so the external call cannot dominate the
// budget. NOTE: sync_call_timeout_ms must be configured BELOW the router's Reserve gRPC deadline — a larger
// value is dead config (the client deadline cancels the whole RPC first); cross-service validation of that
// inversion is a follow-up.
const defaultAuthorizeTimeout = 150 * time.Millisecond

// ExternalMetric counts external-authorization fail-open events (§6.10), labelled by provider. A fail-open
// pass means a provider fault let a reserve through unconfirmed — a dead or misconfigured provider silently
// authorizing everything is a revenue trap, so every pass must be loudly counted for alerting. The provider
// id is a bounded label (few providers) — never a customer id, message id or MSISDN.
type ExternalMetric interface {
	AuthzFailOpen(providerID uuid.UUID)
}

type nopExternalMetric struct{}

func (nopExternalMetric) AuthzFailOpen(uuid.UUID) {}

// ExternalBiller decorates a billing Core with external-provider authorization (§6.10). It runs the external
// gate BEFORE the internal reserve, so an external denial costs zero Lua/ledger. The internal reserve ALWAYS
// runs afterwards (external is an additional gate, never a bypass — the internal ledger is still authoritative
// for capture/release and reconciliation). capture/release/record-mo delegate straight through: consumption is
// reconciled off the critical path (step-147 Reconciler), never confirmed per-capture. It satisfies Core, so
// the gRPC server is oblivious to whether a provider is in play.
type ExternalBiller struct {
	inner    Core
	config   ConfigSource
	provider ExternalProvider
	metric   ExternalMetric
	logger   *slog.Logger
}

// ExternalOption configures an ExternalBiller.
type ExternalOption func(*ExternalBiller)

// WithExternalMetric wires the fail-open counter.
func WithExternalMetric(m ExternalMetric) ExternalOption {
	return func(b *ExternalBiller) {
		if m != nil {
			b.metric = m
		}
	}
}

// WithExternalLogger sets the logger (defaults to slog.Default).
func WithExternalLogger(l *slog.Logger) ExternalOption {
	return func(b *ExternalBiller) {
		if l != nil {
			b.logger = l
		}
	}
}

// NewExternalBiller wraps inner with external-provider authorization driven by config (per-customer mode) and
// provider (the pluggable external system).
func NewExternalBiller(inner Core, config ConfigSource, provider ExternalProvider, opts ...ExternalOption) *ExternalBiller {
	b := &ExternalBiller{inner: inner, config: config, provider: provider, metric: nopExternalMetric{}, logger: slog.Default()}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Reserve applies the external gate for the customer's mode, then always runs the internal reserve. A
// synchronous mode (balance_check, consume_delegate_sync, both) authorizes against the provider first: an
// external denial → insufficient_credit (no internal reserve); a provider fault → failure_policy. An async
// mode reserves locally and reconciles later, so it skips the synchronous call. A customer with no active
// provider delegates straight through.
func (b *ExternalBiller) Reserve(ctx context.Context, owner Owner, messageID uuid.UUID, credits int) (int, error) {
	if cfg, ok := b.config.ExternalFor(owner.CustomerID); ok && cfg.Mode.SyncAuthorize() {
		allowed, err := b.authorize(ctx, cfg, owner.CustomerID, messageID, credits)
		if err != nil {
			return 0, err
		}
		if !allowed {
			return 0, errs.ErrInsufficientCredit
		}
	}
	return b.inner.Reserve(ctx, owner, messageID, credits)
}

// authorize calls the provider under the configured sync deadline and resolves a fault per failure_policy:
// fail_closed → external_billing_unavailable (never let unconfirmed credit through); fail_open → proceed as
// authorized, counted and warned (a dead provider silently authorizing is a trap). A genuine denial is
// (false, nil).
func (b *ExternalBiller) authorize(ctx context.Context, cfg ExternalConfig, customerID, messageID uuid.UUID, credits int) (bool, error) {
	timeout := cfg.SyncTimeout
	if timeout <= 0 {
		timeout = defaultAuthorizeTimeout
	}
	actx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	allowed, err := b.provider.Authorize(actx, customerID, messageID, credits)
	if err != nil {
		if cfg.FailurePolicy == cp.FailClosed {
			return false, errs.ErrExternalBillingUnavailable
		}
		b.metric.AuthzFailOpen(cfg.ProviderID)
		b.logger.WarnContext(ctx, "external billing authorize failed; failing OPEN (proceeding unconfirmed)",
			"customer_id", customerID, "provider_id", cfg.ProviderID, "err", err)
		return true, nil
	}
	return allowed, nil
}

// Capture delegates: consumption is reconciled, not confirmed per-capture (§6.10).
func (b *ExternalBiller) Capture(ctx context.Context, owner Owner, messageID uuid.UUID) (int, error) {
	return b.inner.Capture(ctx, owner, messageID)
}

// Release delegates to the internal core.
func (b *ExternalBiller) Release(ctx context.Context, owner Owner, messageID uuid.UUID) error {
	return b.inner.Release(ctx, owner, messageID)
}

// RecordMO delegates: the MO meter is internal-only (external billing is MT authorization, §6.10).
func (b *ExternalBiller) RecordMO(ctx context.Context, owner Owner, messageID uuid.UUID, credits int) (MOResult, error) {
	return b.inner.RecordMO(ctx, owner, messageID, credits)
}
