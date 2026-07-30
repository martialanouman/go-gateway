package billing

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// DiscrepancyMetric counts external-billing reconciliation discrepancies (§6.10), labelled by provider (a
// bounded label — never a customer id, message id or MSISDN). A rising count means the local ledger and the
// external provider disagree on a customer's consumption and an operator should investigate.
type DiscrepancyMetric interface {
	Discrepancy(providerID uuid.UUID)
}

type nopDiscrepancyMetric struct{}

func (nopDiscrepancyMetric) Discrepancy(uuid.UUID) {}

// ReconcileRepo is the durable read side the reconciler needs: the externally-billed customer list and each
// customer's locally settled consumption. *postgres.BillingRepo satisfies it; declared consumer-side.
type ReconcileRepo interface {
	ListExternalBillingConfigs(ctx context.Context) ([]cp.CustomerExternalBilling, error)
	ConsumedCredits(ctx context.Context, customerID uuid.UUID) (int64, error)
}

// Reconciler is the periodic §6.10 reconciliation job. For each externally-billed customer it compares the
// local settled consumption to the provider's reported usage and REPORTS a discrepancy beyond a tolerance —
// it never mutates the ledger. Auto-correction is deliberately out of scope: mutating an append-only,
// signed-delta ledger to match external totals would break SUM==balance and, given timing skew, could
// double-charge. It runs off the critical path (a bounded async runner), so its reads never touch the hot path.
type Reconciler struct {
	repo      ReconcileRepo
	provider  ExternalProvider
	metric    DiscrepancyMetric
	logger    *slog.Logger
	tolerance int64
}

// ReconcileOption configures a Reconciler.
type ReconcileOption func(*Reconciler)

// WithDiscrepancyMetric wires the discrepancy counter.
func WithDiscrepancyMetric(m DiscrepancyMetric) ReconcileOption {
	return func(r *Reconciler) {
		if m != nil {
			r.metric = m
		}
	}
}

// WithReconcileLogger sets the logger.
func WithReconcileLogger(l *slog.Logger) ReconcileOption {
	return func(r *Reconciler) {
		if l != nil {
			r.logger = l
		}
	}
}

// WithTolerance sets the absolute credit difference below which local/external skew is ignored (in-flight
// holds and rounding). Default 0 (report any difference).
func WithTolerance(t int64) ReconcileOption {
	return func(r *Reconciler) {
		if t >= 0 {
			r.tolerance = t
		}
	}
}

// NewReconciler builds the reconciliation job over the durable repo and the external provider.
func NewReconciler(repo ReconcileRepo, provider ExternalProvider, opts ...ReconcileOption) *Reconciler {
	r := &Reconciler{repo: repo, provider: provider, metric: nopDiscrepancyMetric{}, logger: slog.Default()}
	for _, o := range opts {
		o(r)
	}
	return r
}

// ReconcileOnce runs one reconciliation pass over every externally-billed customer. A per-customer read
// failure is logged and skipped (one bad customer never aborts the whole pass); a load failure of the
// customer list aborts and is returned so the caller can retry the tick. Report-only.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	configs, err := r.repo.ListExternalBillingConfigs(ctx)
	if err != nil {
		return err
	}
	for _, c := range configs {
		local, err := r.repo.ConsumedCredits(ctx, c.CustomerID)
		if err != nil {
			r.logger.WarnContext(ctx, "reconcile: local consumption read failed; skipping customer",
				"customer_id", c.CustomerID, "provider_id", c.ProviderID, "err", err)
			continue
		}
		external, err := r.provider.Usage(ctx, c.CustomerID)
		if err != nil {
			r.logger.WarnContext(ctx, "reconcile: provider usage read failed; skipping customer",
				"customer_id", c.CustomerID, "provider_id", c.ProviderID, "err", err)
			continue
		}
		if diff := local - external; diff > r.tolerance || diff < -r.tolerance {
			r.metric.Discrepancy(c.ProviderID)
			r.logger.WarnContext(ctx, "external billing discrepancy (report-only)",
				"customer_id", c.CustomerID, "provider_id", c.ProviderID, "local_consumed", local, "external_usage", external)
		}
	}
	return nil
}
