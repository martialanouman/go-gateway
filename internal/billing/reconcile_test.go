package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// fakeReconcileRepo returns a fixed externally-billed customer list and per-customer local consumption.
type fakeReconcileRepo struct {
	configs  []cp.CustomerExternalBilling
	consumed map[uuid.UUID]int64
	listErr  error
}

func (r *fakeReconcileRepo) ListExternalBillingConfigs(context.Context) ([]cp.CustomerExternalBilling, error) {
	return r.configs, r.listErr
}
func (r *fakeReconcileRepo) ConsumedCredits(_ context.Context, customerID uuid.UUID) (int64, error) {
	return r.consumed[customerID], nil
}

type countDiscrepancy struct{ n int }

func (c *countDiscrepancy) Discrepancy(uuid.UUID) { c.n++ }

// TestReconcileFlagsDiscrepancy: a customer whose local settled consumption differs from the provider's usage
// is reported; a matching customer is not.
func TestReconcileFlagsDiscrepancy(t *testing.T) {
	mismatch, match := uuid.New(), uuid.New()
	provID := uuid.New()
	repo := &fakeReconcileRepo{
		configs: []cp.CustomerExternalBilling{
			{CustomerID: mismatch, ProviderID: provID, Mode: cp.ExternalModeConsumeAsync},
			{CustomerID: match, ProviderID: provID, Mode: cp.ExternalModeConsumeAsync},
		},
		consumed: map[uuid.UUID]int64{mismatch: 100, match: 50},
	}
	provider := billing.NewStubProvider()
	provider.SetUsage(mismatch, 80) // local 100 vs external 80 → discrepancy
	provider.SetUsage(match, 50)    // agree → no discrepancy
	metric := &countDiscrepancy{}

	rec := billing.NewReconciler(repo, provider, billing.WithDiscrepancyMetric(metric))
	if err := rec.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if metric.n != 1 {
		t.Errorf("discrepancy count = %d, want 1 (only the mismatched customer)", metric.n)
	}
}

// TestReconcileTolerance: a difference within the tolerance is not flagged (in-flight/rounding skew).
func TestReconcileTolerance(t *testing.T) {
	cust := uuid.New()
	repo := &fakeReconcileRepo{
		configs:  []cp.CustomerExternalBilling{{CustomerID: cust, ProviderID: uuid.New()}},
		consumed: map[uuid.UUID]int64{cust: 105},
	}
	provider := billing.NewStubProvider()
	provider.SetUsage(cust, 100) // diff 5, within tolerance 10
	metric := &countDiscrepancy{}

	rec := billing.NewReconciler(repo, provider, billing.WithDiscrepancyMetric(metric), billing.WithTolerance(10))
	if err := rec.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if metric.n != 0 {
		t.Errorf("discrepancy count = %d, want 0 (within tolerance)", metric.n)
	}
}

// TestReconcileListErrorAborts: a failure loading the customer list aborts the pass (the caller retries).
func TestReconcileListErrorAborts(t *testing.T) {
	repo := &fakeReconcileRepo{listErr: errors.New("db down")}
	rec := billing.NewReconciler(repo, billing.NewStubProvider())
	if err := rec.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("ReconcileOnce must return the list-load error")
	}
}
