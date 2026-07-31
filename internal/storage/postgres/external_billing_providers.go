package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// ExternalBillingProviderRepo is the external_billing_providers repository (§6.10). It satisfies the admin
// provider store structurally; the interface is declared consumer-side. auth_config_json is returned raw —
// the handler masks it before a client sees it.
type ExternalBillingProviderRepo struct {
	q *sqlcgen.Queries
}

// NewExternalBillingProviderRepo returns the provider repository backed by pool.
func NewExternalBillingProviderRepo(pool *pgxpool.Pool) *ExternalBillingProviderRepo {
	return &ExternalBillingProviderRepo{q: sqlcgen.New(pool)}
}

// List returns every provider, ordered by name.
func (r *ExternalBillingProviderRepo) List(ctx context.Context) ([]cp.ExternalBillingProvider, error) {
	rows, err := r.q.ListExternalProviders(ctx)
	if err != nil {
		return nil, translate("list external providers", err)
	}
	out := make([]cp.ExternalBillingProvider, 0, len(rows))
	for _, row := range rows {
		out = append(out, providerFromRow(row))
	}
	return out, nil
}

// Get returns one provider by id, or ErrNotFound.
func (r *ExternalBillingProviderRepo) Get(ctx context.Context, id uuid.UUID) (cp.ExternalBillingProvider, error) {
	row, err := r.q.GetExternalProvider(ctx, id)
	if err != nil {
		return cp.ExternalBillingProvider{}, translate("get external provider", err)
	}
	return providerFromRow(row), nil
}

// Create inserts a provider.
func (r *ExternalBillingProviderRepo) Create(ctx context.Context, in cp.NewExternalBillingProvider) (cp.ExternalBillingProvider, error) {
	row, err := r.q.CreateExternalProvider(ctx, sqlcgen.CreateExternalProviderParams{
		Name:              in.Name,
		BaseUrl:           in.BaseURL,
		AuthConfigJson:    in.AuthConfig,
		Mode:              in.Mode,
		CacheTtlMs:        i32ptr(in.CacheTTLMs),
		SyncCallTimeoutMs: i32ptr(in.SyncCallTimeoutMs),
		FailurePolicy:     strPtr(in.FailurePolicy),
	})
	if err != nil {
		return cp.ExternalBillingProvider{}, translate("create external provider", err)
	}
	return providerFromRow(row), nil
}

// Update applies a partial change and returns the updated provider, or ErrNotFound.
func (r *ExternalBillingProviderRepo) Update(ctx context.Context, id uuid.UUID, p cp.ExternalBillingProviderPatch) (cp.ExternalBillingProvider, error) {
	row, err := r.q.UpdateExternalProvider(ctx, sqlcgen.UpdateExternalProviderParams{
		ID:                id,
		Name:              p.Name,
		BaseUrl:           p.BaseURL,
		AuthConfigJson:    p.AuthConfig,
		Mode:              p.Mode,
		CacheTtlMs:        i32ptr(p.CacheTTLMs),
		SyncCallTimeoutMs: i32ptr(p.SyncCallTimeoutMs),
		FailurePolicy:     p.FailurePolicy,
		Status:            p.Status,
	})
	if err != nil {
		return cp.ExternalBillingProvider{}, translate("update external provider", err)
	}
	return providerFromRow(row), nil
}

// Delete removes a provider. customers.external_billing_provider_id references it ON DELETE SET NULL, so
// deleting a provider in use unassigns it; a delete matching no row is ErrNotFound.
func (r *ExternalBillingProviderRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteExternalProvider(ctx, id)
	if err != nil {
		return translate("delete external provider", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func providerFromRow(row sqlcgen.ControlPlaneExternalBillingProvider) cp.ExternalBillingProvider {
	return cp.ExternalBillingProvider{
		ID:                row.ID,
		Name:              row.Name,
		BaseURL:           row.BaseUrl,
		AuthConfig:        row.AuthConfigJson,
		Mode:              row.Mode,
		CacheTTLMs:        int(row.CacheTtlMs),
		SyncCallTimeoutMs: intptr(row.SyncCallTimeoutMs),
		FailurePolicy:     row.FailurePolicy,
		Status:            row.Status,
		CreatedAt:         tsVal(row.CreatedAt),
		UpdatedAt:         tsVal(row.UpdatedAt),
	}
}
