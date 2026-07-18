package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// APIKeyRepo resolves a presented REST API key to its principal. It is the read side of REST
// authentication (§1.9): the key is SHA-256 hashed by internal/credential, then looked up by that
// hash on the indexed api_key_hash column.
type APIKeyRepo struct {
	q *sqlcgen.Queries
}

// NewAPIKeyRepo returns the API-key repository backed by pool.
func NewAPIKeyRepo(pool *pgxpool.Pool) *APIKeyRepo {
	return &APIKeyRepo{q: sqlcgen.New(pool)}
}

// PrincipalByAPIKeyHash returns the principal behind an api_key_hash. found is false when no active
// api_key matches — an invalid or revoked key — which the caller maps to 401 unauthenticated. A row
// is returned for a matching key even when the channel is disabled or the account is suspended, so
// the verifier can distinguish those (403) from an unknown key (401). A genuine database error is
// returned as-is.
func (r *APIKeyRepo) PrincipalByAPIKeyHash(ctx context.Context, hash string) (cp.APIKeyPrincipal, bool, error) {
	row, err := r.q.GetAPIKeyPrincipal(ctx, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return cp.APIKeyPrincipal{}, false, nil
	}
	if err != nil {
		return cp.APIKeyPrincipal{}, false, fmt.Errorf("lookup api key principal: %w", err)
	}
	return cp.APIKeyPrincipal{
		AccountID:      row.AccountID,
		CustomerID:     row.CustomerID,
		AccountStatus:  cp.AccountStatus(row.AccountStatus),
		CustomerStatus: cp.CustomerStatus(row.CustomerStatus),
		RESTEnabled:    row.RestEnabled,
	}, true, nil
}
