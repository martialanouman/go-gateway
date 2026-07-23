package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// entityTypeSMPPAccount is the rate_limits.entity_type discriminator for an SMPP account. The table
// is polymorphic (smpp_account/connector/route) so the scope is carried in this column, not an FK.
const entityTypeSMPPAccount = "smpp_account"

// RateLimitRepo is the throughput-limits repository.
type RateLimitRepo struct {
	q *sqlcgen.Queries
}

// NewRateLimitRepo returns the rate-limits repository backed by pool.
func NewRateLimitRepo(pool *pgxpool.Pool) *RateLimitRepo {
	return &RateLimitRepo{q: sqlcgen.New(pool)}
}

// RateLimit returns the throughput limit configured for an SMPP account. The false return means no
// row is configured for the account, which is a legitimate "no explicit limit" state rather than an
// error.
func (r *RateLimitRepo) RateLimit(ctx context.Context, accountID uuid.UUID) (cp.RateLimit, bool, error) {
	row, err := r.q.GetRateLimit(ctx, sqlcgen.GetRateLimitParams{
		EntityType: entityTypeSMPPAccount,
		EntityID:   accountID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return cp.RateLimit{}, false, nil
	}
	if err != nil {
		return cp.RateLimit{}, false, translate("get rate limit", err)
	}
	return cp.RateLimit{
		MaxPerSec:     intptr(row.MaxPerSec),
		MaxPerDay:     intptr(row.MaxPerDay),
		BurstCapacity: intptr(row.BurstCapacity),
	}, true, nil
}
