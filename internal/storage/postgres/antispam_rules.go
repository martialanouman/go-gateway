package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// AntispamRuleRepo is the anti-spam rules repository, read at boot for the router's evaluation
// snapshot (step-065).
type AntispamRuleRepo struct {
	q *sqlcgen.Queries
}

// NewAntispamRuleRepo returns the anti-spam rules repository backed by pool.
func NewAntispamRuleRepo(pool *pgxpool.Pool) *AntispamRuleRepo {
	return &AntispamRuleRepo{q: sqlcgen.New(pool)}
}

// ListActive returns every active anti-spam rule.
func (r *AntispamRuleRepo) ListActive(ctx context.Context) ([]cp.AntispamRule, error) {
	rows, err := r.q.ListActiveAntispamRules(ctx)
	if err != nil {
		return nil, translate("list antispam rules", err)
	}
	out := make([]cp.AntispamRule, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.AntispamRule{
			ID:         row.ID,
			RuleType:   cp.AntispamRuleType(row.RuleType),
			Scope:      cp.AntispamScope(row.Scope),
			ScopeID:    row.ScopeID,
			ConfigJSON: row.ConfigJson,
			Action:     cp.AntispamAction(row.Action),
			Status:     cp.AntispamRuleStatus(row.Status),
			CreatedAt:  tsVal(row.CreatedAt),
			UpdatedAt:  tsVal(row.UpdatedAt),
		})
	}
	return out, nil
}
