package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// OptOutKeywordRepo is the read-side repository for opt-out keywords (STOP/START/HELP…), consumed by
// the MO STOP-detection snapshot (step-063).
type OptOutKeywordRepo struct {
	q *sqlcgen.Queries
}

// NewOptOutKeywordRepo returns the opt-out keywords repository backed by pool.
func NewOptOutKeywordRepo(pool *pgxpool.Pool) *OptOutKeywordRepo {
	return &OptOutKeywordRepo{q: sqlcgen.New(pool)}
}

// ListActive returns every active opt-out keyword.
func (r *OptOutKeywordRepo) ListActive(ctx context.Context) ([]cp.OptOutKeyword, error) {
	rows, err := r.q.ListActiveOptOutKeywords(ctx)
	if err != nil {
		return nil, translate("list active opt-out keywords", err)
	}
	out := make([]cp.OptOutKeyword, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.OptOutKeyword{
			ID:                row.ID,
			CountryCode:       row.CountryCode,
			Keyword:           row.Keyword,
			Action:            cp.OptOutAction(row.Action),
			MatchType:         cp.OptOutMatchType(row.MatchType),
			AutoReplyTemplate: row.AutoReplyTemplate,
			Status:            cp.OptOutKeywordStatus(row.Status),
			CreatedAt:         tsVal(row.CreatedAt),
			UpdatedAt:         tsVal(row.UpdatedAt),
		})
	}
	return out, nil
}
