package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// OptOutKeywordRepo is the repository for opt-out keywords (STOP/START/HELP…), consumed by the MO
// STOP-detection snapshot (step-063) and the Admin API (step-064).
type OptOutKeywordRepo struct {
	q *sqlcgen.Queries
}

// NewOptOutKeywordRepo returns the opt-out keywords repository backed by pool.
func NewOptOutKeywordRepo(pool *pgxpool.Pool) *OptOutKeywordRepo {
	return &OptOutKeywordRepo{q: sqlcgen.New(pool)}
}

// ListActive returns every active opt-out keyword (the MO STOP-detection snapshot).
func (r *OptOutKeywordRepo) ListActive(ctx context.Context) ([]cp.OptOutKeyword, error) {
	rows, err := r.q.ListActiveOptOutKeywords(ctx)
	if err != nil {
		return nil, translate("list active opt-out keywords", err)
	}
	return optOutKeywordsFromRows(rows), nil
}

// List returns every opt-out keyword, active and disabled (Admin, step-064).
func (r *OptOutKeywordRepo) List(ctx context.Context) ([]cp.OptOutKeyword, error) {
	rows, err := r.q.ListOptOutKeywords(ctx)
	if err != nil {
		return nil, translate("list opt-out keywords", err)
	}
	return optOutKeywordsFromRows(rows), nil
}

// Create adds an opt-out keyword (Admin, step-064).
func (r *OptOutKeywordRepo) Create(ctx context.Context, in cp.NewOptOutKeyword) (cp.OptOutKeyword, error) {
	row, err := r.q.CreateOptOutKeyword(ctx, sqlcgen.CreateOptOutKeywordParams{
		CountryCode:       in.CountryCode,
		Keyword:           in.Keyword,
		Action:            string(in.Action),
		MatchType:         strPtr(in.MatchType),
		AutoReplyTemplate: in.AutoReplyTemplate,
	})
	if err != nil {
		return cp.OptOutKeyword{}, translate("create opt-out keyword", err)
	}
	return optOutKeywordFromRow(row), nil
}

// Update partially updates an opt-out keyword, or reports ErrNotFound (Admin, step-064).
func (r *OptOutKeywordRepo) Update(ctx context.Context, id uuid.UUID, p cp.OptOutKeywordPatch) (cp.OptOutKeyword, error) {
	row, err := r.q.UpdateOptOutKeyword(ctx, sqlcgen.UpdateOptOutKeywordParams{
		ID:                id,
		Keyword:           p.Keyword,
		Action:            strPtr(p.Action),
		MatchType:         strPtr(p.MatchType),
		AutoReplyTemplate: p.AutoReplyTemplate,
		Status:            strPtr(p.Status),
	})
	if err != nil {
		return cp.OptOutKeyword{}, translate("update opt-out keyword", err)
	}
	return optOutKeywordFromRow(row), nil
}

// Delete removes an opt-out keyword by id, or reports ErrNotFound (Admin, step-064).
func (r *OptOutKeywordRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteOptOutKeyword(ctx, id)
	if err != nil {
		return translate("delete opt-out keyword", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func optOutKeywordsFromRows(rows []sqlcgen.ControlPlaneOptOutKeyword) []cp.OptOutKeyword {
	out := make([]cp.OptOutKeyword, 0, len(rows))
	for _, row := range rows {
		out = append(out, optOutKeywordFromRow(row))
	}
	return out
}

func optOutKeywordFromRow(row sqlcgen.ControlPlaneOptOutKeyword) cp.OptOutKeyword {
	return cp.OptOutKeyword{
		ID:                row.ID,
		CountryCode:       row.CountryCode,
		Keyword:           row.Keyword,
		Action:            cp.OptOutAction(row.Action),
		MatchType:         cp.OptOutMatchType(row.MatchType),
		AutoReplyTemplate: row.AutoReplyTemplate,
		Status:            cp.OptOutKeywordStatus(row.Status),
		CreatedAt:         tsVal(row.CreatedAt),
		UpdatedAt:         tsVal(row.UpdatedAt),
	}
}
