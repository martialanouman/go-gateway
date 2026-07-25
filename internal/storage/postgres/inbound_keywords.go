package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// InboundKeywordRepo is the inbound-keywords repository. It satisfies adminapi.InboundKeywordStore
// structurally.
type InboundKeywordRepo struct {
	q *sqlcgen.Queries
}

// NewInboundKeywordRepo returns the inbound-keywords repository backed by pool.
func NewInboundKeywordRepo(pool *pgxpool.Pool) *InboundKeywordRepo {
	return &InboundKeywordRepo{q: sqlcgen.New(pool)}
}

// Create inserts a keyword. An unknown inbound_number_id or account_id is a foreign-key violation,
// reported as validation (422) by translate().
func (r *InboundKeywordRepo) Create(ctx context.Context, in cp.NewInboundKeyword) (cp.InboundKeyword, error) {
	row, err := r.q.CreateInboundKeyword(ctx, sqlcgen.CreateInboundKeywordParams{
		InboundNumberID: in.InboundNumberID,
		Keyword:         in.Keyword,
		MatchType:       string(in.MatchType),
		AccountID:       in.AccountID,
		Priority:        int32(in.Priority), //nolint:gosec // G115: the handler rejects a priority outside int32 (checkPriorityRange).
	})
	if err != nil {
		return cp.InboundKeyword{}, translate("create inbound keyword", err)
	}
	return inboundKeywordFromRow(row), nil
}

// ListAll returns every active keyword across all shared numbers, ordered by number then priority,
// for the MO router's in-memory snapshot (step-045).
func (r *InboundKeywordRepo) ListAll(ctx context.Context) ([]cp.InboundKeyword, error) {
	rows, err := r.q.ListAllInboundKeywords(ctx)
	if err != nil {
		return nil, translate("list all inbound keywords", err)
	}
	out := make([]cp.InboundKeyword, 0, len(rows))
	for _, row := range rows {
		out = append(out, inboundKeywordFromRow(row))
	}
	return out, nil
}

// ListByInboundNumber returns the keywords of one inbound number, ordered by priority then id.
func (r *InboundKeywordRepo) ListByInboundNumber(ctx context.Context, inboundNumberID uuid.UUID) ([]cp.InboundKeyword, error) {
	rows, err := r.q.ListInboundKeywords(ctx, inboundNumberID)
	if err != nil {
		return nil, translate("list inbound keywords", err)
	}
	out := make([]cp.InboundKeyword, 0, len(rows))
	for _, row := range rows {
		out = append(out, inboundKeywordFromRow(row))
	}
	return out, nil
}

// Update applies a partial change to a keyword within its number and returns it, or ErrNotFound when
// the (keyword, number) pair matches no row.
func (r *InboundKeywordRepo) Update(ctx context.Context, inboundNumberID, keywordID uuid.UUID, p cp.InboundKeywordPatch) (cp.InboundKeyword, error) {
	row, err := r.q.UpdateInboundKeyword(ctx, sqlcgen.UpdateInboundKeywordParams{
		ID:              keywordID,
		InboundNumberID: inboundNumberID,
		Keyword:         p.Keyword,
		MatchType:       strPtr(p.MatchType),
		AccountID:       p.AccountID,
		Priority:        i32ptr(p.Priority),
		Status:          strPtr(p.Status),
	})
	if err != nil {
		return cp.InboundKeyword{}, translate("update inbound keyword", err)
	}
	return inboundKeywordFromRow(row), nil
}

// Delete removes a keyword within its number, or reports ErrNotFound when the pair matched no row.
func (r *InboundKeywordRepo) Delete(ctx context.Context, inboundNumberID, keywordID uuid.UUID) error {
	n, err := r.q.DeleteInboundKeyword(ctx, sqlcgen.DeleteInboundKeywordParams{
		ID:              keywordID,
		InboundNumberID: inboundNumberID,
	})
	if err != nil {
		return translate("delete inbound keyword", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// inboundKeywordFromRow maps a sqlc row to the domain type.
func inboundKeywordFromRow(row sqlcgen.ControlPlaneInboundKeyword) cp.InboundKeyword {
	return cp.InboundKeyword{
		ID:              row.ID,
		InboundNumberID: row.InboundNumberID,
		Keyword:         row.Keyword,
		MatchType:       cp.MatchType(row.MatchType),
		AccountID:       row.AccountID,
		Priority:        int(row.Priority),
		Status:          cp.InboundKeywordStatus(row.Status),
		CreatedAt:       tsVal(row.CreatedAt),
		UpdatedAt:       tsVal(row.UpdatedAt),
	}
}
