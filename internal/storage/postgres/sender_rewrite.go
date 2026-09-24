package postgres

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// SenderRewriteRuleRepo is the sender-ID rewrite rules repository (§6.16).
type SenderRewriteRuleRepo struct {
	q *sqlcgen.Queries
}

// NewSenderRewriteRuleRepo returns the sender-ID rewrite rules repository backed by pool.
func NewSenderRewriteRuleRepo(pool *pgxpool.Pool) *SenderRewriteRuleRepo {
	return &SenderRewriteRuleRepo{q: sqlcgen.New(pool)}
}

// List returns the rules matching f, active and disabled, in evaluation order (§6.16).
func (r *SenderRewriteRuleRepo) List(ctx context.Context, f cp.SenderRewriteFilter) ([]cp.SenderRewriteRule, error) {
	rows, err := r.q.ListSenderRewriteRules(ctx, sqlcgen.ListSenderRewriteRulesParams{
		Scope: strPtr(f.Scope), ScopeID: f.ScopeID,
	})
	if err != nil {
		return nil, translate("list sender rewrite rules", err)
	}
	out := make([]cp.SenderRewriteRule, 0, len(rows))
	for _, row := range rows {
		out = append(out, senderRewriteRuleFromRow(row))
	}
	return out, nil
}

// Get returns a rule by id, or ErrNotFound.
func (r *SenderRewriteRuleRepo) Get(ctx context.Context, id uuid.UUID) (cp.SenderRewriteRule, error) {
	row, err := r.q.GetSenderRewriteRule(ctx, id)
	if err != nil {
		return cp.SenderRewriteRule{}, translate("get sender rewrite rule", err)
	}
	return senderRewriteRuleFromRow(row), nil
}

// Create writes a rule. A scope/scope_id mismatch or a static rule without rewrite_to violates a CHECK
// and surfaces as ErrValidation.
func (r *SenderRewriteRuleRepo) Create(ctx context.Context, in cp.NewSenderRewriteRule) (cp.SenderRewriteRule, error) {
	row, err := r.q.CreateSenderRewriteRule(ctx, sqlcgen.CreateSenderRewriteRuleParams{
		Scope:               string(in.Scope),
		ScopeID:             in.ScopeID,
		MatchSenderPattern:  in.MatchSenderPattern,
		MatchDestPattern:    in.MatchDestPattern,
		RewriteType:         string(in.RewriteType),
		RewriteTo:           in.RewriteTo,
		FallbackPoolJson:    poolJSON(in.FallbackPool),
		MaxLength:           in.MaxLength,
		SanitizeCharsetJson: in.SanitizeCharset,
		Priority:            in.Priority,
		Reason:              in.Reason,
	})
	if err != nil {
		return cp.SenderRewriteRule{}, translate("create sender rewrite rule", err)
	}
	return senderRewriteRuleFromRow(row), nil
}

// Update partially updates a rule, or reports ErrNotFound.
func (r *SenderRewriteRuleRepo) Update(ctx context.Context, id uuid.UUID, p cp.SenderRewriteRulePatch) (cp.SenderRewriteRule, error) {
	row, err := r.q.UpdateSenderRewriteRule(ctx, sqlcgen.UpdateSenderRewriteRuleParams{
		ID:                  id,
		MatchSenderPattern:  p.MatchSenderPattern,
		MatchDestPattern:    p.MatchDestPattern,
		RewriteType:         strPtr(p.RewriteType),
		RewriteTo:           p.RewriteTo,
		FallbackPoolJson:    poolJSON(p.FallbackPool),
		MaxLength:           p.MaxLength,
		SanitizeCharsetJson: p.SanitizeCharset,
		Priority:            p.Priority,
		Reason:              p.Reason,
		Status:              p.Status,
	})
	if err != nil {
		return cp.SenderRewriteRule{}, translate("update sender rewrite rule", err)
	}
	return senderRewriteRuleFromRow(row), nil
}

// Delete removes a rule by id, or reports ErrNotFound.
func (r *SenderRewriteRuleRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteSenderRewriteRule(ctx, id)
	if err != nil {
		return translate("delete sender rewrite rule", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// poolJSON encodes a fallback pool for its jsonb column; nil stays SQL NULL. Marshalling a []string
// cannot fail.
func poolJSON(pool []string) []byte {
	if pool == nil {
		return nil
	}
	raw, _ := json.Marshal(pool)
	return raw
}

func senderRewriteRuleFromRow(row sqlcgen.ControlPlaneSenderIDRewriteRule) cp.SenderRewriteRule {
	var pool []string
	_ = json.Unmarshal(row.FallbackPoolJson, &pool) // the write path only stores a string array or NULL
	return cp.SenderRewriteRule{
		ID:                 row.ID,
		Scope:              cp.SenderRewriteScope(row.Scope),
		ScopeID:            row.ScopeID,
		Direction:          row.Direction,
		MatchSenderPattern: row.MatchSenderPattern,
		MatchDestPattern:   row.MatchDestPattern,
		RewriteType:        cp.SenderRewriteType(row.RewriteType),
		RewriteTo:          row.RewriteTo,
		FallbackPool:       pool,
		MaxLength:          row.MaxLength,
		SanitizeCharset:    row.SanitizeCharsetJson,
		Priority:           row.Priority,
		Reason:             row.Reason,
		Status:             row.Status,
		CreatedBy:          row.CreatedBy,
		CreatedAt:          tsVal(row.CreatedAt),
		UpdatedAt:          tsVal(row.UpdatedAt),
	}
}
