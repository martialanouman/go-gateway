package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
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

// ListActive returns every active anti-spam rule (the router's evaluation snapshot).
func (r *AntispamRuleRepo) ListActive(ctx context.Context) ([]cp.AntispamRule, error) {
	rows, err := r.q.ListActiveAntispamRules(ctx)
	if err != nil {
		return nil, translate("list active antispam rules", err)
	}
	return antispamRulesFromRows(rows), nil
}

// List returns every anti-spam rule, active and disabled (Admin, step-067).
func (r *AntispamRuleRepo) List(ctx context.Context) ([]cp.AntispamRule, error) {
	rows, err := r.q.ListAntispamRules(ctx)
	if err != nil {
		return nil, translate("list antispam rules", err)
	}
	return antispamRulesFromRows(rows), nil
}

// Get returns an anti-spam rule by id, or ErrNotFound (Admin, step-067 — the update handler reads the
// immutable rule_type to validate a new config_json against it).
func (r *AntispamRuleRepo) Get(ctx context.Context, id uuid.UUID) (cp.AntispamRule, error) {
	row, err := r.q.GetAntispamRule(ctx, id)
	if err != nil {
		return cp.AntispamRule{}, translate("get antispam rule", err)
	}
	return antispamRuleFromRow(row), nil
}

// Create writes an anti-spam rule (Admin, step-067). A scope/scope_id mismatch violates
// antispam_scope_ck and surfaces as ErrValidation (422).
func (r *AntispamRuleRepo) Create(ctx context.Context, in cp.NewAntispamRule) (cp.AntispamRule, error) {
	config := in.ConfigJSON
	if len(config) == 0 {
		config = []byte("{}")
	}
	row, err := r.q.CreateAntispamRule(ctx, sqlcgen.CreateAntispamRuleParams{
		RuleType:   string(in.RuleType),
		Scope:      string(in.Scope),
		ScopeID:    in.ScopeID,
		ConfigJson: config,
		Action:     string(in.Action),
	})
	if err != nil {
		return cp.AntispamRule{}, translate("create antispam rule", err)
	}
	return antispamRuleFromRow(row), nil
}

// Update partially updates an anti-spam rule, or reports ErrNotFound (Admin, step-067).
func (r *AntispamRuleRepo) Update(ctx context.Context, id uuid.UUID, p cp.AntispamRulePatch) (cp.AntispamRule, error) {
	row, err := r.q.UpdateAntispamRule(ctx, sqlcgen.UpdateAntispamRuleParams{
		ID:         id,
		ConfigJson: p.ConfigJSON,
		Action:     strPtr(p.Action),
		Status:     strPtr(p.Status),
	})
	if err != nil {
		return cp.AntispamRule{}, translate("update antispam rule", err)
	}
	return antispamRuleFromRow(row), nil
}

// Delete removes an anti-spam rule by id, or reports ErrNotFound (Admin, step-067).
func (r *AntispamRuleRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteAntispamRule(ctx, id)
	if err != nil {
		return translate("delete antispam rule", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func antispamRulesFromRows(rows []sqlcgen.ControlPlaneAntispamRule) []cp.AntispamRule {
	out := make([]cp.AntispamRule, 0, len(rows))
	for _, row := range rows {
		out = append(out, antispamRuleFromRow(row))
	}
	return out
}

func antispamRuleFromRow(row sqlcgen.ControlPlaneAntispamRule) cp.AntispamRule {
	return cp.AntispamRule{
		ID:         row.ID,
		RuleType:   cp.AntispamRuleType(row.RuleType),
		Scope:      cp.AntispamScope(row.Scope),
		ScopeID:    row.ScopeID,
		ConfigJSON: row.ConfigJson,
		Action:     cp.AntispamAction(row.Action),
		Status:     cp.AntispamRuleStatus(row.Status),
		CreatedAt:  tsVal(row.CreatedAt),
		UpdatedAt:  tsVal(row.UpdatedAt),
	}
}
