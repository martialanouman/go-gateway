package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/martialanouman/go-gateway/internal/routing/script"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// RoutingScriptRepo persists routing scripts (control_plane.routing_scripts) and their lifecycle — the
// operator-authored JS/Lua that resolves a message to a route (schema §12).
type RoutingScriptRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// NewRoutingScriptRepo returns the routing-script repository backed by pool.
func NewRoutingScriptRepo(pool *pgxpool.Pool) *RoutingScriptRepo {
	return &RoutingScriptRepo{pool: pool, q: sqlcgen.New(pool)}
}

// Create persists a new script as a draft (publish is a separate step) and returns it.
func (r *RoutingScriptRepo) Create(ctx context.Context, s script.Script) (script.Script, error) {
	row, err := r.q.CreateRoutingScript(ctx, sqlcgen.CreateRoutingScriptParams{
		Scope:           string(s.Scope),
		ScopeID:         s.ScopeID,
		Name:            s.Name,
		Language:        string(s.Language),
		SourceCode:      s.Source,
		Checksum:        s.Checksum,
		TimeoutMs:       int32(s.TimeoutMs), //nolint:gosec // bounded 1..20 by the schema CHECK
		MaxInstructions: s.MaxInstructions,
		MaxMemoryKb:     i32ptr(s.MaxMemoryKB),
		CreatedBy:       s.CreatedBy,
	})
	if err != nil {
		return script.Script{}, translate("create routing script", err)
	}
	return toRoutingScript(row), nil
}

// Get returns a script by id. found is false when none matches.
func (r *RoutingScriptRepo) Get(ctx context.Context, id uuid.UUID) (script.Script, bool, error) {
	row, err := r.q.GetRoutingScript(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return script.Script{}, false, nil
	}
	if err != nil {
		return script.Script{}, false, translate("get routing script", err)
	}
	return toRoutingScript(row), true, nil
}

// GetActive returns the single active script for a (scope, scope_id), if any. found is false when no
// script is active for that scope — the resolver then walks to the next scope (or falls back).
func (r *RoutingScriptRepo) GetActive(ctx context.Context, scope script.Scope, scopeID *uuid.UUID) (script.Script, bool, error) {
	row, err := r.q.GetActiveRoutingScript(ctx, sqlcgen.GetActiveRoutingScriptParams{
		Scope: string(scope), ScopeID: scopeID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return script.Script{}, false, nil
	}
	if err != nil {
		return script.Script{}, false, translate("get active routing script", err)
	}
	return toRoutingScript(row), true, nil
}

// Update rewrites a script's editable fields and returns the updated row. found is false when no script
// matches the id.
func (r *RoutingScriptRepo) Update(ctx context.Context, id uuid.UUID, s script.Script) (script.Script, bool, error) {
	row, err := r.q.UpdateRoutingScript(ctx, sqlcgen.UpdateRoutingScriptParams{
		ID:              id,
		Name:            s.Name,
		Language:        string(s.Language),
		SourceCode:      s.Source,
		Checksum:        s.Checksum,
		TimeoutMs:       int32(s.TimeoutMs), //nolint:gosec // bounded 1..20 by the schema CHECK
		MaxInstructions: s.MaxInstructions,
		MaxMemoryKb:     i32ptr(s.MaxMemoryKB),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return script.Script{}, false, nil
	}
	if err != nil {
		return script.Script{}, false, translate("update routing script", err)
	}
	return toRoutingScript(row), true, nil
}

// Delete removes a script. found is false when no row matched.
func (r *RoutingScriptRepo) Delete(ctx context.Context, id uuid.UUID) (bool, error) {
	n, err := r.q.DeleteRoutingScript(ctx, id)
	if err != nil {
		return false, translate("delete routing script", err)
	}
	return n > 0, nil
}

// ListVersions returns every version for a (scope, scope_id), newest first.
func (r *RoutingScriptRepo) ListVersions(ctx context.Context, scope script.Scope, scopeID *uuid.UUID) ([]script.Script, error) {
	rows, err := r.q.ListRoutingScriptVersions(ctx, sqlcgen.ListRoutingScriptVersionsParams{
		Scope: string(scope), ScopeID: scopeID,
	})
	if err != nil {
		return nil, translate("list routing script versions", err)
	}
	return toRoutingScripts(rows), nil
}

// ListActive returns every active script across all scopes — the input to the router's immutable
// script snapshot (step-110).
func (r *RoutingScriptRepo) ListActive(ctx context.Context) ([]script.Script, error) {
	rows, err := r.q.ListActiveRoutingScripts(ctx)
	if err != nil {
		return nil, translate("list active routing scripts", err)
	}
	return toRoutingScripts(rows), nil
}

// List returns a page of all scripts in id order, after the cursor (nil UUID for the first page).
func (r *RoutingScriptRepo) List(ctx context.Context, after uuid.UUID, limit int) ([]script.Script, error) {
	rows, err := r.q.ListRoutingScripts(ctx, sqlcgen.ListRoutingScriptsParams{
		After: after,
		Lim:   int32(limit), //nolint:gosec // a page size is a small positive integer
	})
	if err != nil {
		return nil, translate("list routing scripts", err)
	}
	return toRoutingScripts(rows), nil
}

// Publish promotes a script to active, demoting any current active for the same scope in one
// transaction so the one-active-per-scope unique index is never violated. found is false when the id
// does not exist.
func (r *RoutingScriptRepo) Publish(ctx context.Context, id uuid.UUID) (script.Script, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return script.Script{}, false, translate("publish routing script", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	qtx := r.q.WithTx(tx)
	target, err := qtx.GetRoutingScript(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return script.Script{}, false, nil
	}
	if err != nil {
		return script.Script{}, false, translate("publish routing script", err)
	}
	// Demote the current active for this scope, then promote the target — both under the same tx so a
	// reader never sees two actives and a failure rolls back cleanly.
	if _, err := qtx.DemoteActiveRoutingScript(ctx, sqlcgen.DemoteActiveRoutingScriptParams{
		Scope: target.Scope, ScopeID: target.ScopeID,
	}); err != nil {
		return script.Script{}, false, translate("publish routing script", err)
	}
	row, err := qtx.PromoteRoutingScript(ctx, id)
	if err != nil {
		return script.Script{}, false, translate("publish routing script", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return script.Script{}, false, translate("publish routing script", err)
	}
	return toRoutingScript(row), true, nil
}

// toRoutingScript maps a stored row to the domain type.
func toRoutingScript(row sqlcgen.ControlPlaneRoutingScript) script.Script {
	return script.Script{
		ID:              row.ID,
		Scope:           script.Scope(row.Scope),
		ScopeID:         row.ScopeID,
		Name:            row.Name,
		Language:        script.Language(row.Language),
		Source:          row.SourceCode,
		Checksum:        row.Checksum,
		Status:          script.Status(row.Status),
		TimeoutMs:       int(row.TimeoutMs),
		MaxInstructions: row.MaxInstructions,
		MaxMemoryKB:     intptr(row.MaxMemoryKb),
		CreatedBy:       row.CreatedBy,
		CreatedAt:       tsVal(row.CreatedAt),
		PublishedAt:     tsPtr(row.PublishedAt),
	}
}

func toRoutingScripts(rows []sqlcgen.ControlPlaneRoutingScript) []script.Script {
	out := make([]script.Script, len(rows))
	for i, row := range rows {
		out[i] = toRoutingScript(row)
	}
	return out
}
