package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// CustomerGroupRepo is the customer_groups repository (§6.17). It satisfies adminapi's group store
// structurally; the interface is declared consumer-side, so this package never imports adminapi.
// It holds no pool: every operation here is a single statement, and nothing about a group spans two
// of them — the detach on delete is the schema's, not this code's.
type CustomerGroupRepo struct {
	q *sqlcgen.Queries
}

// NewCustomerGroupRepo returns the group repository backed by pool.
func NewCustomerGroupRepo(pool *pgxpool.Pool) *CustomerGroupRepo {
	return &CustomerGroupRepo{q: sqlcgen.New(pool)}
}

// List returns the groups matching f, ordered by name. It is not paginated: the contract returns a
// bare array.
func (r *CustomerGroupRepo) List(ctx context.Context, f cp.CustomerGroupFilter) ([]cp.CustomerGroup, error) {
	rows, err := r.q.ListCustomerGroups(ctx, strPtr(f.Status))
	if err != nil {
		return nil, translate("list customer groups", err)
	}
	out := make([]cp.CustomerGroup, 0, len(rows))
	for _, row := range rows {
		out = append(out, customerGroupFromRow(row))
	}
	return out, nil
}

// Get returns one group by id, or ErrNotFound.
func (r *CustomerGroupRepo) Get(ctx context.Context, id uuid.UUID) (cp.CustomerGroup, error) {
	row, err := r.q.GetCustomerGroup(ctx, id)
	if err != nil {
		return cp.CustomerGroup{}, translate("get customer group", err)
	}
	return customerGroupFromRow(row), nil
}

// Create inserts a group. A duplicate name hits the UNIQUE constraint and comes back as ErrConflict.
func (r *CustomerGroupRepo) Create(ctx context.Context, in cp.NewCustomerGroup) (cp.CustomerGroup, error) {
	row, err := r.q.CreateCustomerGroup(ctx, sqlcgen.CreateCustomerGroupParams{
		Name:        in.Name,
		Description: in.Description,
	})
	if err != nil {
		return cp.CustomerGroup{}, translate("create customer group", err)
	}
	return customerGroupFromRow(row), nil
}

// Update applies a partial change and returns the updated group, or ErrNotFound. Renaming onto a
// taken name is ErrConflict, the same UNIQUE constraint create meets.
func (r *CustomerGroupRepo) Update(ctx context.Context, id uuid.UUID, p cp.CustomerGroupPatch) (cp.CustomerGroup, error) {
	row, err := r.q.UpdateCustomerGroup(ctx, sqlcgen.UpdateCustomerGroupParams{
		ID:          id,
		Name:        p.Name,
		Description: p.Description,
		Status:      strPtr(p.Status),
	})
	if err != nil {
		return cp.CustomerGroup{}, translate("update customer group", err)
	}
	return customerGroupFromRow(row), nil
}

// Delete removes a group. customers.group_id references it ON DELETE SET NULL, so this detaches its
// customers and deletes none of them (§6.17) — there is no application-side cascade here, on
// purpose. A delete matching no row is ErrNotFound.
func (r *CustomerGroupRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteCustomerGroup(ctx, id)
	if err != nil {
		return translate("delete customer group", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func customerGroupFromRow(row sqlcgen.ControlPlaneCustomerGroup) cp.CustomerGroup {
	return cp.CustomerGroup{
		ID:          row.ID,
		Name:        row.Name,
		Description: row.Description,
		Status:      cp.CustomerGroupStatus(row.Status),
		CreatedBy:   row.CreatedBy,
		CreatedAt:   tsVal(row.CreatedAt),
		UpdatedAt:   tsVal(row.UpdatedAt),
	}
}
