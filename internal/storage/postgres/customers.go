package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// CustomerRepo is the customers repository. It satisfies adminapi.CustomerStore structurally; the
// interface is declared on the consumer side, so this package never imports adminapi.
type CustomerRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// NewCustomerRepo returns the customers repository backed by pool.
func NewCustomerRepo(pool *pgxpool.Pool) *CustomerRepo {
	return &CustomerRepo{pool: pool, q: sqlcgen.New(pool)}
}

// Create inserts a customer, applying the schema defaults for the optional fields left nil.
func (r *CustomerRepo) Create(ctx context.Context, in cp.NewCustomer) (cp.Customer, error) {
	row, err := r.q.CreateCustomer(ctx, sqlcgen.CreateCustomerParams{
		Name:                 in.Name,
		GroupID:              in.GroupID,
		RatePlanID:           in.RatePlanID,
		BillingEnabled:       in.BillingEnabled,
		BillingMode:          strPtr(in.BillingMode),
		OverdraftEnabled:     in.OverdraftEnabled,
		OverdraftLimit:       i32ptr(in.OverdraftLimit),
		CreditLimit:          i32ptr(in.CreditLimit),
		CreditLimitIsHard:    in.CreditLimitIsHard,
		BalanceScope:         strPtr(in.BalanceScope),
		MoBillingFloor:       i32ptr(in.MoBillingFloor),
		ContentStorage:       strPtr(in.ContentStorage),
		ContentRetentionDays: i32ptr(in.ContentRetentionDays),
	})
	if err != nil {
		return cp.Customer{}, translate("create customer", err)
	}
	return customerFromRow(row), nil
}

// Get returns the customer with id, or ErrNotFound.
func (r *CustomerRepo) Get(ctx context.Context, id uuid.UUID) (cp.Customer, error) {
	row, err := r.q.GetCustomer(ctx, id)
	if err != nil {
		return cp.Customer{}, translate("get customer", err)
	}
	return customerFromRow(row), nil
}

// List returns one keyset page of customers plus whether a further page exists. It asks the query
// for one row beyond the page size to decide HasMore without a second round-trip.
func (r *CustomerRepo) List(ctx context.Context, f cp.CustomerFilter) (cp.Page[cp.Customer], error) {
	rows, err := r.q.ListCustomers(ctx, sqlcgen.ListCustomersParams{
		GroupID: f.GroupID,
		Status:  strPtr(f.Status),
		After:   afterPtr(f.After),
		//nolint:gosec // G115: Limit is capped at 500 by the API, so +1 cannot overflow int32.
		Lim: int32(f.Limit) + 1,
	})
	if err != nil {
		return cp.Page[cp.Customer]{}, translate("list customers", err)
	}

	items := make([]cp.Customer, 0, len(rows))
	for _, row := range rows {
		items = append(items, customerFromRow(row))
	}
	return paginate(items, f.Limit, func(c cp.Customer) uuid.UUID { return c.ID }), nil
}

// Update applies a partial change and returns the updated customer, or ErrNotFound.
func (r *CustomerRepo) Update(ctx context.Context, id uuid.UUID, p cp.CustomerPatch) (cp.Customer, error) {
	row, err := r.q.UpdateCustomer(ctx, sqlcgen.UpdateCustomerParams{
		ID:                        id,
		Name:                      p.Name,
		Status:                    strPtr(p.Status),
		RatePlanID:                p.RatePlanID,
		BillingEnabled:            p.BillingEnabled,
		BillingMode:               strPtr(p.BillingMode),
		OverdraftEnabled:          p.OverdraftEnabled,
		OverdraftLimit:            i32ptr(p.OverdraftLimit),
		CreditLimit:               i32ptr(p.CreditLimit),
		CreditLimitIsHard:         p.CreditLimitIsHard,
		MoBillingFloor:            i32ptr(p.MoBillingFloor),
		ExternalBillingProviderID: p.ExternalBillingProviderID,
		ContentStorage:            strPtr(p.ContentStorage),
		ContentRetentionDays:      i32ptr(p.ContentRetentionDays),
	})
	if err != nil {
		return cp.Customer{}, translate("update customer", err)
	}
	return customerFromRow(row), nil
}

// Delete removes a customer. A delete that matches no row is ErrNotFound; the schema's ON DELETE
// rules cascade to the customer's accounts and credentials.
func (r *CustomerRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteCustomer(ctx, id)
	if err != nil {
		return translate("delete customer", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// Suspend sets the customer and every one of its accounts to suspended, in one transaction. The
// cascade is an M1 acceptance criterion; without the transaction a mid-flight failure would leave a
// suspended customer with active accounts.
func (r *CustomerRepo) Suspend(ctx context.Context, id uuid.UUID) (cp.Customer, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return cp.Customer{}, translate("suspend customer", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	qtx := r.q.WithTx(tx)
	row, err := qtx.SuspendCustomer(ctx, id)
	if err != nil {
		return cp.Customer{}, translate("suspend customer", err)
	}
	if err := qtx.SuspendCustomerAccounts(ctx, id); err != nil {
		return cp.Customer{}, translate("suspend customer accounts", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return cp.Customer{}, translate("suspend customer", err)
	}
	return customerFromRow(row), nil
}

// customerFromRow maps a generated row to the domain type.
func customerFromRow(row sqlcgen.ControlPlaneCustomer) cp.Customer {
	return cp.Customer{
		ID:                        row.ID,
		Name:                      row.Name,
		Status:                    cp.CustomerStatus(row.Status),
		GroupID:                   row.GroupID,
		RatePlanID:                row.RatePlanID,
		BillingEnabled:            row.BillingEnabled,
		BillingMode:               enumPtr[cp.BillingMode](row.BillingMode),
		OverdraftEnabled:          row.OverdraftEnabled,
		OverdraftLimit:            intptr(row.OverdraftLimit),
		CreditLimit:               intptr(row.CreditLimit),
		CreditLimitIsHard:         row.CreditLimitIsHard,
		ExternalBillingProviderID: row.ExternalBillingProviderID,
		BalanceScope:              cp.BalanceScope(row.BalanceScope),
		MoBillingFloor:            intptr(row.MoBillingFloor),
		ContentStorage:            cp.ContentStorage(row.ContentStorage),
		ContentRetentionDays:      intptr(row.ContentRetentionDays),
		ContentKeyID:              row.ContentKeyID,
		CreatedAt:                 tsVal(row.CreatedAt),
		UpdatedAt:                 tsVal(row.UpdatedAt),
	}
}

// ListContentStorage returns every customer's content_storage for the data-plane content-policy snapshot
// (content.PolicySnapshot). It is a bulk boot-time read, not a per-message query.
func (r *CustomerRepo) ListContentStorage(ctx context.Context) ([]cp.CustomerContentPolicy, error) {
	rows, err := r.q.ListContentStorage(ctx)
	if err != nil {
		return nil, translate("list content storage", err)
	}
	out := make([]cp.CustomerContentPolicy, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.CustomerContentPolicy{CustomerID: row.ID, ContentStorage: cp.ContentStorage(row.ContentStorage)})
	}
	return out, nil
}
