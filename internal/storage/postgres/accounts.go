package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// AccountRepo is the SMPP accounts repository. It satisfies adminapi.AccountStore structurally.
type AccountRepo struct {
	q *sqlcgen.Queries
}

// NewAccountRepo returns the accounts repository backed by pool.
func NewAccountRepo(pool *pgxpool.Pool) *AccountRepo {
	return &AccountRepo{q: sqlcgen.New(pool)}
}

// Create inserts an SMPP account, applying the schema defaults for the optional fields left nil.
func (r *AccountRepo) Create(ctx context.Context, in cp.NewAccount) (cp.Account, error) {
	row, err := r.q.CreateAccount(ctx, sqlcgen.CreateAccountParams{
		CustomerID:       in.CustomerID,
		Name:             in.Name,
		SmppEnabled:      in.SMPPEnabled,
		RestEnabled:      in.RESTEnabled,
		SenderIDPolicy:   strPtr(in.SenderIDPolicy),
		QuerySmEnabled:   in.QuerySMEnabled,
		CancelSmEnabled:  in.CancelSMEnabled,
		AllowedBindTypes: strPtr(in.AllowedBindTypes),
		MaxSessions:      i32ptr(in.MaxSessions),
	})
	if err != nil {
		return cp.Account{}, translate("create account", err)
	}
	return accountFromRow(row), nil
}

// Get returns the account with id, or ErrNotFound.
func (r *AccountRepo) Get(ctx context.Context, id uuid.UUID) (cp.Account, error) {
	row, err := r.q.GetAccount(ctx, id)
	if err != nil {
		return cp.Account{}, translate("get account", err)
	}
	return accountFromRow(row), nil
}

// List returns one keyset page of accounts and whether a further page exists.
func (r *AccountRepo) List(ctx context.Context, f cp.AccountFilter) (cp.Page[cp.Account], error) {
	rows, err := r.q.ListAccounts(ctx, sqlcgen.ListAccountsParams{
		CustomerID: f.CustomerID,
		Status:     strPtr(f.Status),
		After:      afterPtr(f.After),
		GroupID:    f.GroupID,
		//nolint:gosec // G115: Limit is capped at 500 by the API, so +1 cannot overflow int32.
		Lim: int32(f.Limit) + 1,
	})
	if err != nil {
		return cp.Page[cp.Account]{}, translate("list accounts", err)
	}
	items := make([]cp.Account, 0, len(rows))
	for _, row := range rows {
		items = append(items, accountFromRow(row))
	}
	return paginate(items, f.Limit, func(a cp.Account) uuid.UUID { return a.ID }), nil
}

// Update applies a partial change (name and status only) and returns the account, or ErrNotFound.
func (r *AccountRepo) Update(ctx context.Context, id uuid.UUID, p cp.AccountPatch) (cp.Account, error) {
	row, err := r.q.UpdateAccount(ctx, sqlcgen.UpdateAccountParams{
		ID:     id,
		Name:   p.Name,
		Status: strPtr(p.Status),
	})
	if err != nil {
		return cp.Account{}, translate("update account", err)
	}
	return accountFromRow(row), nil
}

// Delete removes an account, or reports ErrNotFound when nothing matched.
func (r *AccountRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteAccount(ctx, id)
	if err != nil {
		return translate("delete account", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// SetChannels sets the SMPP and REST channel flags. The smpp_accounts_channel_ck constraint refuses
// disabling both, which translate() reports as a validation error (422).
func (r *AccountRepo) SetChannels(ctx context.Context, id uuid.UUID, smpp, rest bool) (cp.Account, error) {
	row, err := r.q.SetAccountChannels(ctx, sqlcgen.SetAccountChannelsParams{
		ID:          id,
		SmppEnabled: smpp,
		RestEnabled: rest,
	})
	if err != nil {
		return cp.Account{}, translate("set account channels", err)
	}
	return accountFromRow(row), nil
}

// SetSessionLimits sets the max session count and the allowed bind type.
func (r *AccountRepo) SetSessionLimits(ctx context.Context, id uuid.UUID, maxSessions int, bind cp.BindType) (cp.Account, error) {
	row, err := r.q.SetAccountSessionLimits(ctx, sqlcgen.SetAccountSessionLimitsParams{
		ID: id,
		//nolint:gosec // G115: max_sessions is bounded by the API (min 0) and small in practice.
		MaxSessions:      int32(maxSessions),
		AllowedBindTypes: string(bind),
	})
	if err != nil {
		return cp.Account{}, translate("set account session limits", err)
	}
	return accountFromRow(row), nil
}

// Suspend sets a single account to suspended (the customer-level cascade lives on CustomerRepo).
func (r *AccountRepo) Suspend(ctx context.Context, id uuid.UUID) (cp.Account, error) {
	row, err := r.q.SuspendAccount(ctx, id)
	if err != nil {
		return cp.Account{}, translate("suspend account", err)
	}
	return accountFromRow(row), nil
}

func accountFromRow(row sqlcgen.ControlPlaneSmppAccount) cp.Account {
	return cp.Account{
		ID:               row.ID,
		CustomerID:       row.CustomerID,
		Name:             row.Name,
		Status:           cp.AccountStatus(row.Status),
		SMPPEnabled:      row.SmppEnabled,
		RESTEnabled:      row.RestEnabled,
		SenderIDPolicy:   cp.SenderIDPolicy(row.SenderIDPolicy),
		QuerySMEnabled:   row.QuerySmEnabled,
		CancelSMEnabled:  row.CancelSmEnabled,
		AllowedBindTypes: cp.BindType(row.AllowedBindTypes),
		MaxSessions:      int(row.MaxSessions),
		CreatedAt:        tsVal(row.CreatedAt),
		UpdatedAt:        tsVal(row.UpdatedAt),
	}
}

// ListAccountCustomers returns the account -> customer map for the MO router's snapshot (step-045):
// resolving an inbound number or keyword to an account needs the owning customer for the routed
// envelope.
func (r *AccountRepo) ListAccountCustomers(ctx context.Context) (map[uuid.UUID]uuid.UUID, error) {
	rows, err := r.q.ListAccountCustomers(ctx)
	if err != nil {
		return nil, translate("list account customers", err)
	}
	out := make(map[uuid.UUID]uuid.UUID, len(rows))
	for _, row := range rows {
		out[row.ID] = row.CustomerID
	}
	return out, nil
}

// ListSenderIDPolicies returns every account's sender-ID policy with its owning customer, for the
// sender-ID authorization snapshot (step-060).
func (r *AccountRepo) ListSenderIDPolicies(ctx context.Context) ([]cp.AccountSenderIDPolicy, error) {
	rows, err := r.q.ListAccountSenderIDPolicies(ctx)
	if err != nil {
		return nil, translate("list account sender-id policies", err)
	}
	out := make([]cp.AccountSenderIDPolicy, 0, len(rows))
	for _, row := range rows {
		out = append(out, cp.AccountSenderIDPolicy{
			AccountID:  row.ID,
			CustomerID: row.CustomerID,
			Policy:     cp.SenderIDPolicy(row.SenderIDPolicy),
		})
	}
	return out, nil
}
