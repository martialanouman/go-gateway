package adminapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// noopBalanceCache is the New default when no cache invalidator is wired: a durable-only write with no cache
// delete (the TTL self-heals). The contract test uses it.
type noopBalanceCache struct{}

func (noopBalanceCache) Del(context.Context, ...string) error { return nil }

type billingHandlers struct {
	customers CustomerStore
	billing   BillingStore
	accounts  AccountStore
	cache     BalanceCacheInvalidator
	logger    *slog.Logger
}

// registerBilling wires the admin billing endpoints: read/update the reserve-floor config, read balances,
// top-up, transfer between a customer's own balances, and flip the balance scope (guarded).
func registerBilling(api huma.API, customers CustomerStore, billingStore BillingStore, accounts AccountStore, cache BalanceCacheInvalidator, logger *slog.Logger) {
	if cache == nil {
		cache = noopBalanceCache{}
	}
	h := &billingHandlers{customers: customers, billing: billingStore, accounts: accounts, cache: cache, logger: logger}

	readErrs := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity}
	writeErrs := readErrs
	mutErrs := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity}

	register(api, huma.Operation{
		OperationID: "get-customer-billing", Method: http.MethodGet, Path: "/admin/customers/{id}/billing",
		Summary: "Get a customer's billing config", Tags: []string{"Billing"},
		Security: scopeSecurity(auth.ScopeAdminRead), Errors: readErrs,
	}, h.getBilling)
	register(api, huma.Operation{
		OperationID: "update-customer-billing", Method: http.MethodPatch, Path: "/admin/customers/{id}/billing",
		Summary: "Update a customer's billing config", Tags: []string{"Billing"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: writeErrs,
	}, h.updateBilling)
	register(api, huma.Operation{
		OperationID: "get-customer-balances", Method: http.MethodGet, Path: "/admin/customers/{id}/balances",
		Summary: "Get MT and MO balances (per owner, per direction)", Tags: []string{"Billing"},
		Security: scopeSecurity(auth.ScopeAdminRead), Errors: readErrs,
	}, h.getBalances)
	register(api, huma.Operation{
		OperationID: "topup-balance", Method: http.MethodPost, Path: "/admin/customers/{id}/billing/topup",
		Summary: "Top up a balance (credits, direction, optional account)", Tags: []string{"Billing"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: mutErrs,
	}, h.topup)
	register(api, huma.Operation{
		OperationID: "transfer-balance", Method: http.MethodPost, Path: "/admin/customers/{id}/billing/transfer",
		Summary: "Net-zero transfer between the customer's own balances", Tags: []string{"Billing"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: mutErrs,
	}, h.transfer)
	register(api, huma.Operation{
		OperationID: "change-balance-scope", Method: http.MethodPost, Path: "/admin/customers/{id}/billing/scope",
		Summary: "Change balance_scope (409 unless ALL balances are zero)", Tags: []string{"Billing"},
		Security: scopeSecurity(auth.ScopeAdminWrite), Errors: mutErrs,
	}, h.changeScope)
}

// --- DTOs (conform to api/openapi-admin.yaml: BillingCustomer, Balance, LedgerEntry) ---

type billingCustomerDTO struct {
	CustomerID                string  `json:"customer_id" format:"uuid"`
	BillingMode               string  `json:"billing_mode" enum:"prepaid,postpaid"`
	OverdraftEnabled          bool    `json:"overdraft_enabled"`
	OverdraftLimit            *int    `json:"overdraft_limit,omitempty" nullable:"true" minimum:"0"`
	CreditLimit               *int    `json:"credit_limit,omitempty" nullable:"true" minimum:"0"`
	CreditLimitIsHard         bool    `json:"credit_limit_is_hard"`
	ExternalBillingProviderID *string `json:"external_billing_provider_id,omitempty" nullable:"true" format:"uuid"`
	UpdatedAt                 string  `json:"updated_at,omitempty" format:"date-time"`
}

type billingCustomerUpdateBody struct {
	BillingMode               *string `json:"billing_mode,omitempty" enum:"prepaid,postpaid"`
	OverdraftEnabled          *bool   `json:"overdraft_enabled,omitempty"`
	OverdraftLimit            *int    `json:"overdraft_limit,omitempty" nullable:"true" minimum:"0"`
	CreditLimit               *int    `json:"credit_limit,omitempty" nullable:"true" minimum:"0"`
	CreditLimitIsHard         *bool   `json:"credit_limit_is_hard,omitempty"`
	ExternalBillingProviderID *string `json:"external_billing_provider_id,omitempty" nullable:"true" format:"uuid"`
}

type balanceDTO struct {
	OwnerType string `json:"owner_type" enum:"customer,smpp_account"`
	OwnerID   string `json:"owner_id" format:"uuid"`
	Direction string `json:"direction" enum:"mt,mo"`
	Credits   int    `json:"credits"`
	UpdatedAt string `json:"updated_at,omitempty" format:"date-time"`
}

type ledgerEntryDTO struct {
	ID           string  `json:"id" format:"uuid"`
	OwnerType    string  `json:"owner_type" enum:"customer,smpp_account"`
	OwnerID      string  `json:"owner_id" format:"uuid"`
	Direction    string  `json:"direction" enum:"mt,mo"`
	CustomerID   string  `json:"customer_id" format:"uuid"`
	AccountID    *string `json:"account_id,omitempty" nullable:"true" format:"uuid"`
	MessageID    *string `json:"message_id,omitempty" nullable:"true" format:"uuid"`
	EntryType    string  `json:"entry_type" enum:"reserve,capture,release,refund,topup,adjustment,mo_charge,transfer"`
	Credits      int     `json:"credits"`
	BalanceAfter int     `json:"balance_after"`
	Reference    *string `json:"reference,omitempty" nullable:"true"`
	CreatedAt    string  `json:"created_at" format:"date-time"`
}

type topupRequestBody struct {
	Credits        int     `json:"credits" minimum:"1" maximum:"1000000000"`
	Direction      string  `json:"direction" enum:"mt" doc:"Only the prepaid MT balance can be topped up (the MO meter is a postpaid counter)."`
	AccountID      *string `json:"account_id,omitempty" nullable:"true" format:"uuid"`
	Reference      *string `json:"reference,omitempty" nullable:"true"`
	IdempotencyKey string  `json:"idempotency_key" format:"uuid" doc:"Client key making the top-up retry-safe."`
}

type transferRequestBody struct {
	Credits        int    `json:"credits" minimum:"1" maximum:"1000000000"`
	Direction      string `json:"direction" enum:"mt" doc:"Only MT credit can be transferred (the MO meter is a postpaid counter)."`
	FromOwnerID    string `json:"from_owner_id" format:"uuid"`
	ToOwnerID      string `json:"to_owner_id" format:"uuid"`
	IdempotencyKey string `json:"idempotency_key" format:"uuid" doc:"Client key making the transfer retry-safe."`
}

type changeBalanceScopeBody struct {
	BalanceScope string `json:"balance_scope" enum:"customer,smpp_account"`
}

// I/O wrappers
type customerIDPathInput struct {
	ID string `path:"id" format:"uuid"`
}
type billingCustomerOutput struct{ Body billingCustomerDTO }
type updateBillingInput struct {
	ID   string `path:"id" format:"uuid"`
	Body billingCustomerUpdateBody
}
type balancesOutput struct {
	Body []balanceDTO
}
type ledgerEntryOutput struct{ Body ledgerEntryDTO }
type ledgerEntriesOutput struct {
	Body []ledgerEntryDTO
}
type topupInput struct {
	ID   string `path:"id" format:"uuid"`
	Body topupRequestBody
}
type transferInput struct {
	ID   string `path:"id" format:"uuid"`
	Body transferRequestBody
}
type changeScopeInput struct {
	ID   string `path:"id" format:"uuid"`
	Body changeBalanceScopeBody
}

// --- handlers ---

func (h *billingHandlers) getBilling(ctx context.Context, in *customerIDPathInput) (*billingCustomerOutput, error) {
	c, err := h.customer(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	return &billingCustomerOutput{Body: toBillingCustomerDTO(c)}, nil
}

func (h *billingHandlers) updateBilling(ctx context.Context, in *updateBillingInput) (*billingCustomerOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	providerID, err := parseIDPtr("external_billing_provider_id", in.Body.ExternalBillingProviderID)
	if err != nil {
		return nil, err
	}
	c, err := h.customers.Update(ctx, id, cp.CustomerPatch{
		BillingMode:               enumPtr[cp.BillingMode](in.Body.BillingMode),
		OverdraftEnabled:          in.Body.OverdraftEnabled,
		OverdraftLimit:            in.Body.OverdraftLimit,
		CreditLimit:               in.Body.CreditLimit,
		CreditLimitIsHard:         in.Body.CreditLimitIsHard,
		ExternalBillingProviderID: providerID,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &billingCustomerOutput{Body: toBillingCustomerDTO(c)}, nil
}

func (h *billingHandlers) getBalances(ctx context.Context, in *customerIDPathInput) (*balancesOutput, error) {
	c, err := h.customer(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	owners, err := h.ownersFor(ctx, c)
	if err != nil {
		return nil, err
	}
	rows, err := h.billing.Balances(ctx, owners)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := make([]balanceDTO, 0, len(rows))
	for _, r := range rows {
		out = append(out, balanceDTO{OwnerType: r.OwnerType, OwnerID: r.OwnerID.String(), Direction: r.Direction, Credits: r.Credits})
	}
	return &balancesOutput{Body: out}, nil
}

func (h *billingHandlers) topup(ctx context.Context, in *topupInput) (*ledgerEntryOutput, error) {
	c, err := h.customer(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	idem, err := parseUUID("idempotency_key", in.Body.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	owner, accountID, err := h.topupOwner(ctx, c, in.Body.AccountID)
	if err != nil {
		return nil, err
	}
	row, applied, err := h.billing.Topup(ctx, cp.LedgerEntry{
		OwnerType: owner.OwnerType, OwnerID: owner.OwnerID, Direction: in.Body.Direction,
		CustomerID: c.ID, AccountID: accountID, MessageID: &idem, EntryType: cp.EntryTopup,
		Credits: in.Body.Credits, Reference: in.Body.Reference,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !applied {
		// The idempotency key was already used: the money moved exactly once. Report it as a conflict rather
		// than a misleading 200 with an empty entry — the operator learns the top-up already happened.
		return nil, humaerr.Fail(errs.ErrIdempotencyConflict, "idempotency_key already used for a top-up")
	}
	h.invalidate(ctx, owner)
	return &ledgerEntryOutput{Body: toLedgerEntryDTO(row)}, nil
}

func (h *billingHandlers) transfer(ctx context.Context, in *transferInput) (*ledgerEntriesOutput, error) {
	c, err := h.customer(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	from, err := h.ownerRef(ctx, c, in.Body.FromOwnerID, "from_owner_id")
	if err != nil {
		return nil, err
	}
	to, err := h.ownerRef(ctx, c, in.Body.ToOwnerID, "to_owner_id")
	if err != nil {
		return nil, err
	}
	if from.OwnerID == to.OwnerID {
		return nil, humaerr.FailValidation("source and destination must differ",
			humaerr.FieldError{Field: "to_owner_id", Message: "must differ from from_owner_id"})
	}
	idem, err := parseUUID("idempotency_key", in.Body.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	debit := cp.LedgerEntry{OwnerType: from.OwnerType, OwnerID: from.OwnerID, Direction: in.Body.Direction, CustomerID: c.ID, AccountID: from.AccountID, MessageID: &idem, EntryType: cp.EntryTransfer, Credits: -in.Body.Credits, Reference: nil}
	credit := cp.LedgerEntry{OwnerType: to.OwnerType, OwnerID: to.OwnerID, Direction: in.Body.Direction, CustomerID: c.ID, AccountID: to.AccountID, MessageID: &idem, EntryType: cp.EntryTransfer, Credits: in.Body.Credits, Reference: nil}
	rows, applied, err := h.billing.Transfer(ctx, debit, credit, idem)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !applied {
		return nil, humaerr.Fail(errs.ErrIdempotencyConflict, "idempotency_key already used for a transfer")
	}
	h.invalidate(ctx, cp.BalanceOwner{OwnerType: from.OwnerType, OwnerID: from.OwnerID}, cp.BalanceOwner{OwnerType: to.OwnerType, OwnerID: to.OwnerID})
	out := make([]ledgerEntryDTO, 0, len(rows))
	for _, r := range rows {
		out = append(out, toLedgerEntryDTO(r))
	}
	return &ledgerEntriesOutput{Body: out}, nil
}

func (h *billingHandlers) changeScope(ctx context.Context, in *changeScopeInput) (*customerOutput, error) {
	c, err := h.customer(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	newScope := cp.BalanceScope(in.Body.BalanceScope)
	if !newScope.Valid() {
		return nil, humaerr.FailValidation("invalid balance_scope", humaerr.FieldError{Field: "balance_scope", Message: "must be customer or smpp_account"})
	}
	currentOwners, err := h.ownersFor(ctx, c)
	if err != nil {
		return nil, err
	}
	if err := h.billing.ChangeBalanceScope(ctx, c.ID, currentOwners, in.Body.BalanceScope); err != nil {
		return nil, humaerr.FromError(err)
	}
	h.invalidate(ctx, currentOwners...) // old owners must not serve a stale cache under a scope nothing reads
	updated, err := h.customers.Get(ctx, c.ID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerOutput{Body: toCustomerDTO(updated)}, nil
}

// --- helpers ---

// ownerResolved is a balance owner plus the account id to attribute on the ledger (nil for a customer owner).
type ownerResolved struct {
	OwnerType string
	OwnerID   uuid.UUID
	AccountID *uuid.UUID
}

func (h *billingHandlers) customer(ctx context.Context, rawID string) (cp.Customer, error) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		return cp.Customer{}, notFound("customer")
	}
	c, err := h.customers.Get(ctx, id)
	if err != nil {
		return cp.Customer{}, humaerr.FromError(err)
	}
	return c, nil
}

// ownersFor resolves the customer's balance owners under its current scope: one customer owner, or one per
// SMPP account (enumerated in full — the change-scope zero-check must see every account).
func (h *billingHandlers) ownersFor(ctx context.Context, c cp.Customer) ([]cp.BalanceOwner, error) {
	if c.BalanceScope != cp.BalanceScopeSMPPAccount {
		return []cp.BalanceOwner{{OwnerType: cp.OwnerTypeCustomer, OwnerID: c.ID}}, nil
	}
	var owners []cp.BalanceOwner
	after := uuid.Nil
	for {
		page, err := h.accounts.List(ctx, cp.AccountFilter{CustomerID: &c.ID, After: after, Limit: 500})
		if err != nil {
			return nil, humaerr.FromError(err)
		}
		for _, a := range page.Items {
			owners = append(owners, cp.BalanceOwner{OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: a.ID})
			after = a.ID
		}
		if !page.HasMore {
			break
		}
	}
	return owners, nil
}

// topupOwner resolves the owner a top-up targets, validating account_id against the customer's scope.
func (h *billingHandlers) topupOwner(ctx context.Context, c cp.Customer, accountID *string) (cp.BalanceOwner, *uuid.UUID, error) {
	if c.BalanceScope == cp.BalanceScopeSMPPAccount {
		if accountID == nil {
			return cp.BalanceOwner{}, nil, humaerr.FailValidation("account_id is required for an account-scoped customer",
				humaerr.FieldError{Field: "account_id", Message: "required when balance_scope is smpp_account"})
		}
		acct, err := h.customerAccount(ctx, c.ID, *accountID)
		if err != nil {
			return cp.BalanceOwner{}, nil, err
		}
		return cp.BalanceOwner{OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: acct}, &acct, nil
	}
	if accountID != nil {
		return cp.BalanceOwner{}, nil, humaerr.FailValidation("account_id is not allowed for a customer-scoped customer",
			humaerr.FieldError{Field: "account_id", Message: "must be absent when balance_scope is customer"})
	}
	return cp.BalanceOwner{OwnerType: cp.OwnerTypeCustomer, OwnerID: c.ID}, nil, nil
}

// ownerRef resolves a transfer owner id against the customer's scope: a customer-scoped customer's only owner
// is the customer id; an account-scoped customer's owners are its account ids.
func (h *billingHandlers) ownerRef(ctx context.Context, c cp.Customer, rawID, field string) (ownerResolved, error) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		return ownerResolved{}, humaerr.FailValidation("invalid owner id", humaerr.FieldError{Field: field, Message: "must be a UUID"})
	}
	if c.BalanceScope == cp.BalanceScopeSMPPAccount {
		acct, aerr := h.customerAccount(ctx, c.ID, rawID)
		if aerr != nil {
			return ownerResolved{}, humaerr.FailValidation("owner is not one of the customer's accounts", humaerr.FieldError{Field: field, Message: "must be an SMPP account of the customer"})
		}
		return ownerResolved{OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: acct, AccountID: &acct}, nil
	}
	if id != c.ID {
		return ownerResolved{}, humaerr.FailValidation("owner is not the customer", humaerr.FieldError{Field: field, Message: "must be the customer id for a customer-scoped customer"})
	}
	return ownerResolved{OwnerType: cp.OwnerTypeCustomer, OwnerID: c.ID}, nil
}

// customerAccount parses an account id and verifies it belongs to the customer.
func (h *billingHandlers) customerAccount(ctx context.Context, customerID uuid.UUID, rawID string) (uuid.UUID, error) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		return uuid.Nil, humaerr.Fail(errs.ErrValidation, "account does not belong to the customer")
	}
	a, err := h.accounts.Get(ctx, id)
	if err != nil || a.CustomerID != customerID {
		return uuid.Nil, humaerr.Fail(errs.ErrValidation, "account does not belong to the customer")
	}
	return id, nil
}

// invalidate best-effort deletes the MT and MO balance-cache keys of the given owners after a durable write.
func (h *billingHandlers) invalidate(ctx context.Context, owners ...cp.BalanceOwner) {
	keys := make([]string, 0, len(owners)*2)
	for _, o := range owners {
		keys = append(keys,
			billing.BalanceCacheKey(cp.BillingDirectionMT, o.OwnerType, o.OwnerID),
			billing.BalanceCacheKey(cp.BillingDirectionMO, o.OwnerType, o.OwnerID))
	}
	if err := h.cache.Del(ctx, keys...); err != nil {
		h.logger.WarnContext(ctx, "billing: balance-cache invalidation failed (TTL will self-heal)", "err", err)
	}
}

func parseUUID(field, raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, humaerr.FailValidation("invalid "+field, humaerr.FieldError{Field: field, Message: "must be a UUID"})
	}
	return id, nil
}

func toBillingCustomerDTO(c cp.Customer) billingCustomerDTO {
	mode := string(cp.BillingPrepaid)
	if c.BillingMode != nil && *c.BillingMode != "" {
		mode = string(*c.BillingMode)
	}
	return billingCustomerDTO{
		CustomerID:                c.ID.String(),
		BillingMode:               mode,
		OverdraftEnabled:          c.OverdraftEnabled,
		OverdraftLimit:            c.OverdraftLimit,
		CreditLimit:               c.CreditLimit,
		CreditLimitIsHard:         c.CreditLimitIsHard,
		ExternalBillingProviderID: idPtr(c.ExternalBillingProviderID),
		UpdatedAt:                 c.UpdatedAt.Format(timeRFC3339),
	}
}

func toLedgerEntryDTO(r cp.LedgerRow) ledgerEntryDTO {
	dto := ledgerEntryDTO{
		ID: r.ID.String(), OwnerType: r.OwnerType, OwnerID: r.OwnerID.String(), Direction: r.Direction,
		CustomerID: r.CustomerID.String(), AccountID: idPtr(r.AccountID), MessageID: idPtr(r.MessageID),
		EntryType: string(r.EntryType), Credits: r.Credits, BalanceAfter: r.BalanceAfter, Reference: r.Reference,
	}
	if !r.CreatedAt.IsZero() {
		dto.CreatedAt = r.CreatedAt.Format(timeRFC3339)
	}
	return dto
}

const timeRFC3339 = "2006-01-02T15:04:05.999999999Z07:00"
