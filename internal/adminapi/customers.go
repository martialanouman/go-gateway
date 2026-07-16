package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// customerDTO is the wire form of a Customer (contract schema Customer). The struct tags encode the
// contract: a plain field is required and non-null; `omitempty nullable:"true"` is optional and
// nullable; `omitempty` alone is optional and non-null. overdraft_enabled is a *bool because the
// contract does not require it — the pointer is always set, so the value still always appears.
type customerDTO struct {
	ID                   string    `json:"id" format:"uuid"`
	Name                 string    `json:"name"`
	Status               string    `json:"status" enum:"active,suspended,closed"`
	GroupID              *string   `json:"group_id,omitempty" format:"uuid" nullable:"true"`
	RatePlanID           *string   `json:"rate_plan_id,omitempty" format:"uuid" nullable:"true"`
	BillingEnabled       bool      `json:"billing_enabled"`
	BillingMode          *string   `json:"billing_mode,omitempty" enum:"prepaid,postpaid" nullable:"true"`
	OverdraftEnabled     *bool     `json:"overdraft_enabled,omitempty"`
	OverdraftLimit       *int      `json:"overdraft_limit,omitempty" minimum:"0" nullable:"true"`
	BalanceScope         string    `json:"balance_scope" enum:"customer,smpp_account"`
	MoBillingFloor       *int      `json:"mo_billing_floor,omitempty" nullable:"true"`
	ContentStorage       string    `json:"content_storage" enum:"inherit,off,stored_plaintext,stored_encrypted"`
	ContentRetentionDays *int      `json:"content_retention_days,omitempty" minimum:"0" nullable:"true"`
	ContentKeyID         *string   `json:"content_key_id,omitempty" format:"uuid" nullable:"true"`
	CreatedAt            time.Time `json:"created_at" format:"date-time"`
	UpdatedAt            time.Time `json:"updated_at" format:"date-time"`
}

func toCustomerDTO(c cp.Customer) customerDTO {
	overdraft := c.OverdraftEnabled
	return customerDTO{
		ID:                   idString(c.ID),
		Name:                 c.Name,
		Status:               string(c.Status),
		GroupID:              idPtr(c.GroupID),
		RatePlanID:           idPtr(c.RatePlanID),
		BillingEnabled:       c.BillingEnabled,
		BillingMode:          (*string)(c.BillingMode),
		OverdraftEnabled:     &overdraft,
		OverdraftLimit:       c.OverdraftLimit,
		BalanceScope:         string(c.BalanceScope),
		MoBillingFloor:       c.MoBillingFloor,
		ContentStorage:       string(c.ContentStorage),
		ContentRetentionDays: c.ContentRetentionDays,
		ContentKeyID:         idPtr(c.ContentKeyID),
		CreatedAt:            c.CreatedAt,
		UpdatedAt:            c.UpdatedAt,
	}
}

// customerCreateBody is the contract schema CustomerCreate: only name is required.
type customerCreateBody struct {
	Name                 string  `json:"name"`
	GroupID              *string `json:"group_id,omitempty" format:"uuid" nullable:"true"`
	RatePlanID           *string `json:"rate_plan_id,omitempty" format:"uuid" nullable:"true"`
	BillingEnabled       *bool   `json:"billing_enabled,omitempty"`
	BillingMode          *string `json:"billing_mode,omitempty" enum:"prepaid,postpaid" nullable:"true"`
	OverdraftEnabled     *bool   `json:"overdraft_enabled,omitempty"`
	OverdraftLimit       *int    `json:"overdraft_limit,omitempty" minimum:"0" nullable:"true"`
	BalanceScope         *string `json:"balance_scope,omitempty" enum:"customer,smpp_account"`
	MoBillingFloor       *int    `json:"mo_billing_floor,omitempty" nullable:"true"`
	ContentStorage       *string `json:"content_storage,omitempty" enum:"inherit,off,stored_plaintext,stored_encrypted"`
	ContentRetentionDays *int    `json:"content_retention_days,omitempty" minimum:"0" nullable:"true"`
}

func (b customerCreateBody) toNew() (cp.NewCustomer, error) {
	groupID, err := parseIDPtr("group_id", b.GroupID)
	if err != nil {
		return cp.NewCustomer{}, err
	}
	ratePlanID, err := parseIDPtr("rate_plan_id", b.RatePlanID)
	if err != nil {
		return cp.NewCustomer{}, err
	}
	return cp.NewCustomer{
		Name:                 b.Name,
		GroupID:              groupID,
		RatePlanID:           ratePlanID,
		BillingEnabled:       deref(b.BillingEnabled),
		BillingMode:          enumPtr[cp.BillingMode](b.BillingMode),
		OverdraftEnabled:     deref(b.OverdraftEnabled),
		OverdraftLimit:       b.OverdraftLimit,
		BalanceScope:         enumPtr[cp.BalanceScope](b.BalanceScope),
		MoBillingFloor:       b.MoBillingFloor,
		ContentStorage:       enumPtr[cp.ContentStorage](b.ContentStorage),
		ContentRetentionDays: b.ContentRetentionDays,
	}, nil
}

// customerUpdateBody is the contract schema CustomerUpdate: all fields optional; group_id is absent
// (group membership has its own endpoint).
type customerUpdateBody struct {
	Name                 *string `json:"name,omitempty"`
	Status               *string `json:"status,omitempty" enum:"active,suspended,closed"`
	RatePlanID           *string `json:"rate_plan_id,omitempty" format:"uuid" nullable:"true"`
	BillingEnabled       *bool   `json:"billing_enabled,omitempty"`
	BillingMode          *string `json:"billing_mode,omitempty" enum:"prepaid,postpaid" nullable:"true"`
	OverdraftEnabled     *bool   `json:"overdraft_enabled,omitempty"`
	OverdraftLimit       *int    `json:"overdraft_limit,omitempty" minimum:"0" nullable:"true"`
	MoBillingFloor       *int    `json:"mo_billing_floor,omitempty" nullable:"true"`
	ContentStorage       *string `json:"content_storage,omitempty" enum:"inherit,off,stored_plaintext,stored_encrypted"`
	ContentRetentionDays *int    `json:"content_retention_days,omitempty" minimum:"0" nullable:"true"`
}

func (b customerUpdateBody) toPatch() (cp.CustomerPatch, error) {
	ratePlanID, err := parseIDPtr("rate_plan_id", b.RatePlanID)
	if err != nil {
		return cp.CustomerPatch{}, err
	}
	return cp.CustomerPatch{
		Name:                 b.Name,
		Status:               enumPtr[cp.CustomerStatus](b.Status),
		RatePlanID:           ratePlanID,
		BillingEnabled:       b.BillingEnabled,
		BillingMode:          enumPtr[cp.BillingMode](b.BillingMode),
		OverdraftEnabled:     b.OverdraftEnabled,
		OverdraftLimit:       b.OverdraftLimit,
		MoBillingFloor:       b.MoBillingFloor,
		ContentStorage:       enumPtr[cp.ContentStorage](b.ContentStorage),
		ContentRetentionDays: b.ContentRetentionDays,
	}, nil
}

// customerPage mirrors CustomerPage: allOf[PageMeta, {data}]. Huma flattens the embedded PageMeta
// into one object schema, which is exactly what the contract's allOf resolves to.
type customerPage struct {
	PageMeta
	Data []customerDTO `json:"data"`
}

// customerHandlers holds the store the customer operations use.
type customerHandlers struct {
	store CustomerStore
}

func registerCustomers(api huma.API, store CustomerStore) {
	h := &customerHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-customers", Method: http.MethodGet, Path: "/admin/customers",
		Summary: "List customers", Tags: []string{"Customers"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-customer", Method: http.MethodPost, Path: "/admin/customers",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a customer", Tags: []string{"Customers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "get-customer", Method: http.MethodGet, Path: "/admin/customers/{id}",
		Summary: "Get a customer", Tags: []string{"Customers"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.get)

	register(api, huma.Operation{
		OperationID: "update-customer", Method: http.MethodPatch, Path: "/admin/customers/{id}",
		Summary: "Update a customer", Tags: []string{"Customers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-customer", Method: http.MethodDelete, Path: "/admin/customers/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a customer", Tags: []string{"Customers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict},
	}, h.delete)

	register(api, huma.Operation{
		OperationID: "suspend-customer", Method: http.MethodPost, Path: "/admin/customers/{id}/suspend",
		Summary: "Suspend a customer (cascades to its accounts)", Tags: []string{"Customers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.suspend)
}

type listCustomersInput struct {
	GroupID string `query:"groupId" format:"uuid" doc:"Filter by customer group."`
	Status  string `query:"status" doc:"Filter by status."`
	Cursor  string `query:"cursor" doc:"Opaque page position from a previous page."`
	Limit   int    `query:"limit" minimum:"1" maximum:"500" default:"50" doc:"Page size."`
}

type listCustomersOutput struct{ Body customerPage }

func (h *customerHandlers) list(ctx context.Context, in *listCustomersInput) (*listCustomersOutput, error) {
	filter := cp.CustomerFilter{Limit: in.Limit}
	if filter.Limit == 0 {
		filter.Limit = 50
	}

	if in.GroupID != "" {
		id, err := uuid.Parse(in.GroupID)
		if err != nil {
			return nil, humaerr.FailValidation("invalid groupId",
				humaerr.FieldError{Field: "groupId", Message: "must be a UUID"})
		}
		filter.GroupID = &id
	}
	if in.Status != "" {
		s := cp.CustomerStatus(in.Status)
		if !s.Valid() {
			return nil, humaerr.FailValidation("invalid status",
				humaerr.FieldError{Field: "status", Message: "unknown customer status"})
		}
		filter.Status = &s
	}
	after, err := cp.DecodeCursor(cp.Cursor(in.Cursor))
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	filter.After = after

	page, err := h.store.List(ctx, filter)
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	out := &listCustomersOutput{}
	out.Body.NextCursor = cursorString(string(page.NextCursor))
	out.Body.HasMore = page.HasMore
	out.Body.Data = make([]customerDTO, 0, len(page.Items))
	for _, c := range page.Items {
		out.Body.Data = append(out.Body.Data, toCustomerDTO(c))
	}
	return out, nil
}

type createCustomerInput struct{ Body customerCreateBody }
type customerOutput struct{ Body customerDTO }

func (h *customerHandlers) create(ctx context.Context, in *createCustomerInput) (*customerOutput, error) {
	newCustomer, err := in.Body.toNew()
	if err != nil {
		return nil, err
	}
	c, err := h.store.Create(ctx, newCustomer)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerOutput{Body: toCustomerDTO(c)}, nil
}

type customerIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *customerHandlers) get(ctx context.Context, in *customerIDInput) (*customerOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	c, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerOutput{Body: toCustomerDTO(c)}, nil
}

type updateCustomerInput struct {
	ID   string `path:"id" format:"uuid"`
	Body customerUpdateBody
}

func (h *customerHandlers) update(ctx context.Context, in *updateCustomerInput) (*customerOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	patch, err := in.Body.toPatch()
	if err != nil {
		return nil, err
	}
	c, err := h.store.Update(ctx, id, patch)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerOutput{Body: toCustomerDTO(c)}, nil
}

type deleteOutput struct{}

func (h *customerHandlers) delete(ctx context.Context, in *customerIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

func (h *customerHandlers) suspend(ctx context.Context, in *customerIDInput) (*customerOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	c, err := h.store.Suspend(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerOutput{Body: toCustomerDTO(c)}, nil
}

// deref returns the pointed-to bool, or false when nil.
func deref(p *bool) bool { return p != nil && *p }
