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

// customerGroupDTO is the wire form of a CustomerGroup (contract schema CustomerGroup). created_by
// is read-only and stays null until real operator auth lands (step-310).
type customerGroupDTO struct {
	ID          string    `json:"id" format:"uuid"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty" nullable:"true"`
	Status      string    `json:"status" enum:"active,archived"`
	CreatedBy   *string   `json:"created_by,omitempty" format:"uuid" nullable:"true"`
	CreatedAt   time.Time `json:"created_at" format:"date-time"`
	UpdatedAt   time.Time `json:"updated_at" format:"date-time"`
}

func toCustomerGroupDTO(g cp.CustomerGroup) customerGroupDTO {
	return customerGroupDTO{
		ID:          idString(g.ID),
		Name:        g.Name,
		Description: g.Description,
		Status:      string(g.Status),
		CreatedBy:   idPtr(g.CreatedBy),
		CreatedAt:   g.CreatedAt,
		UpdatedAt:   g.UpdatedAt,
	}
}

// customerGroupCreateBody is the contract schema CustomerGroupCreate: only name is required.
type customerGroupCreateBody struct {
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty" nullable:"true"`
}

// customerGroupUpdateBody is the contract schema CustomerGroupUpdate: every field optional.
type customerGroupUpdateBody struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty" nullable:"true"`
	Status      *string `json:"status,omitempty" enum:"active,archived"`
}

func (b customerGroupUpdateBody) toPatch() cp.CustomerGroupPatch {
	return cp.CustomerGroupPatch{
		Name:        b.Name,
		Description: b.Description,
		Status:      enumPtr[cp.CustomerGroupStatus](b.Status),
	}
}

type customerGroupHandlers struct {
	groups    CustomerGroupStore
	customers CustomerStore
}

func registerCustomerGroups(api huma.API, groups CustomerGroupStore, customers CustomerStore) {
	h := &customerGroupHandlers{groups: groups, customers: customers}

	register(api, huma.Operation{
		OperationID: "list-customer-groups", Method: http.MethodGet, Path: "/admin/customer-groups",
		Summary: "List customer groups", Tags: []string{"Customer Groups"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-customer-group", Method: http.MethodPost, Path: "/admin/customer-groups",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a customer group", Tags: []string{"Customer Groups"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusConflict,
			http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "get-customer-group", Method: http.MethodGet, Path: "/admin/customer-groups/{id}",
		Summary: "Get a customer group", Tags: []string{"Customer Groups"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusUnprocessableEntity},
	}, h.get)

	register(api, huma.Operation{
		OperationID: "update-customer-group", Method: http.MethodPatch, Path: "/admin/customer-groups/{id}",
		Summary: "Update a customer group", Tags: []string{"Customer Groups"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusConflict, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-customer-group", Method: http.MethodDelete, Path: "/admin/customer-groups/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a group (non-destructive — detaches customers)", Tags: []string{"Customer Groups"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusUnprocessableEntity},
	}, h.delete)

	register(api, huma.Operation{
		OperationID: "list-group-customers", Method: http.MethodGet,
		Path:    "/admin/customer-groups/{id}/customers",
		Summary: "List customers in a group", Tags: []string{"Customer Groups"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
			http.StatusUnprocessableEntity},
	}, h.listCustomers)
}

type listCustomerGroupsInput struct {
	Status string `query:"status" doc:"Filter by status."`
}

// listCustomerGroupsOutput is a bare array, not a page: the contract says so.
type listCustomerGroupsOutput struct{ Body []customerGroupDTO }

func (h *customerGroupHandlers) list(ctx context.Context, in *listCustomerGroupsInput) (*listCustomerGroupsOutput, error) {
	var filter cp.CustomerGroupFilter
	if in.Status != "" {
		// The shared Status query parameter is a free-form string in the contract, so the enum lives
		// here — as it does for list-customers.
		s := cp.CustomerGroupStatus(in.Status)
		if !s.Valid() {
			return nil, humaerr.FailValidation("invalid status",
				humaerr.FieldError{Field: "status", Message: "unknown customer group status"})
		}
		filter.Status = &s
	}

	groups, err := h.groups.List(ctx, filter)
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	out := &listCustomerGroupsOutput{Body: make([]customerGroupDTO, 0, len(groups))}
	for _, g := range groups {
		out.Body = append(out.Body, toCustomerGroupDTO(g))
	}
	return out, nil
}

type createCustomerGroupInput struct{ Body customerGroupCreateBody }
type customerGroupOutput struct{ Body customerGroupDTO }

func (h *customerGroupHandlers) create(ctx context.Context, in *createCustomerGroupInput) (*customerGroupOutput, error) {
	g, err := h.groups.Create(ctx, cp.NewCustomerGroup{Name: in.Body.Name, Description: in.Body.Description})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerGroupOutput{Body: toCustomerGroupDTO(g)}, nil
}

type customerGroupIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *customerGroupHandlers) get(ctx context.Context, in *customerGroupIDInput) (*customerGroupOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer group")
	}
	g, err := h.groups.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerGroupOutput{Body: toCustomerGroupDTO(g)}, nil
}

type updateCustomerGroupInput struct {
	ID   string `path:"id" format:"uuid"`
	Body customerGroupUpdateBody
}

func (h *customerGroupHandlers) update(ctx context.Context, in *updateCustomerGroupInput) (*customerGroupOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer group")
	}
	g, err := h.groups.Update(ctx, id, in.Body.toPatch())
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &customerGroupOutput{Body: toCustomerGroupDTO(g)}, nil
}

func (h *customerGroupHandlers) delete(ctx context.Context, in *customerGroupIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer group")
	}
	if err := h.groups.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

type listGroupCustomersInput struct {
	ID     string `path:"id" format:"uuid"`
	Cursor string `query:"cursor" doc:"Opaque page position from a previous page."`
	Limit  int    `query:"limit" minimum:"1" maximum:"500" default:"50" doc:"Page size."`
}

// listCustomers is the group-scoped view of list-customers?groupId=, and it resolves through the
// same filter — membership is read at query time, which is what keeps the answer right the moment
// after a customer changes group (§6.17). The group itself is read first, and only to honour the
// 404 the contract declares: without it an unknown group would answer 200 with an empty page.
func (h *customerGroupHandlers) listCustomers(ctx context.Context, in *listGroupCustomersInput) (*listCustomersOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer group")
	}
	if _, err := h.groups.Get(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}

	after, err := cp.DecodeCursor(cp.Cursor(in.Cursor))
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	page, err := h.customers.List(ctx, cp.CustomerFilter{GroupID: &id, After: after, Limit: in.Limit})
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	return customersPage(page), nil
}
