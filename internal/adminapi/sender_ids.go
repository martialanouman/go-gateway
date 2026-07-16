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

// senderIDDTO is the wire form of a SenderId (contract schema SenderId).
type senderIDDTO struct {
	ID         string     `json:"id" format:"uuid"`
	CustomerID string     `json:"customer_id" format:"uuid"`
	Address    string     `json:"address"`
	Status     string     `json:"status" enum:"pending_carrier_approval,active,disabled"`
	CreatedBy  *string    `json:"created_by,omitempty" format:"uuid" nullable:"true"`
	ApprovedAt *time.Time `json:"approved_at,omitempty" format:"date-time" nullable:"true"`
	CreatedAt  time.Time  `json:"created_at" format:"date-time"`
	UpdatedAt  time.Time  `json:"updated_at" format:"date-time"`
}

func toSenderIDDTO(s cp.SenderID) senderIDDTO {
	return senderIDDTO{
		ID:         idString(s.ID),
		CustomerID: idString(s.CustomerID),
		Address:    s.Address,
		Status:     string(s.Status),
		CreatedBy:  idPtr(s.CreatedBy),
		ApprovedAt: s.ApprovedAt,
		CreatedAt:  s.CreatedAt,
		UpdatedAt:  s.UpdatedAt,
	}
}

type senderIDCreateBody struct {
	Address string `json:"address" maxLength:"20"`
}

type senderIDUpdateBody struct {
	Status *string `json:"status,omitempty" enum:"pending_carrier_approval,active,disabled"`
}

type senderIDHandlers struct {
	senders   SenderIDStore
	customers CustomerStore
}

func registerSenderIDs(api huma.API, senders SenderIDStore, customers CustomerStore) {
	h := &senderIDHandlers{senders: senders, customers: customers}

	register(api, huma.Operation{
		OperationID: "list-sender-ids", Method: http.MethodGet, Path: "/admin/customers/{id}/sender-ids",
		Summary: "List a customer's sender IDs", Tags: []string{"Sender IDs"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-sender-id", Method: http.MethodPost, Path: "/admin/customers/{id}/sender-ids",
		DefaultStatus: http.StatusCreated,
		Summary:       "Register a sender ID (starts pending carrier approval)", Tags: []string{"Sender IDs"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusConflict},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-sender-id", Method: http.MethodPatch,
		Path:    "/admin/customers/{id}/sender-ids/{senderId}",
		Summary: "Update a sender ID's status", Tags: []string{"Sender IDs"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-sender-id", Method: http.MethodDelete,
		Path:          "/admin/customers/{id}/sender-ids/{senderId}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a sender ID", Tags: []string{"Sender IDs"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)
}

type customerScopedInput struct {
	ID string `path:"id" format:"uuid"`
}
type listSenderIDsOutput struct {
	Body []senderIDDTO
}

func (h *senderIDHandlers) list(ctx context.Context, in *customerScopedInput) (*listSenderIDsOutput, error) {
	customerID, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("customer")
	}
	// The list has no sender id to miss, so a 404 means the customer is unknown.
	if _, err := h.customers.Get(ctx, customerID); err != nil {
		return nil, humaerr.FromError(err)
	}
	senders, err := h.senders.ListByCustomer(ctx, customerID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listSenderIDsOutput{Body: make([]senderIDDTO, 0, len(senders))}
	for _, s := range senders {
		out.Body = append(out.Body, toSenderIDDTO(s))
	}
	return out, nil
}

type createSenderIDInput struct {
	ID   string `path:"id" format:"uuid"`
	Body senderIDCreateBody
}
type senderIDOutput struct {
	Body senderIDDTO
}

func (h *senderIDHandlers) create(ctx context.Context, in *createSenderIDInput) (*senderIDOutput, error) {
	customerID, err := uuid.Parse(in.ID)
	if err != nil {
		// An unknown customer surfaces as a foreign-key validation (422), matching the contract.
		return nil, humaerr.FailValidation("invalid customer id",
			humaerr.FieldError{Field: "id", Message: "must be a UUID"})
	}
	s, err := h.senders.Create(ctx, cp.NewSenderID{CustomerID: customerID, Address: in.Body.Address})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &senderIDOutput{Body: toSenderIDDTO(s)}, nil
}

type senderIDScopedInput struct {
	ID       string `path:"id" format:"uuid"`
	SenderID string `path:"senderId" format:"uuid"`
	Body     senderIDUpdateBody
}

func (h *senderIDHandlers) update(ctx context.Context, in *senderIDScopedInput) (*senderIDOutput, error) {
	customerID, senderID, err := parseCustomerAndSender(in.ID, in.SenderID)
	if err != nil {
		return nil, err
	}
	s, err := h.senders.Update(ctx, customerID, senderID, cp.SenderIDPatch{
		Status: enumPtr[cp.SenderIDStatus](in.Body.Status),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &senderIDOutput{Body: toSenderIDDTO(s)}, nil
}

type deleteSenderIDInput struct {
	ID       string `path:"id" format:"uuid"`
	SenderID string `path:"senderId" format:"uuid"`
}

func (h *senderIDHandlers) delete(ctx context.Context, in *deleteSenderIDInput) (*deleteOutput, error) {
	customerID, senderID, err := parseCustomerAndSender(in.ID, in.SenderID)
	if err != nil {
		return nil, err
	}
	if err := h.senders.Delete(ctx, customerID, senderID); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

func parseCustomerAndSender(customerIDStr, senderIDStr string) (uuid.UUID, uuid.UUID, error) {
	customerID, err := uuid.Parse(customerIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, notFound("sender id")
	}
	senderID, err := uuid.Parse(senderIDStr)
	if err != nil {
		return uuid.Nil, uuid.Nil, notFound("sender id")
	}
	return customerID, senderID, nil
}
