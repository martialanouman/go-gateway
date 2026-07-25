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

// inboundNumberDTO is the wire form of an InboundNumber (contract schema InboundNumber). Required and
// non-null: id, address, number_type, country_code, status. mccmnc, connector_id and account_id are
// optional-and-nullable (account_id null = shared, keyword-resolved).
type inboundNumberDTO struct {
	ID          string     `json:"id" format:"uuid"`
	Address     string     `json:"address"`
	NumberType  string     `json:"number_type" enum:"shortcode,longcode,alphanumeric"`
	CountryCode string     `json:"country_code"`
	MCCMNC      *string    `json:"mccmnc,omitempty" nullable:"true"`
	ConnectorID *string    `json:"connector_id,omitempty" format:"uuid" nullable:"true"`
	AccountID   *string    `json:"account_id,omitempty" format:"uuid" nullable:"true"`
	Status      string     `json:"status" enum:"active,disabled"`
	CreatedAt   *time.Time `json:"created_at,omitempty" format:"date-time"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty" format:"date-time"`
}

func toInboundNumberDTO(n cp.InboundNumber) inboundNumberDTO {
	return inboundNumberDTO{
		ID:          idString(n.ID),
		Address:     n.Address,
		NumberType:  string(n.NumberType),
		CountryCode: n.CountryCode,
		MCCMNC:      n.MCCMNC,
		ConnectorID: idPtr(n.ConnectorID),
		AccountID:   idPtr(n.AccountID),
		Status:      string(n.Status),
		CreatedAt:   ptr(n.CreatedAt),
		UpdatedAt:   ptr(n.UpdatedAt),
	}
}

// inboundNumberCreateBody is the request body of create-inbound-number (contract InboundNumberCreate).
// status is not settable here (it takes the DDL default 'active').
type inboundNumberCreateBody struct {
	Address     string  `json:"address"`
	NumberType  string  `json:"number_type" enum:"shortcode,longcode,alphanumeric"`
	CountryCode string  `json:"country_code"`
	MCCMNC      *string `json:"mccmnc,omitempty" nullable:"true"`
	ConnectorID *string `json:"connector_id,omitempty" format:"uuid" nullable:"true"`
	AccountID   *string `json:"account_id,omitempty" format:"uuid" nullable:"true"`
}

// inboundNumberUpdateBody is the request body of update-inbound-number (contract InboundNumberUpdate).
// address, country_code and account_id are not patchable here (address/country_code are the immutable
// unique key; account_id is changed only through assign).
type inboundNumberUpdateBody struct {
	NumberType  *string `json:"number_type,omitempty" enum:"shortcode,longcode,alphanumeric"`
	MCCMNC      *string `json:"mccmnc,omitempty" nullable:"true"`
	ConnectorID *string `json:"connector_id,omitempty" format:"uuid" nullable:"true"`
	Status      *string `json:"status,omitempty" enum:"active,disabled"`
}

// assignInboundNumberBody is the request body of assign-inbound-number. account_id is required and
// nullable: the key must be present, and an explicit null clears the dedication (shared). A plain
// pointer with no omitempty keeps it required in the generated schema while allowing the null member.
type assignInboundNumberBody struct {
	AccountID *string `json:"account_id" format:"uuid" nullable:"true"`
}

type inboundNumberHandlers struct {
	store InboundNumberStore
}

func registerInboundNumbers(api huma.API, store InboundNumberStore) {
	h := &inboundNumberHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-inbound-numbers", Method: http.MethodGet, Path: "/admin/inbound-numbers",
		Summary: "List inbound numbers", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-inbound-number", Method: http.MethodPost, Path: "/admin/inbound-numbers",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create an inbound number", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusConflict},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-inbound-number", Method: http.MethodPatch, Path: "/admin/inbound-numbers/{id}",
		Summary: "Update an inbound number", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-inbound-number", Method: http.MethodDelete, Path: "/admin/inbound-numbers/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete an inbound number", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)

	register(api, huma.Operation{
		OperationID: "assign-inbound-number", Method: http.MethodPatch, Path: "/admin/inbound-numbers/{id}/assign",
		Summary:  "Assign to an account (dedicated) or clear (shared, keyword-resolved)",
		Tags:     []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.assign)
}

type listInboundNumbersOutput struct {
	Body []inboundNumberDTO
}

func (h *inboundNumberHandlers) list(ctx context.Context, _ *struct{}) (*listInboundNumbersOutput, error) {
	numbers, err := h.store.List(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listInboundNumbersOutput{Body: make([]inboundNumberDTO, 0, len(numbers))}
	for _, n := range numbers {
		out.Body = append(out.Body, toInboundNumberDTO(n))
	}
	return out, nil
}

type createInboundNumberInput struct{ Body inboundNumberCreateBody }
type inboundNumberOutput struct{ Body inboundNumberDTO }

func (h *inboundNumberHandlers) create(ctx context.Context, in *createInboundNumberInput) (*inboundNumberOutput, error) {
	connectorID, err := parseIDPtr("connector_id", in.Body.ConnectorID)
	if err != nil {
		return nil, err
	}
	accountID, err := parseIDPtr("account_id", in.Body.AccountID)
	if err != nil {
		return nil, err
	}
	n, err := h.store.Create(ctx, cp.NewInboundNumber{
		Address:     in.Body.Address,
		NumberType:  cp.NumberType(in.Body.NumberType),
		CountryCode: in.Body.CountryCode,
		MCCMNC:      in.Body.MCCMNC,
		ConnectorID: connectorID,
		AccountID:   accountID,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &inboundNumberOutput{Body: toInboundNumberDTO(n)}, nil
}

type inboundNumberIDInput struct {
	ID string `path:"id" format:"uuid"`
}

type updateInboundNumberInput struct {
	ID   string `path:"id" format:"uuid"`
	Body inboundNumberUpdateBody
}

func (h *inboundNumberHandlers) update(ctx context.Context, in *updateInboundNumberInput) (*inboundNumberOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("inbound number")
	}
	connectorID, err := parseIDPtr("connector_id", in.Body.ConnectorID)
	if err != nil {
		return nil, err
	}
	n, err := h.store.Update(ctx, id, cp.InboundNumberPatch{
		NumberType:  enumPtr[cp.NumberType](in.Body.NumberType),
		MCCMNC:      in.Body.MCCMNC,
		ConnectorID: connectorID,
		Status:      enumPtr[cp.InboundNumberStatus](in.Body.Status),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &inboundNumberOutput{Body: toInboundNumberDTO(n)}, nil
}

func (h *inboundNumberHandlers) delete(ctx context.Context, in *inboundNumberIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("inbound number")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

type assignInboundNumberInput struct {
	ID   string `path:"id" format:"uuid"`
	Body assignInboundNumberBody
}

func (h *inboundNumberHandlers) assign(ctx context.Context, in *assignInboundNumberInput) (*inboundNumberOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("inbound number")
	}
	accountID, err := parseIDPtr("account_id", in.Body.AccountID)
	if err != nil {
		return nil, err
	}
	n, err := h.store.Assign(ctx, id, accountID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &inboundNumberOutput{Body: toInboundNumberDTO(n)}, nil
}
