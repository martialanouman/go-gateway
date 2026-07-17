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

// accountDTO is the wire form of an SMPP account (contract schema SmppAccount). query_sm_enabled and
// cancel_sm_enabled are pointers because the contract does not require them; the pointers are always
// set, so the values still always appear.
type accountDTO struct {
	ID               string    `json:"id" format:"uuid"`
	CustomerID       string    `json:"customer_id" format:"uuid"`
	Name             string    `json:"name"`
	Status           string    `json:"status" enum:"active,suspended,closed"`
	SMPPEnabled      bool      `json:"smpp_enabled"`
	RESTEnabled      bool      `json:"rest_enabled"`
	SenderIDPolicy   string    `json:"sender_id_policy" enum:"strict,allow_unregistered_numeric,disabled"`
	QuerySMEnabled   *bool     `json:"query_sm_enabled,omitempty"`
	CancelSMEnabled  *bool     `json:"cancel_sm_enabled,omitempty"`
	AllowedBindTypes string    `json:"allowed_bind_types" enum:"tx,rx,trx"`
	MaxSessions      int       `json:"max_sessions" minimum:"0"`
	CreatedAt        time.Time `json:"created_at" format:"date-time"`
	UpdatedAt        time.Time `json:"updated_at" format:"date-time"`
}

func toAccountDTO(a cp.Account) accountDTO {
	querySM := a.QuerySMEnabled
	cancelSM := a.CancelSMEnabled
	return accountDTO{
		ID:               idString(a.ID),
		CustomerID:       idString(a.CustomerID),
		Name:             a.Name,
		Status:           string(a.Status),
		SMPPEnabled:      a.SMPPEnabled,
		RESTEnabled:      a.RESTEnabled,
		SenderIDPolicy:   string(a.SenderIDPolicy),
		QuerySMEnabled:   &querySM,
		CancelSMEnabled:  &cancelSM,
		AllowedBindTypes: string(a.AllowedBindTypes),
		MaxSessions:      a.MaxSessions,
		CreatedAt:        a.CreatedAt,
		UpdatedAt:        a.UpdatedAt,
	}
}

type accountCreateBody struct {
	CustomerID       string  `json:"customer_id" format:"uuid"`
	Name             string  `json:"name"`
	SMPPEnabled      *bool   `json:"smpp_enabled,omitempty"`
	RESTEnabled      *bool   `json:"rest_enabled,omitempty"`
	SenderIDPolicy   *string `json:"sender_id_policy,omitempty" enum:"strict,allow_unregistered_numeric,disabled"`
	QuerySMEnabled   *bool   `json:"query_sm_enabled,omitempty"`
	CancelSMEnabled  *bool   `json:"cancel_sm_enabled,omitempty"`
	AllowedBindTypes *string `json:"allowed_bind_types,omitempty" enum:"tx,rx,trx"`
	MaxSessions      *int    `json:"max_sessions,omitempty" minimum:"0"`
}

func (b accountCreateBody) toNew() (cp.NewAccount, error) {
	customerID, err := uuid.Parse(b.CustomerID)
	if err != nil {
		return cp.NewAccount{}, humaerr.FailValidation("invalid customer_id",
			humaerr.FieldError{Field: "customer_id", Message: "must be a UUID"})
	}
	return cp.NewAccount{
		CustomerID:       customerID,
		Name:             b.Name,
		SMPPEnabled:      b.SMPPEnabled,
		RESTEnabled:      b.RESTEnabled,
		SenderIDPolicy:   enumPtr[cp.SenderIDPolicy](b.SenderIDPolicy),
		QuerySMEnabled:   b.QuerySMEnabled,
		CancelSMEnabled:  b.CancelSMEnabled,
		AllowedBindTypes: enumPtr[cp.BindType](b.AllowedBindTypes),
		MaxSessions:      b.MaxSessions,
	}, nil
}

type accountUpdateBody struct {
	Name   *string `json:"name,omitempty"`
	Status *string `json:"status,omitempty" enum:"active,suspended,closed"`
}

type channelsBody struct {
	SMPPEnabled bool `json:"smpp_enabled"`
	RESTEnabled bool `json:"rest_enabled"`
}

type sessionLimitsBody struct {
	MaxSessions      int    `json:"max_sessions" minimum:"0"`
	AllowedBindTypes string `json:"allowed_bind_types" enum:"tx,rx,trx"`
}

type accountPage struct {
	PageMeta
	Data []accountDTO `json:"data"`
}

type accountHandlers struct {
	store AccountStore
}

func registerAccounts(api huma.API, store AccountStore) {
	h := &accountHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-smpp-accounts", Method: http.MethodGet, Path: "/admin/smpp-accounts",
		Summary: "List SMPP accounts", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-smpp-account", Method: http.MethodPost, Path: "/admin/smpp-accounts",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create an SMPP account", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusConflict},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "get-smpp-account", Method: http.MethodGet, Path: "/admin/smpp-accounts/{id}",
		Summary: "Get an SMPP account", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.get)

	register(api, huma.Operation{
		OperationID: "update-smpp-account", Method: http.MethodPatch, Path: "/admin/smpp-accounts/{id}",
		Summary: "Update an SMPP account", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-smpp-account", Method: http.MethodDelete, Path: "/admin/smpp-accounts/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete an SMPP account", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.delete)

	register(api, huma.Operation{
		OperationID: "set-account-channels", Method: http.MethodPatch, Path: "/admin/smpp-accounts/{id}/channels",
		Summary: "Enable or disable an account's channels", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.setChannels)

	register(api, huma.Operation{
		OperationID: "set-account-session-limits", Method: http.MethodPatch, Path: "/admin/smpp-accounts/{id}/session-limits",
		Summary: "Set an account's session limits", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.setSessionLimits)
}

type listAccountsInput struct {
	CustomerID string `query:"customerId" format:"uuid"`
	GroupID    string `query:"groupId" format:"uuid"`
	Status     string `query:"status"`
	Cursor     string `query:"cursor"`
	Limit      int    `query:"limit" minimum:"1" maximum:"500" default:"50"`
}

type listAccountsOutput struct{ Body accountPage }

func (h *accountHandlers) list(ctx context.Context, in *listAccountsInput) (*listAccountsOutput, error) {
	filter := cp.AccountFilter{Limit: in.Limit}
	if in.CustomerID != "" {
		id, err := uuid.Parse(in.CustomerID)
		if err != nil {
			return nil, humaerr.FailValidation("invalid customerId",
				humaerr.FieldError{Field: "customerId", Message: "must be a UUID"})
		}
		filter.CustomerID = &id
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
		s := cp.AccountStatus(in.Status)
		if !s.Valid() {
			return nil, humaerr.FailValidation("invalid status",
				humaerr.FieldError{Field: "status", Message: "unknown account status"})
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
	out := &listAccountsOutput{}
	out.Body.NextCursor = cursorString(string(page.NextCursor))
	out.Body.HasMore = page.HasMore
	out.Body.Data = make([]accountDTO, 0, len(page.Items))
	for _, a := range page.Items {
		out.Body.Data = append(out.Body.Data, toAccountDTO(a))
	}
	return out, nil
}

type createAccountInput struct{ Body accountCreateBody }
type accountOutput struct{ Body accountDTO }

func (h *accountHandlers) create(ctx context.Context, in *createAccountInput) (*accountOutput, error) {
	newAccount, err := in.Body.toNew()
	if err != nil {
		return nil, err
	}
	a, err := h.store.Create(ctx, newAccount)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &accountOutput{Body: toAccountDTO(a)}, nil
}

type accountIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *accountHandlers) get(ctx context.Context, in *accountIDInput) (*accountOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	a, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &accountOutput{Body: toAccountDTO(a)}, nil
}

type updateAccountInput struct {
	ID   string `path:"id" format:"uuid"`
	Body accountUpdateBody
}

func (h *accountHandlers) update(ctx context.Context, in *updateAccountInput) (*accountOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	a, err := h.store.Update(ctx, id, cp.AccountPatch{
		Name:   in.Body.Name,
		Status: enumPtr[cp.AccountStatus](in.Body.Status),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &accountOutput{Body: toAccountDTO(a)}, nil
}

func (h *accountHandlers) delete(ctx context.Context, in *accountIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

type setChannelsInput struct {
	ID   string `path:"id" format:"uuid"`
	Body channelsBody
}

func (h *accountHandlers) setChannels(ctx context.Context, in *setChannelsInput) (*accountOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	// The schema's channel CHECK also guards this, but a pre-check names the field for a cleaner 422.
	if !in.Body.SMPPEnabled && !in.Body.RESTEnabled {
		return nil, humaerr.FailValidation("at least one channel must remain enabled",
			humaerr.FieldError{Field: "smpp_enabled", Message: "smpp_enabled and rest_enabled cannot both be false"})
	}
	a, err := h.store.SetChannels(ctx, id, in.Body.SMPPEnabled, in.Body.RESTEnabled)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &accountOutput{Body: toAccountDTO(a)}, nil
}

type setSessionLimitsInput struct {
	ID   string `path:"id" format:"uuid"`
	Body sessionLimitsBody
}

func (h *accountHandlers) setSessionLimits(ctx context.Context, in *setSessionLimitsInput) (*accountOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	a, err := h.store.SetSessionLimits(ctx, id, in.Body.MaxSessions, cp.BindType(in.Body.AllowedBindTypes))
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &accountOutput{Body: toAccountDTO(a)}, nil
}
