package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/bindfailure"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

type bindFailureHandlers struct {
	log      BindFailureLog
	accounts AccountStore
}

func registerBindFailures(api huma.API, log BindFailureLog, accounts AccountStore) {
	h := &bindFailureHandlers{log: log, accounts: accounts}
	register(api, huma.Operation{
		OperationID: "list-account-bind-failures", Method: http.MethodGet, Path: "/admin/smpp-accounts/{id}/bind-failures",
		Summary: "Recent refused binds of this account (§6.14)", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.list)
}

type listBindFailuresInput struct {
	ID    string    `path:"id" format:"uuid"`
	Since time.Time `query:"since"`
}

type bindFailureDTO struct {
	At            time.Time `json:"at" format:"date-time"`
	RemoteIP      string    `json:"remote_ip"`
	BindType      string    `json:"bind_type" enum:"tx,rx,trx"`
	CommandStatus string    `json:"command_status" enum:"ESME_RINVPASWD,ESME_RBINDFAIL,ESME_RSYSERR"`
	Reason        string    `json:"reason" enum:"password_mismatch,credential_revoked,credential_disabled,account_inactive,smpp_channel_disabled,bind_type_not_allowed,max_sessions_exceeded,throttled,registry_unavailable"`
}

type listBindFailuresOutput struct {
	Body struct {
		Data []bindFailureDTO `json:"data" nullable:"false"`
	}
}

func (h *bindFailureHandlers) list(ctx context.Context, in *listBindFailuresInput) (*listBindFailuresOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	now := time.Now()
	since := in.Since
	if since.IsZero() {
		since = now.Add(-bindfailure.Retention)
	}
	if since.Before(now.Add(-bindfailure.Retention)) || since.After(now) {
		return nil, humaerr.FailValidation("since is out of range",
			humaerr.FieldError{Field: "since", Message: "must fall within the retention window, and not in the future"})
	}
	if _, err := h.accounts.Get(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	failures, err := h.log.List(ctx, id, since)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listBindFailuresOutput{}
	out.Body.Data = make([]bindFailureDTO, 0, len(failures))
	for _, f := range failures {
		out.Body.Data = append(out.Body.Data, bindFailureDTO{
			At: f.At, RemoteIP: f.RemoteIP, BindType: f.BindType, CommandStatus: f.CommandStatus, Reason: string(f.Reason),
		})
	}
	return out, nil
}
