package adminapi

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// ErrSessionNotFound is a SessionDirectory's answer for a bind that is not live.
var ErrSessionNotFound = errors.New("session not found")

// LiveSession is one client (ESME) bind as the registry describes it.
type LiveSession struct {
	AccountID   uuid.UUID
	BindID      string
	SystemID    string
	PodID       string
	BindType    string
	RemoteAddr  string
	WindowSize  int
	ConnectedAt time.Time
}

// OperatorDisconnectReason labels an operator's close of one session, in the pod's log and the audit.
const OperatorDisconnectReason = "operator_disconnect"

type sessionDTO struct {
	ID              string     `json:"id" format:"uuid"`
	AccountID       *string    `json:"account_id,omitempty" format:"uuid" nullable:"true"`
	ConnectorID     *string    `json:"connector_id,omitempty" format:"uuid" nullable:"true"`
	BindID          string     `json:"bind_id"`
	BindType        string     `json:"bind_type" enum:"tx,rx,trx"`
	Direction       *string    `json:"direction,omitempty" enum:"user,smsc"`
	PodID           string     `json:"pod_id"`
	RemoteAddr      *string    `json:"remote_addr,omitempty" nullable:"true"`
	WindowSize      *int       `json:"window_size,omitempty"`
	ConnectedAt     time.Time  `json:"connected_at" format:"date-time"`
	LastEnquireLink *time.Time `json:"last_enquire_link,omitempty" format:"date-time" nullable:"true"`
}

func toSessionDTO(s LiveSession) sessionDTO {
	dto := sessionDTO{
		ID: s.BindID, AccountID: ptr(s.AccountID.String()), BindID: s.BindID, BindType: s.BindType,
		Direction: ptr("user"), PodID: s.PodID, WindowSize: ptr(s.WindowSize), ConnectedAt: s.ConnectedAt.UTC(),
	}
	if s.RemoteAddr != "" {
		dto.RemoteAddr = &s.RemoteAddr
	}
	return dto
}

func toSessionDTOs(sessions []LiveSession) []sessionDTO {
	out := make([]sessionDTO, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, toSessionDTO(s))
	}
	return out
}

type sessionPage struct {
	PageMeta
	Data []sessionDTO `json:"data"`
}

type sessionHandlers struct {
	sessions SessionDirectory
	accounts AccountStore
}

func registerSessions(api huma.API, sessions SessionDirectory, accounts AccountStore) {
	h := &sessionHandlers{sessions: sessions, accounts: accounts}

	register(api, huma.Operation{
		OperationID: "list-sessions", Method: http.MethodGet, Path: "/admin/sessions",
		Summary: "List live SMPP sessions", Tags: []string{"Sessions"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "disconnect-session", Method: http.MethodDelete, Path: "/admin/sessions/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Force-disconnect a session", Tags: []string{"Sessions"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.disconnect)

	register(api, huma.Operation{
		OperationID: "list-account-sessions", Method: http.MethodGet, Path: "/admin/smpp-accounts/{id}/sessions",
		Summary: "Live binds vs maxSessions for this account", Tags: []string{"SMPP Accounts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.listAccount)
}

type listSessionsInput struct {
	AccountID   string `query:"accountId" format:"uuid"`
	ConnectorID string `query:"connectorId" format:"uuid"`
	Cursor      string `query:"cursor"`
	Limit       int    `query:"limit" minimum:"1" maximum:"500" default:"50"`
}

type listSessionsOutput struct{ Body sessionPage }

func (h *sessionHandlers) list(ctx context.Context, in *listSessionsInput) (*listSessionsOutput, error) {
	if in.ConnectorID != "" {
		return nil, humaerr.FailValidation("connectorId is not served",
			humaerr.FieldError{Field: "connectorId", Message: "outbound connector binds are not in the session registry; read get-connector-status"})
	}
	var (
		sessions []LiveSession
		next     string
		err      error
	)
	if in.AccountID != "" {
		sessions, next, err = h.accountPage(ctx, in)
	} else {
		sessions, next, err = h.sessions.ListSessions(ctx, in.Cursor, in.Limit)
	}
	if err != nil {
		return nil, err
	}
	out := &listSessionsOutput{}
	out.Body.Data = toSessionDTOs(sessions)
	out.Body.NextCursor = cursorString(next)
	out.Body.HasMore = next != ""
	return out, nil
}

// accountPage pages one account's sessions in memory: there are at most max_sessions of them, so the
// registry returns them all.
func (h *sessionHandlers) accountPage(ctx context.Context, in *listSessionsInput) ([]LiveSession, string, error) {
	id, err := uuid.Parse(in.AccountID)
	if err != nil {
		return nil, "", humaerr.FailValidation("invalid accountId",
			humaerr.FieldError{Field: "accountId", Message: "must be a UUID"})
	}
	all, _, err := h.sessions.ListAccountSessions(ctx, id)
	if err != nil {
		return nil, "", humaerr.FromError(err)
	}
	start := sort.Search(len(all), func(i int) bool { return all[i].BindID > in.Cursor })
	page := all[start:]
	if len(page) > in.Limit {
		return page[:in.Limit], page[in.Limit-1].BindID, nil
	}
	return page, "", nil
}

type sessionIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *sessionHandlers) disconnect(ctx context.Context, in *sessionIDInput) (*struct{}, error) {
	err := h.sessions.DisconnectSession(ctx, in.ID, OperatorDisconnectReason)
	if errors.Is(err, ErrSessionNotFound) {
		return nil, notFound("session")
	}
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return nil, nil
}

type accountSessionsOutput struct {
	Body struct {
		MaxSessions int          `json:"max_sessions"`
		Active      int          `json:"active"`
		Sessions    []sessionDTO `json:"sessions" nullable:"false"`
	}
}

func (h *sessionHandlers) listAccount(ctx context.Context, in *accountIDInput) (*accountSessionsOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("smpp account")
	}
	account, err := h.accounts.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	sessions, active, err := h.sessions.ListAccountSessions(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &accountSessionsOutput{}
	out.Body.MaxSessions = account.MaxSessions
	out.Body.Active = active
	out.Body.Sessions = toSessionDTOs(sessions)
	return out, nil
}
