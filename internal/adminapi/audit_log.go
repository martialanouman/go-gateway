package adminapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/platform/keyset"
)

// msisdnInTarget are the audited operations whose path ends in a subscriber number.
var msisdnInTarget = map[string]bool{
	"update-exact-route": true,
	"delete-exact-route": true,
}

// auditEntryDTO is the wire form of an audit row (contract schema AuditEntry). The strings are free on
// purpose: a replay row is not an HTTP request, and step-310 adds a third operator format.
type auditEntryDTO struct {
	ID          string     `json:"id" format:"uuid"`
	Operator    string     `json:"operator"`
	OperationID string     `json:"operation_id"`
	Method      string     `json:"method"`
	Target      string     `json:"target"`
	RequestID   *string    `json:"request_id,omitempty" nullable:"true"`
	Status      *int       `json:"status,omitempty" nullable:"true"`
	At          time.Time  `json:"at" format:"date-time"`
	FinishedAt  *time.Time `json:"finished_at,omitempty" format:"date-time" nullable:"true"`
}

type auditEntryPageDTO struct {
	Data []auditEntryDTO `json:"data"`
	PageMeta
}

func toAuditEntryDTO(e cp.AuditEntry, reveal bool) auditEntryDTO {
	target := e.Target
	if msisdnInTarget[e.OperationID] {
		i := strings.LastIndexByte(target, '/')
		target = target[:i+1] + maskMSISDN(target[i+1:], reveal)
	}
	return auditEntryDTO{
		ID: idString(e.ID), Operator: e.Operator, OperationID: e.OperationID, Method: e.Method, Target: target,
		RequestID: e.RequestID, Status: e.Status, At: e.At, FinishedAt: e.FinishedAt,
	}
}

type auditLogHandlers struct {
	store AuditLogStore
}

func registerAuditLog(api huma.API, store AuditLogStore) {
	h := &auditLogHandlers{store: store}
	register(api, huma.Operation{
		OperationID: "list-audit-log", Method: http.MethodGet, Path: "/admin/audit-log",
		Summary: "List the operator audit trail", Tags: []string{"Audit"},
		Security: scopeSecurity(auth.ScopeAuditRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)
}

type listAuditLogInput struct {
	Operator string    `query:"operator" doc:"Exact match on the recorded operator."`
	FromDate time.Time `query:"from_date" doc:"Inclusive lower bound on at."`
	ToDate   time.Time `query:"to_date" doc:"Exclusive upper bound on at."`
	Cursor   string    `query:"cursor" doc:"Opaque pagination cursor from a previous next_cursor."`
	Limit    int       `query:"limit" minimum:"1" maximum:"500" default:"50" doc:"Maximum rows to return."`
}

type listAuditLogOutput struct {
	Body auditEntryPageDTO
}

func (h *auditLogHandlers) list(ctx context.Context, in *listAuditLogInput) (*listAuditLogOutput, error) {
	filter := cp.AuditLogFilter{Operator: in.Operator}
	if !in.FromDate.IsZero() {
		filter.From = &in.FromDate
	}
	if !in.ToDate.IsZero() {
		filter.To = &in.ToDate
	}
	var after *cp.AuditLogKey
	if in.Cursor != "" {
		key, err := keyset.Decode(in.Cursor, keyset.Micro)
		if err != nil {
			return nil, humaerr.FailValidation("invalid cursor", humaerr.FieldError{Field: "cursor", Message: "malformed page cursor"})
		}
		after = &cp.AuditLogKey{At: key.At, ID: key.ID}
	}

	rows, err := h.store.List(ctx, filter, in.Limit+1, after)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	hasMore := len(rows) > in.Limit
	if hasMore {
		rows = rows[:in.Limit]
	}

	page := auditEntryPageDTO{Data: make([]auditEntryDTO, 0, len(rows)), PageMeta: PageMeta{HasMore: hasMore}}
	reveal := mayRevealMSISDN(ctx)
	for _, r := range rows {
		page.Data = append(page.Data, toAuditEntryDTO(r, reveal))
	}
	if hasMore {
		last := rows[len(rows)-1]
		cursor := keyset.Encode(keyset.Key{At: last.At, ID: last.ID}, keyset.Micro)
		page.NextCursor = &cursor
	}
	return &listAuditLogOutput{Body: page}, nil
}
