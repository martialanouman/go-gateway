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
	"github.com/martialanouman/go-gateway/internal/platform/keyset"
)

// messageSummaryDTO is the wire form of a CDR/MO summary (contract schema MessageSummary). For an
// unrouted MO — which has no account yet — account_id, customer_id and trace_id are the nil UUID: the
// contract makes them required (non-null), and the nil UUID is a present, valid value, so the sentinel
// is confined to this serialization layer and never reaches the unrouted_mo store.
type messageSummaryDTO struct {
	MessageID          string     `json:"message_id" format:"uuid"`
	TraceID            string     `json:"trace_id" format:"uuid"`
	AccountID          string     `json:"account_id" format:"uuid"`
	CustomerID         string     `json:"customer_id" format:"uuid"`
	Direction          string     `json:"direction" enum:"mt,mo"`
	SourceAddr         string     `json:"source_addr"`
	DestAddr           string     `json:"dest_addr"`
	OriginalSourceAddr *string    `json:"original_source_addr,omitempty" nullable:"true"`
	ConnectorID        *string    `json:"connector_id,omitempty" format:"uuid" nullable:"true"`
	RouteID            *string    `json:"route_id,omitempty" format:"uuid" nullable:"true"`
	Status             string     `json:"status" enum:"accepted,enroute,delivered,failed,expired,rejected,rerouted,cancelled"`
	ErrorCode          *string    `json:"error_code,omitempty" nullable:"true"`
	SegmentCount       int        `json:"segment_count"`
	Encoding           *string    `json:"encoding,omitempty" enum:"gsm7,ucs2,binary" nullable:"true"`
	SubmittedAt        time.Time  `json:"submitted_at" format:"date-time"`
	DeliveredAt        *time.Time `json:"delivered_at,omitempty" format:"date-time" nullable:"true"`
	LatencyMs          *int       `json:"latency_ms,omitempty" nullable:"true"`
	Billed             bool       `json:"billed,omitempty"`
	CreditsCharged     *int       `json:"credits_charged,omitempty" nullable:"true"`
}

// toUnroutedSummaryDTO projects an unrouted MO onto a MessageSummary. An unrouted MO reached no
// account, so its status is 'rejected' and the routing failure reason travels in error_code; the
// remaining delivery fields are unset.
func toUnroutedSummaryDTO(u cp.UnroutedMO, reveal bool) messageSummaryDTO {
	reason := string(u.Reason)
	encoding := u.Encoding
	// Unrouted MO: the subscriber is the source; the destination is the operator's inbound number.
	source, dest := maskAddresses("mo", u.SourceAddr, u.DestAddr, reveal)
	return messageSummaryDTO{
		MessageID:    idString(u.ID),
		TraceID:      idString(uuid.Nil),
		AccountID:    idString(uuid.Nil),
		CustomerID:   idString(uuid.Nil),
		Direction:    "mo",
		SourceAddr:   source,
		DestAddr:     dest,
		ConnectorID:  idPtr(u.ConnectorID),
		Status:       "rejected",
		ErrorCode:    &reason,
		SegmentCount: u.SegmentCount,
		Encoding:     &encoding,
		SubmittedAt:  u.ReceivedAt,
		Billed:       false,
	}
}

// unroutedMOPageDTO is a cursor-paginated page of unrouted MO (contract MessageSummaryPage). Data is
// always a (possibly empty) array; the embedded PageMeta carries next_cursor (null on the last page)
// and has_more, matching the contract's allOf[PageMeta, {data}].
type unroutedMOPageDTO struct {
	Data []messageSummaryDTO `json:"data"`
	PageMeta
}

type unroutedMOHandlers struct {
	store UnroutedMOStore
}

func registerUnroutedMO(api huma.API, store UnroutedMOStore) {
	h := &unroutedMOHandlers{store: store}
	register(api, huma.Operation{
		OperationID: "list-unrouted-mo", Method: http.MethodGet, Path: "/admin/mo/unrouted",
		Summary: "List unrouted MO messages", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnprocessableEntity},
	}, h.list)
}

type listUnroutedMOInput struct {
	Cursor string `query:"cursor" doc:"Opaque pagination cursor from a previous next_cursor."`
	Limit  int    `query:"limit" minimum:"1" maximum:"200" default:"50" doc:"Maximum rows to return."`
}

type listUnroutedMOOutput struct {
	Body unroutedMOPageDTO
}

func (h *unroutedMOHandlers) list(ctx context.Context, in *listUnroutedMOInput) (*listUnroutedMOOutput, error) {
	var after *cp.UnroutedMOKey
	if in.Cursor != "" {
		key, err := keyset.Decode(in.Cursor, keyset.Micro)
		if err != nil {
			return nil, humaerr.FailValidation("invalid cursor", humaerr.FieldError{Field: "cursor", Message: "malformed page cursor"})
		}
		after = &cp.UnroutedMOKey{ReceivedAt: key.At, ID: key.ID}
	}

	// Fetch one extra row to learn whether a further page exists without a second query.
	rows, err := h.store.List(ctx, in.Limit+1, after)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	hasMore := len(rows) > in.Limit
	if hasMore {
		rows = rows[:in.Limit]
	}

	page := unroutedMOPageDTO{
		Data:     make([]messageSummaryDTO, 0, len(rows)),
		PageMeta: PageMeta{HasMore: hasMore},
	}
	reveal := mayRevealMSISDN(ctx)
	for _, r := range rows {
		page.Data = append(page.Data, toUnroutedSummaryDTO(r, reveal))
	}
	if hasMore {
		last := rows[len(rows)-1]
		cursor := keyset.Encode(keyset.Key{At: last.ReceivedAt, ID: last.ID}, keyset.Micro)
		page.NextCursor = &cursor
	}
	return &listUnroutedMOOutput{Body: page}, nil
}
