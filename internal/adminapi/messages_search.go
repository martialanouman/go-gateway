package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// SearchStore reads the CDR for search-messages. Declared consumer-side.
type SearchStore interface {
	Search(ctx context.Context, f clickhouse.CDRSearchFilter, limit int) ([]clickhouse.CDRRow, error)
}

// searchMaxWindow bounds a single search. The CDR is partitioned by day and retained 90 days, so a
// 31-day ceiling covers the operational question ("what happened last month") while keeping the scan
// to a third of the store.
//
// It is a guardrail, not a product setting: in configuration it would be widened away the first time
// a search returned 422, which is precisely when it is doing its job.
const searchMaxWindow = 31 * 24 * time.Hour

// searchMaxGroupCustomers caps the group expansion. A customer group is flat organisational
// segmentation (§6.17), so a real one is far smaller; the cap exists so a mis-sized group cannot
// build an unbounded IN list.
const searchMaxGroupCustomers = 500

type searchMessagesInput struct {
	TraceID    string    `query:"traceId" format:"uuid" doc:"Find the message carrying this trace id."`
	AccountID  string    `query:"accountId" format:"uuid" doc:"Restrict to one SMPP account."`
	CustomerID string    `query:"customerId" format:"uuid" doc:"Restrict to one customer."`
	GroupID    string    `query:"groupId" format:"uuid" doc:"Restrict to the customers currently in this group."`
	Status     string    `query:"status" enum:"accepted,enroute,delivered,failed,expired,rejected,rerouted,cancelled" doc:"Filter by current lifecycle status."`
	Direction  string    `query:"direction" enum:"mt,mo" doc:"Filter by direction."`
	MSISDN     string    `query:"msisdn" doc:"Exact E.164 match on either address."`
	FromDate   time.Time `query:"from_date" required:"true" format:"date-time" doc:"Inclusive lower bound on submitted_at (RFC 3339)."`
	ToDate     time.Time `query:"to_date" required:"true" format:"date-time" doc:"Exclusive upper bound on submitted_at (RFC 3339)."`
	Cursor     string    `query:"cursor" doc:"Opaque pagination cursor from a previous next_cursor."`
	Limit      int       `query:"limit" minimum:"1" maximum:"200" default:"50" doc:"Maximum messages to return."`
}

type messageSummaryPageDTO struct {
	PageMeta
	Data []messageSummaryDTO `json:"data"`
}

type searchMessagesOutput struct{ Body messageSummaryPageDTO }

func registerMessageSearch(api huma.API, store SearchStore, customers CustomerStore) {
	h := &searchHandler{store: store, customers: customers}
	register(api, huma.Operation{
		OperationID: "search-messages",
		Method:      http.MethodGet,
		Path:        "/admin/messages/search",
		Summary:     "Search CDR (by traceId, account, customer, group, status, ...)",
		Tags:        []string{"Messages"},
		Security:    scopeSecurity(auth.ScopeAdminRead),
		Errors:      []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.search)
}

type searchHandler struct {
	store     SearchStore
	customers CustomerStore
}

// search answers one page of the CDR.
//
// Every predicate combines with AND, so an added filter can only narrow the result. The window is
// validated first because it is the one bound that makes the read affordable at all; the rest of the
// input is then normalised (an MSISDN to E.164, a cursor to a keyset position) so a malformed value
// is a named 422 rather than a query that quietly matches nothing.
func (h *searchHandler) search(ctx context.Context, in *searchMessagesInput) (*searchMessagesOutput, error) {
	filter, empty, err := buildCDRSearchFilter(ctx, searchPredicates{
		TraceID:    in.TraceID,
		AccountID:  in.AccountID,
		CustomerID: in.CustomerID,
		GroupID:    in.GroupID,
		Status:     in.Status,
		Direction:  in.Direction,
		MSISDN:     in.MSISDN,
		FromDate:   in.FromDate,
		ToDate:     in.ToDate,
		Cursor:     in.Cursor,
	}, h.customers)
	if err != nil {
		return nil, err
	}
	if empty {
		// The predicates intersect to nothing (a customer outside the requested group). Answering an
		// empty page without querying is both correct and the cheapest possible read.
		return &searchMessagesOutput{Body: messageSummaryPageDTO{Data: []messageSummaryDTO{}}}, nil
	}

	// One extra row tells us whether a further page exists, without a second query.
	rows, err := h.store.Search(ctx, filter, in.Limit+1)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	hasMore := len(rows) > in.Limit
	if hasMore {
		rows = rows[:in.Limit]
	}

	reveal := mayRevealMSISDN(ctx)
	page := messageSummaryPageDTO{PageMeta: PageMeta{HasMore: hasMore}, Data: exportRowsProjection(rows, reveal)}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		page.NextCursor = ptr(clickhouse.EncodeCDRCursor(
			clickhouse.CDRKey{SubmittedAt: last.SubmittedAt, MessageID: last.MessageID}))
	}
	return &searchMessagesOutput{Body: page}, nil
}

// searchPredicates are the filters BOTH search-messages and create-message-export accept. They are
// one type on purpose: the two endpoints must agree on what a window, a group or an MSISDN means, and
// a second copy of the rules would drift the day one of them gains a filter.
type searchPredicates struct {
	TraceID, AccountID, CustomerID, GroupID string
	Status, Direction, MSISDN               string
	FromDate, ToDate                        time.Time
	// Cursor is search-only: an export streams every page itself.
	Cursor string
}

// buildCDRSearchFilter validates the predicates and turns them into a store filter. empty reports
// that they intersect to nothing, which is an empty result rather than an error — the request is
// well-formed, it just cannot match anything.
//
// The window is validated first because it is the one bound that makes the read affordable at all;
// the rest is then normalised (an MSISDN to E.164, a cursor to a keyset position) so a malformed
// value is a named 422 rather than a query that quietly matches nothing.
func buildCDRSearchFilter(ctx context.Context, p searchPredicates, customers CustomerStore) (clickhouse.CDRSearchFilter, bool, error) {
	var zero clickhouse.CDRSearchFilter
	if !p.ToDate.After(p.FromDate) {
		return zero, false, humaerr.FailValidation("invalid window",
			humaerr.FieldError{Field: "to_date", Message: "must be after from_date"})
	}
	if p.ToDate.Sub(p.FromDate) > searchMaxWindow {
		return zero, false, humaerr.FailValidation("window too wide",
			humaerr.FieldError{Field: "from_date", Message: "the window must not exceed 31 days"})
	}

	filter := clickhouse.CDRSearchFilter{FromDate: p.FromDate, ToDate: p.ToDate}

	if p.MSISDN != "" {
		// Normalised, then compared for equality: the CDR stores E.164, so a spaced or +-prefixed input
		// would otherwise match nothing at all and read as "this subscriber has no traffic".
		normalised, err := e164.Normalize(p.MSISDN)
		if err != nil {
			return zero, false, humaerr.FailValidation("invalid msisdn",
				humaerr.FieldError{Field: "msisdn", Message: "must be an E.164 number"})
		}
		filter.MSISDN = &normalised
	}
	if p.Cursor != "" {
		key, err := clickhouse.DecodeCDRCursor(p.Cursor)
		if err != nil {
			return zero, false, humaerr.FromError(err)
		}
		filter.After = &key
	}
	if p.TraceID != "" {
		id := uuid.MustParse(p.TraceID) // huma rejects a malformed uuid before the handler runs
		filter.TraceID = &id
	}
	if p.AccountID != "" {
		id := uuid.MustParse(p.AccountID)
		filter.AccountID = &id
	}
	if p.Status != "" {
		st := clickhouse.Status(p.Status)
		filter.Status = &st
	}
	if p.Direction != "" {
		dir := clickhouse.Direction(p.Direction)
		filter.Direction = &dir
	}

	tenants, empty, err := tenantIDs(ctx, p, customers)
	if err != nil {
		return zero, false, err
	}
	if empty {
		return zero, true, nil
	}
	filter.CustomerIDs = tenants
	return filter, false, nil
}

// tenantIDs resolves the customer scope of a search: the customer id, the group's current members, or
// their intersection. empty reports that the two cannot intersect, which is an empty page rather than
// an error — the request is well-formed, it just cannot match anything.
//
// The group is expanded HERE because the CDR carries no group_id: a customer's group is mutable, so a
// column frozen at send time would answer a different question ("who was in the group then") from the
// one an operator asks ("whose messages are these now").
func tenantIDs(ctx context.Context, p searchPredicates, customers CustomerStore) (ids []uuid.UUID, empty bool, err error) {
	var customerID *uuid.UUID
	if p.CustomerID != "" {
		id := uuid.MustParse(p.CustomerID)
		customerID = &id
	}
	if p.GroupID == "" {
		if customerID != nil {
			return []uuid.UUID{*customerID}, false, nil
		}
		return nil, false, nil
	}

	if customers == nil {
		return nil, false, humaerr.FailValidation("group unavailable",
			humaerr.FieldError{Field: "groupId", Message: "customer groups cannot be resolved here"})
	}
	groupID := uuid.MustParse(p.GroupID)
	// One over the cap: enough to detect an oversized group without listing all of it.
	page, err := customers.List(ctx, cp.CustomerFilter{GroupID: &groupID, Limit: searchMaxGroupCustomers + 1})
	if err != nil {
		return nil, false, humaerr.FromError(err)
	}
	if len(page.Items) > searchMaxGroupCustomers {
		return nil, false, humaerr.FailValidation("group too large",
			humaerr.FieldError{Field: "groupId", Message: "the group holds more than 500 customers; narrow the search with customerId"})
	}
	if len(page.Items) == 0 {
		return nil, true, nil
	}

	members := make([]uuid.UUID, 0, len(page.Items))
	for _, c := range page.Items {
		members = append(members, c.ID)
	}
	if customerID == nil {
		return members, false, nil
	}
	// Both given: intersect. A filter must never widen a result set, so a customer outside the group
	// matches nothing at all.
	for _, id := range members {
		if id == *customerID {
			return []uuid.UUID{id}, false, nil
		}
	}
	return nil, true, nil
}

// toMessageSummaryDTO projects one CDR row. It reads no content column, and the subscriber address is
// masked unless the caller holds the reveal scope — the same rule as the trace and, later, the export.
func toMessageSummaryDTO(row clickhouse.CDRRow, reveal bool) messageSummaryDTO {
	source, dest := maskAddresses(string(row.Direction), row.SourceAddr, row.DestAddr, reveal)
	out := messageSummaryDTO{
		MessageID:    row.MessageID.String(),
		TraceID:      row.TraceID.String(),
		AccountID:    row.AccountID.String(),
		CustomerID:   row.CustomerID.String(),
		Direction:    string(row.Direction),
		SourceAddr:   source,
		DestAddr:     dest,
		ConnectorID:  idPtr(row.ConnectorID),
		RouteID:      idPtr(row.RouteID),
		Status:       string(row.Status),
		ErrorCode:    row.ErrorCode,
		SegmentCount: int(row.SegmentCount),
		SubmittedAt:  row.SubmittedAt,
		DeliveredAt:  row.DeliveredAt,
		Billed:       row.Billed,
	}
	if row.OriginalSourceAddr != nil {
		// The original sender is masked on the same rule as the current one: on an MO it is the
		// subscriber, and a rewritten MT sender is a sender ID either way.
		original, _ := maskAddresses(string(row.Direction), *row.OriginalSourceAddr, row.DestAddr, reveal)
		out.OriginalSourceAddr = &original
	}
	if row.Encoding != "" {
		enc := string(row.Encoding)
		out.Encoding = &enc
	}
	if row.LatencyMs != nil {
		ms := int(*row.LatencyMs)
		out.LatencyMs = &ms
	}
	if row.CreditsCharged != nil {
		credits := int(*row.CreditsCharged)
		out.CreditsCharged = &credits
	}
	return out
}
