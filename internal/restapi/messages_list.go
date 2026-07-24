package restapi

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// listMessagesInput is the huma input for list-messages: the contract's query filters plus the
// opaque cursor and page limit. Every filter is optional — an omitted string is empty, an omitted
// time is the zero value — and huma validates status/direction against their enums and limit against
// its 1..200 range before the handler runs.
type listMessagesInput struct {
	Status    string    `query:"status" enum:"accepted,enroute,delivered,failed,expired,rejected,rerouted,cancelled" doc:"Filter by current lifecycle status."`
	Direction string    `query:"direction" enum:"mt,mo" doc:"Filter by direction."`
	FromDate  time.Time `query:"from_date" format:"date-time" doc:"Inclusive lower bound on submitted_at (RFC 3339)."`
	ToDate    time.Time `query:"to_date" format:"date-time" doc:"Exclusive upper bound on submitted_at (RFC 3339)."`
	Cursor    string    `query:"cursor" doc:"Opaque pagination cursor from a previous next_cursor."`
	Limit     int       `query:"limit" minimum:"1" maximum:"200" default:"50" doc:"Maximum messages to return."`
}

// MessagePage is a cursor-paginated page of messages (api/openapi-public.yaml MessagePage). Data is
// always a (possibly empty) array, never null. NextCursor is null on the last page.
type MessagePage struct {
	Data       []Message `json:"data"`
	NextCursor *string   `json:"next_cursor"`
	HasMore    bool      `json:"has_more"`
}

type listMessagesOutput struct {
	Body MessagePage
}

// listMessages returns a cursor-paginated page of the caller's own messages, newest first, read from
// the CDR and scoped to the principal's account. The customer/account scope always comes from the
// authenticated principal, never from the query, so a cursor or filter cannot widen a caller's view
// beyond its own messages. The body is never projected (invariant a): messageFromRow carries only
// the customer-facing status view.
func (s *server) listMessages(ctx context.Context, in *listMessagesInput) (*listMessagesOutput, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return nil, humaerr.FromError(errs.ErrUnauthenticated)
	}

	filter := clickhouse.CDRListFilter{}
	if in.Status != "" {
		st := clickhouse.Status(in.Status)
		filter.Status = &st
	}
	if in.Direction != "" {
		dir := clickhouse.Direction(in.Direction)
		filter.Direction = &dir
	}
	if !in.FromDate.IsZero() {
		from := in.FromDate
		filter.FromDate = &from
	}
	if !in.ToDate.IsZero() {
		to := in.ToDate
		filter.ToDate = &to
	}
	if in.Cursor != "" {
		key, err := decodeCursor(in.Cursor)
		if err != nil {
			return nil, humaerr.FromError(errs.ErrValidation)
		}
		filter.After = &key
	}

	// Fetch one extra row to learn whether a further page exists without a second query.
	rows, err := s.deps.CDRReader.List(ctx, principal.CustomerID, principal.AccountID, filter, in.Limit+1)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "list cdr", "account_id", principal.AccountID, "err", err)
		return nil, humaerr.FromError(errs.ErrInternal)
	}

	hasMore := len(rows) > in.Limit
	if hasMore {
		rows = rows[:in.Limit]
	}

	page := MessagePage{Data: make([]Message, 0, len(rows)), HasMore: hasMore}
	for _, row := range rows {
		page.Data = append(page.Data, messageFromRow(row))
	}
	if hasMore {
		last := rows[len(rows)-1]
		cursor := encodeCursor(clickhouse.CDRKey{SubmittedAt: last.SubmittedAt, MessageID: last.MessageID})
		page.NextCursor = &cursor
	}

	return &listMessagesOutput{Body: page}, nil
}

// cursorSep separates the two keyset fields inside the pre-base64 cursor payload. A message_id is a
// UUID (no separator char) and the timestamp is decimal, so '|' is unambiguous.
const cursorSep = "|"

// encodeCursor renders a keyset position as an opaque base64url token. The timestamp is encoded at
// millisecond precision to match the CDR's DateTime64(3) column, so decoding round-trips to exactly
// the stored value and pagination neither skips nor repeats a row.
func encodeCursor(k clickhouse.CDRKey) string {
	payload := strconv.FormatInt(k.SubmittedAt.UnixMilli(), 10) + cursorSep + k.MessageID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

// decodeCursor parses a token produced by encodeCursor. Any malformed input is an error, which the
// handler maps to 422: a cursor is opaque, so a client should only ever echo one back verbatim.
func decodeCursor(s string) (clickhouse.CDRKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return clickhouse.CDRKey{}, err
	}
	ts, id, ok := strings.Cut(string(raw), cursorSep)
	if !ok {
		return clickhouse.CDRKey{}, errors.New("cursor: missing separator")
	}
	ms, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return clickhouse.CDRKey{}, err
	}
	messageID, err := uuid.Parse(id)
	if err != nil {
		return clickhouse.CDRKey{}, err
	}
	return clickhouse.CDRKey{SubmittedAt: time.UnixMilli(ms).UTC(), MessageID: messageID}, nil
}
