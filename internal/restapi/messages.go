package restapi

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/platform/uuidx"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// SubmitMessageRequest is the single-submission body (api/openapi-public.yaml SubmitMessageRequest).
// M2 serves single submissions only; a batch-shaped body fails validation and returns 422.
type SubmitMessageRequest struct {
	To                 string  `json:"to" pattern:"^\\+?[1-9][0-9]{6,14}$" doc:"Destination MSISDN in E.164."`
	From               string  `json:"from" maxLength:"20" doc:"Source address / sender ID."`
	Text               string  `json:"text" minLength:"1" maxLength:"2000" doc:"Message body."`
	Encoding           string  `json:"encoding,omitempty" enum:"auto,gsm7,ucs2,binary" default:"auto"`
	RegisteredDelivery *bool   `json:"registered_delivery,omitempty" doc:"Request a DLR (default true)."`
	ValidityPeriod     *string `json:"validity_period,omitempty" maxLength:"16" doc:"SMPP validity period (relative or absolute), max 16 chars."`
	Priority           int     `json:"priority,omitempty" minimum:"0" maximum:"3" default:"0"`
	ClientRef          *string `json:"client_ref,omitempty" maxLength:"128"`
	DataCoding         *int    `json:"data_coding,omitempty" minimum:"0" maximum:"255"`
}

// submitInput is the huma input for submit-messages.
type submitInput struct {
	Body SubmitMessageRequest
}

// AcceptedMessage is the 202 body (api/openapi-public.yaml AcceptedMessage).
type AcceptedMessage struct {
	ID         string    `json:"id" format:"uuid"`
	TraceID    string    `json:"trace_id" format:"uuid"`
	Status     string    `json:"status" enum:"accepted,enroute,delivered,failed,expired,rejected,rerouted,cancelled"`
	ClientRef  *string   `json:"client_ref"`
	AcceptedAt time.Time `json:"accepted_at" format:"date-time"`
}

type submitOutput struct {
	Body AcceptedMessage
}

// submit accepts a single message: it authenticates (via the middleware), mints the ids, opens the
// root span, publishes to mt.inbound (the durability boundary), and only then returns 202. The
// accepted CDR row is written off the request path by the worker pool (§1.10).
func (s *server) submit(ctx context.Context, in *submitInput) (*submitOutput, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return nil, humaerr.FromError(errs.ErrUnauthenticated)
	}

	ctx, span := s.deps.Tracer.Start(ctx, "rest.submit")
	defer span.End()

	now := s.now()
	messageID := uuidx.New()
	traceID := uuidx.New()

	env := pipeline.InboundMT{
		MessageID:          messageID,
		TraceID:            traceID,
		AccountID:          principal.AccountID,
		CustomerID:         principal.CustomerID,
		From:               in.Body.From,
		To:                 in.Body.To,
		Body:               msg.NewBodyString(in.Body.Text),
		Encoding:           orAuto(in.Body.Encoding),
		RegisteredDelivery: registeredDelivery(in.Body.RegisteredDelivery),
		ValidityPeriod:     in.Body.ValidityPeriod,
		Priority:           in.Body.Priority,
		ClientRef:          in.Body.ClientRef,
		DataCoding:         in.Body.DataCoding,
		SubmittedAt:        now,
	}

	rec, err := pipeline.EncodeInbound(env)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "encode mt.inbound", "message_id", messageID, "err", err)
		return nil, humaerr.FromError(errs.ErrInternal)
	}

	// Durability boundary (§6.7 / §7.3): the 202 is earned only once the record is durably written.
	if err := s.deps.Producer.Produce(ctx, rec); err != nil {
		s.deps.Logger.ErrorContext(ctx, "produce mt.inbound", "message_id", messageID, "err", err)
		return nil, humaerr.FromError(errs.ErrServiceUnavailable)
	}

	// 202 earned. The accepted CDR row is written asynchronously, never blocking this response.
	s.deps.Accepted.Enqueue(acceptedRow(env))

	return &submitOutput{Body: AcceptedMessage{
		ID:         messageID.String(),
		TraceID:    traceID.String(),
		Status:     string(clickhouse.StatusAccepted),
		ClientRef:  in.Body.ClientRef,
		AcceptedAt: now,
	}}, nil
}

// getMessageInput is the huma input for get-message.
type getMessageInput struct {
	ID string `path:"id" format:"uuid" doc:"Message ID (UUIDv7)."`
}

// Message is the customer-facing status view (api/openapi-public.yaml Message).
type Message struct {
	ID             string     `json:"id" format:"uuid"`
	TraceID        string     `json:"trace_id" format:"uuid"`
	Direction      string     `json:"direction" enum:"mt,mo"`
	To             string     `json:"to"`
	From           string     `json:"from"`
	Status         string     `json:"status" enum:"accepted,enroute,delivered,failed,expired,rejected,rerouted,cancelled"`
	SegmentCount   *int       `json:"segment_count"`
	Encoding       *string    `json:"encoding" enum:"gsm7,ucs2,binary"`
	ClientRef      *string    `json:"client_ref"`
	ErrorCode      *string    `json:"error_code"`
	CreditsCharged *int       `json:"credits_charged"`
	SubmittedAt    time.Time  `json:"submitted_at" format:"date-time"`
	DeliveredAt    *time.Time `json:"delivered_at"`
}

type getMessageOutput struct {
	Body Message
}

// getMessage reads the current status of a message from the CDR, scoped to the caller's account.
// A message that does not exist in the caller's scope is 404. The accepted row (§1.10) is written
// off the request path within a few tens of ms, so it closes the just-accepted 404 window in the
// common case; under saturation the row may be dropped and a just-accepted GET can 404 until the
// connector's enroute row lands.
func (s *server) getMessage(ctx context.Context, in *getMessageInput) (*getMessageOutput, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return nil, humaerr.FromError(errs.ErrUnauthenticated)
	}

	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, humaerr.FromError(errs.ErrValidation)
	}

	row, found, err := s.deps.CDRReader.Current(ctx, principal.CustomerID, principal.AccountID, id)
	if err != nil {
		s.deps.Logger.ErrorContext(ctx, "read cdr", "message_id", id, "err", err)
		return nil, humaerr.FromError(errs.ErrInternal)
	}
	if !found {
		return nil, humaerr.FromError(errs.ErrNotFound)
	}

	return &getMessageOutput{Body: messageFromRow(row)}, nil
}

// acceptedRow builds the pre-dispatch accepted CDR row from the inbound envelope. The destination is
// left as submitted here: the AcceptedWriter normalizes it off the request path (the phone parse is
// too heavy to run inline at the ingest rate), to the same canonical form the router stores, so a
// message spells its destination the same across all its lifecycle rows. The body is never included
// (invariant a).
func acceptedRow(env pipeline.InboundMT) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID:    env.MessageID,
		TraceID:      env.TraceID,
		AccountID:    env.AccountID,
		CustomerID:   env.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   env.From,
		DestAddr:     env.To,
		SubmittedAt:  env.SubmittedAt,
		Status:       clickhouse.StatusAccepted,
		SegmentCount: 1,
		Encoding:     clickhouse.EncodingOf(env.Encoding),
		Billed:       false,
	}
}

// messageFromRow projects a CDR row onto the customer-facing Message. client_ref is not stored in
// the CDR in M2, so it is null here (it is still echoed on the 202).
func messageFromRow(row clickhouse.CDRRow) Message {
	segments := int(row.SegmentCount)
	encoding := string(row.Encoding)
	out := Message{
		ID:           row.MessageID.String(),
		TraceID:      row.TraceID.String(),
		Direction:    string(row.Direction),
		To:           row.DestAddr,
		From:         row.SourceAddr,
		Status:       string(row.Status),
		SegmentCount: &segments,
		Encoding:     &encoding,
		ErrorCode:    row.ErrorCode,
		SubmittedAt:  row.SubmittedAt,
		DeliveredAt:  row.DeliveredAt,
	}
	if row.CreditsCharged != nil {
		credits := int(*row.CreditsCharged)
		out.CreditsCharged = &credits
	}
	return out
}

func orAuto(encoding string) string {
	if encoding == "" {
		return "auto"
	}
	return encoding
}

func registeredDelivery(v *bool) bool {
	if v == nil {
		return true // contract default
	}
	return *v
}
