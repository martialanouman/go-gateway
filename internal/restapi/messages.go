package restapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/idempotency"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/platform/uuidx"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// awaitTimeout bounds how long a concurrent submit of the same Idempotency-Key waits for the winning
// request to confirm its publish before giving up with a retriable 503. The reserve→finalize window is
// a single durable produce, so this is generous headroom, not a latency the happy path ever pays.
const awaitTimeout = 2 * time.Second

// idemOpTimeout bounds the post-publish Finalize (and the on-failure Release). These run on a
// cancellation-detached context so a client that hangs up right after its message is durably queued
// cannot leave the entry stuck "pending" — which would 503 every retry of that key until its 24 h TTL.
const idemOpTimeout = 2 * time.Second

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

// submitInput is the huma input for submit-messages. IdempotencyKey is the optional client-chosen key
// (api/openapi-public.yaml IdempotencyKey parameter); an empty value means no idempotency.
type submitInput struct {
	IdempotencyKey string `header:"Idempotency-Key" maxLength:"128" doc:"Client-chosen key; a repeat with the same key returns the original result (24 h window)."`
	Body           SubmitMessageRequest
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
func (s *server) submit(ctx context.Context, in *submitInput) (out *submitOutput, err error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return nil, humaerr.FromError(errs.ErrUnauthenticated)
	}

	ctx, span := s.deps.Tracer.Start(ctx, "rest.submit")
	defer span.End()
	// The ingress root span: unmarked, error-biased sampling drops it and every stage span below it is
	// left orphaned (step-181).
	defer func() { observability.RecordSpanError(span, err) }()

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

	accepted := AcceptedMessage{
		ID:         messageID.String(),
		TraceID:    traceID.String(),
		Status:     string(clickhouse.StatusAccepted),
		ClientRef:  in.Body.ClientRef,
		AcceptedAt: now,
	}

	// The Idempotency-Key is an opaque client token (net/http already trims surrounding header
	// whitespace); an empty value, or no store configured, means the M2 non-idempotent path.
	if in.IdempotencyKey != "" && s.deps.Idempotency != nil {
		return s.submitIdempotent(ctx, principal.AccountID.String(), in.IdempotencyKey, in.Body, env, accepted)
	}

	// Durability boundary (§6.7 / §7.3): the 202 is earned only once the record is durably written.
	// Ingestor.Accept encodes, produces durably and projects the accepted CDR row off the request
	// path — the same helper the SMPP submit_sm path uses, so both surfaces reach the pipeline
	// identically.
	if err := s.deps.Ingestor.Accept(ctx, env); err != nil {
		return nil, humaerr.FromError(err)
	}

	return &submitOutput{Body: accepted}, nil
}

// submitIdempotent runs the two-phase idempotent submit for a request carrying an Idempotency-Key.
// Reserve claims the slot atomically; the winner publishes and finalizes; a repeat replays the stored
// result; a concurrent in-flight repeat awaits the winner; a same-key request with a different body is
// a 409. The 202-earned-only-when-durable invariant holds: a result is replayed only after the winner's
// publish is confirmed (state "done"), never during the pending window.
func (s *server) submitIdempotent(ctx context.Context, accountID, idemKey string, body SubmitMessageRequest, env pipeline.InboundMT, accepted AcceptedMessage) (*submitOutput, error) {
	bodyHash, err := hashSubmitBody(body)
	if err != nil {
		return nil, humaerr.FromError(errs.ErrInternal)
	}
	response, err := json.Marshal(accepted)
	if err != nil {
		return nil, humaerr.FromError(errs.ErrInternal)
	}

	res, err := s.deps.Idempotency.Reserve(ctx, accountID, idemKey, bodyHash, response)
	if err != nil {
		// Redis is unreachable: we cannot honor the idempotency guarantee, so fail retriably rather than
		// risk publishing the same message twice on a client retry.
		return nil, humaerr.FromError(errs.ErrServiceUnavailable)
	}

	switch res.Outcome {
	case idempotency.Reserved:
		if err := s.deps.Ingestor.Accept(ctx, env); err != nil {
			// The message was not durably queued: release the slot so a retry can reserve afresh instead
			// of waiting out the 24 h window on a message that was never sent.
			s.releaseIdempotent(ctx, accountID, idemKey)
			return nil, humaerr.FromError(err)
		}
		// The message is durably queued — the 202 is earned. Finalize flips the entry to "done" so
		// retries replay it; it runs detached from the request ctx so a client that hangs up here cannot
		// freeze the entry "pending".
		s.finalizeIdempotent(ctx, accountID, idemKey)
		return &submitOutput{Body: accepted}, nil
	case idempotency.Replay:
		return replaySubmit(res.Response)
	case idempotency.Pending:
		stored, err := s.deps.Idempotency.Await(ctx, accountID, idemKey, awaitTimeout)
		if err != nil {
			// The winner is taking unusually long (or Redis blipped): retriable rather than block forever.
			return nil, humaerr.FromError(errs.ErrServiceUnavailable)
		}
		return replaySubmit(stored)
	case idempotency.Conflict:
		return nil, humaerr.FromError(errs.ErrIdempotencyConflict)
	default:
		return nil, humaerr.FromError(errs.ErrInternal)
	}
}

// finalizeIdempotent flips a reserved entry to "done" after a successful publish, detached from the
// request ctx (which cancels on client disconnect) with its own short deadline. A failure leaves the
// entry "pending" until its 24 h TTL — safe for the sent message (never a double publish), but it 503s
// that key's retries, so it is logged rather than swallowed.
func (s *server) finalizeIdempotent(ctx context.Context, accountID, idemKey string) {
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), idemOpTimeout)
	defer cancel()
	if err := s.deps.Idempotency.Finalize(bg, accountID, idemKey); err != nil {
		s.deps.Logger.WarnContext(ctx, "idempotency finalize failed; entry stays pending until its TTL",
			"error", err)
	}
}

// releaseIdempotent frees a reservation whose publish failed, detached from the request ctx so a
// cancelled request still clears the slot for the client's retry. A failure is logged, not surfaced:
// the caller already returns the publish error.
func (s *server) releaseIdempotent(ctx context.Context, accountID, idemKey string) {
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), idemOpTimeout)
	defer cancel()
	if err := s.deps.Idempotency.Release(bg, accountID, idemKey); err != nil {
		s.deps.Logger.WarnContext(ctx, "idempotency release failed; entry stays pending until its TTL",
			"error", err)
	}
}

// replaySubmit decodes a stored 202 body and returns it as the response. A replay intentionally returns
// the original result verbatim — including the winner's id, trace_id and accepted_at — so a retry that
// receives it is told exactly what the first request produced.
func replaySubmit(stored []byte) (*submitOutput, error) {
	var accepted AcceptedMessage
	if err := json.Unmarshal(stored, &accepted); err != nil {
		return nil, humaerr.FromError(errs.ErrInternal)
	}
	return &submitOutput{Body: accepted}, nil
}

// hashSubmitBody is the deterministic fingerprint of a submission body: a SHA-256 of its canonical JSON.
// It is a one-way digest (never the cleartext body) used only to detect a same-key replay with a
// changed body — so invariant (a) is preserved.
func hashSubmitBody(body SubmitMessageRequest) (string, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
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
