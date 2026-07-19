package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// The WIRE codec is the ONE audited place where a message body is revealed (invariant a). The body
// travels in the Kafka record VALUE — the durable data plane, which legitimately carries it — and
// NEVER in a record header or anywhere loggable. Every producer reveals here and every consumer
// re-wraps the bytes into msg.Body immediately, so the plaintext exists only as []byte inside these
// functions and inside the transport, never as a stray string a log or span could pick up.

// inboundWire is the JSON body of an mt.inbound record. Body is the revealed plaintext, encoded as
// base64 by encoding/json (a []byte field), so binary (UCS-2) content survives intact.
type inboundWire struct {
	MessageID          uuid.UUID `json:"message_id"`
	TraceID            uuid.UUID `json:"trace_id"`
	AccountID          uuid.UUID `json:"account_id"`
	CustomerID         uuid.UUID `json:"customer_id"`
	From               string    `json:"from"`
	To                 string    `json:"to"`
	Body               []byte    `json:"body"`
	Encoding           string    `json:"encoding"`
	ESMClass           uint8     `json:"esm_class,omitempty"`
	RegisteredDelivery bool      `json:"registered_delivery"`
	ValidityPeriod     *string   `json:"validity_period,omitempty"`
	Priority           int       `json:"priority"`
	ClientRef          *string   `json:"client_ref,omitempty"`
	DataCoding         *int      `json:"data_coding,omitempty"`
	SubmittedAt        time.Time `json:"submitted_at"`
}

// routedWire is the JSON body of an mt.routed record.
type routedWire struct {
	MessageID          uuid.UUID  `json:"message_id"`
	TraceID            uuid.UUID  `json:"trace_id"`
	AccountID          uuid.UUID  `json:"account_id"`
	CustomerID         uuid.UUID  `json:"customer_id"`
	From               string     `json:"from"`
	To                 string     `json:"to"`
	Body               []byte     `json:"body"`
	Encoding           string     `json:"encoding"`
	RegisteredDelivery bool       `json:"registered_delivery"`
	ValidityPeriod     *string    `json:"validity_period,omitempty"`
	DataCoding         *int       `json:"data_coding,omitempty"`
	ConnectorID        uuid.UUID  `json:"connector_id"`
	RouteID            *uuid.UUID `json:"route_id,omitempty"`
	SegmentCount       int        `json:"segment_count"`
	SubmittedAt        time.Time  `json:"submitted_at"`
}

// EncodeInbound builds the mt.inbound record for env, keyed by account so an account's submissions
// keep their partition order (§1.6). The headers carry ids only.
func EncodeInbound(env InboundMT) (kafka.Record, error) {
	value, err := json.Marshal(inboundWire{
		MessageID:          env.MessageID,
		TraceID:            env.TraceID,
		AccountID:          env.AccountID,
		CustomerID:         env.CustomerID,
		From:               env.From,
		To:                 env.To,
		Body:               env.Body.Reveal(), // audited: body -> durable value, never a header
		Encoding:           env.Encoding,
		ESMClass:           env.ESMClass,
		RegisteredDelivery: env.RegisteredDelivery,
		ValidityPeriod:     env.ValidityPeriod,
		Priority:           env.Priority,
		ClientRef:          env.ClientRef,
		DataCoding:         env.DataCoding,
		SubmittedAt:        env.SubmittedAt,
	})
	if err != nil {
		return kafka.Record{}, fmt.Errorf("pipeline: encode mt.inbound: %w", err)
	}
	key := env.AccountID
	return kafka.Record{
		Topic:   kafka.TopicMTInbound,
		Key:     key[:],
		Value:   value,
		Headers: idHeaders(env.MessageID, env.TraceID, env.AccountID, env.CustomerID),
	}, nil
}

// DecodeInbound parses an mt.inbound record, re-wrapping the body into msg.Body immediately so no
// plaintext string escapes downstream.
func DecodeInbound(rec kafka.Record) (InboundMT, error) {
	var w inboundWire
	if err := json.Unmarshal(rec.Value, &w); err != nil {
		return InboundMT{}, fmt.Errorf("pipeline: decode mt.inbound: %w", err)
	}
	return InboundMT{
		MessageID:          w.MessageID,
		TraceID:            w.TraceID,
		AccountID:          w.AccountID,
		CustomerID:         w.CustomerID,
		From:               w.From,
		To:                 w.To,
		Body:               msg.NewBody(w.Body),
		Encoding:           w.Encoding,
		ESMClass:           w.ESMClass,
		RegisteredDelivery: w.RegisteredDelivery,
		ValidityPeriod:     w.ValidityPeriod,
		Priority:           w.Priority,
		ClientRef:          w.ClientRef,
		DataCoding:         w.DataCoding,
		SubmittedAt:        w.SubmittedAt,
	}, nil
}

// EncodeRouted builds the mt.routed record for env, keyed by the logical message id so every
// segment reaches the same connector bind in order (§7.3).
func EncodeRouted(env RoutedMT) (kafka.Record, error) {
	value, err := json.Marshal(routedWire{
		MessageID:          env.MessageID,
		TraceID:            env.TraceID,
		AccountID:          env.AccountID,
		CustomerID:         env.CustomerID,
		From:               env.From,
		To:                 env.To,
		Body:               env.Body.Reveal(), // audited: body -> durable value, never a header
		Encoding:           env.Encoding,
		RegisteredDelivery: env.RegisteredDelivery,
		ValidityPeriod:     env.ValidityPeriod,
		DataCoding:         env.DataCoding,
		ConnectorID:        env.ConnectorID,
		RouteID:            env.RouteID,
		SegmentCount:       env.SegmentCount,
		SubmittedAt:        env.SubmittedAt,
	})
	if err != nil {
		return kafka.Record{}, fmt.Errorf("pipeline: encode mt.routed: %w", err)
	}
	key := env.MessageID
	return kafka.Record{
		Topic:   kafka.TopicMTRouted,
		Key:     key[:],
		Value:   value,
		Headers: idHeaders(env.MessageID, env.TraceID, env.AccountID, env.CustomerID),
	}, nil
}

// DecodeRouted parses an mt.routed record, re-wrapping the body into msg.Body immediately.
func DecodeRouted(rec kafka.Record) (RoutedMT, error) {
	var w routedWire
	if err := json.Unmarshal(rec.Value, &w); err != nil {
		return RoutedMT{}, fmt.Errorf("pipeline: decode mt.routed: %w", err)
	}
	return RoutedMT{
		MessageID:          w.MessageID,
		TraceID:            w.TraceID,
		AccountID:          w.AccountID,
		CustomerID:         w.CustomerID,
		From:               w.From,
		To:                 w.To,
		Body:               msg.NewBody(w.Body),
		Encoding:           w.Encoding,
		RegisteredDelivery: w.RegisteredDelivery,
		ValidityPeriod:     w.ValidityPeriod,
		DataCoding:         w.DataCoding,
		ConnectorID:        w.ConnectorID,
		RouteID:            w.RouteID,
		SegmentCount:       w.SegmentCount,
		SubmittedAt:        w.SubmittedAt,
	}, nil
}

// idHeaders builds the identifier headers every pipeline record carries (§7.3). Identifiers only —
// never the body.
func idHeaders(messageID, traceID, accountID, customerID uuid.UUID) []kafka.Header {
	return []kafka.Header{
		{Key: kafka.HeaderMessageID, Value: []byte(messageID.String())},
		{Key: kafka.HeaderTraceID, Value: []byte(traceID.String())},
		{Key: kafka.HeaderAccountID, Value: []byte(accountID.String())},
		{Key: kafka.HeaderCustomerID, Value: []byte(customerID.String())},
	}
}
