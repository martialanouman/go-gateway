// Package pipeline is the MT processing pipeline: the message envelopes that travel the Kafka data
// plane, the ordered stages the router runs, and the WIRE codec that moves an envelope on and off a
// Kafka record. It sits above internal/storage/kafka (the transport) and internal/smpp (unused
// here); it never logs or spans a message body (invariant a).
package pipeline

import (
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/platform/msg"
)

// InboundMT is a submission after authentication and ingestion, carried on mt.inbound. The ids are
// minted at ingestion (UUIDv7); SubmittedAt is the accept time and is immutable — it is carried
// unchanged to mt.routed and onto every CDR row so the CDR partition stays stable (§1.10). The
// address fields are as the client sent them; the router normalises them.
type InboundMT struct {
	MessageID  uuid.UUID
	TraceID    uuid.UUID
	AccountID  uuid.UUID
	CustomerID uuid.UUID
	From       string
	To         string
	Body       msg.Body
	Encoding   string // requested: auto|gsm7|ucs2|binary
	// ESMClass is the SMPP esm_class byte as submitted (message mode/type, UDH indicator — SMPP v3.4
	// §5.2.12). It carries the UDHI bit the router needs to detect pre-segmented content. REST leaves
	// it 0 (a plain point-to-point message); an SMPP submit_sm maps it straight from the PDU.
	ESMClass           uint8
	RegisteredDelivery bool
	ValidityPeriod     *string
	Priority           int
	ClientRef          *string
	DataCoding         *int
	SubmittedAt        time.Time
}

// RoutedMT is a message after the router's pipeline: destination normalised to E.164, encoding
// resolved, and a connector chosen. It is carried on mt.routed, keyed by the logical message id so
// every segment reaches the same connector bind in order (§7.3). M2 assumes a single segment.
type RoutedMT struct {
	MessageID          uuid.UUID
	TraceID            uuid.UUID
	AccountID          uuid.UUID
	CustomerID         uuid.UUID
	From               string
	To                 string // E.164 normalised
	Body               msg.Body
	Encoding           string // resolved: gsm7|ucs2|binary
	RegisteredDelivery bool
	ValidityPeriod     *string
	DataCoding         *int // client data_coding override, carried through to the SMSC (nil = derive from Encoding)
	ConnectorID        uuid.UUID
	RouteID            *uuid.UUID
	SegmentCount       int
	SubmittedAt        time.Time
}

// MOInbound is a mobile-originated message a SMSC delivered to one of our inbound numbers, carried on
// mo.inbound before routing to an account (step-045). It has no message id yet — that is minted when
// the return-path router accepts it. From is the subscriber's address as the SMSC sent it (normalised
// to E.164 downstream); To is our inbound number. ConnectorID names the link it arrived on. Body is
// the MO content, masked (invariant a). ReceivedAt is when the connector processed the deliver_sm.
//
// ORDERING: the connector publishes with a bounded worker pool, so records are NOT guaranteed to be
// produced in arrival order even within one partition key (the inbound number). The consumer
// (step-045) must not assume partition order — reassemble a multipart MO by its UDH reference, not by
// arrival sequence.
type MOInbound struct {
	ConnectorID uuid.UUID
	From        string
	To          string
	Body        msg.Body
	DataCoding  uint8
	ESMClass    uint8
	ReceivedAt  time.Time
}

// MORouted is a mobile-originated message after account resolution (step-045), carried on mo.routed
// for the delivery step (step-048) to hand to the account's active bind or webhook. The router mints
// MessageID and TraceID at resolution (an MO has none before), resolves AccountID (a dedicated number
// or a matched keyword) and its CustomerID, and remembers which InboundNumberID and ConnectorID it
// came in on. Body is the MO content, masked (invariant a); it rides the record value only. Keyed by
// AccountID so one account's MO stay ordered.
type MORouted struct {
	MessageID       uuid.UUID
	TraceID         uuid.UUID
	AccountID       uuid.UUID
	CustomerID      uuid.UUID
	InboundNumberID uuid.UUID
	ConnectorID     uuid.UUID
	From            string
	To              string
	Body            msg.Body
	DataCoding      uint8
	Encoding        string
	ESMClass        uint8
	ReceivedAt      time.Time
}

// DLREvent is a delivery receipt a SMSC returned for a message we submitted, carried on dlr.events.
// SMSCMessageID is the id the SMSC assigned at submit time (the key into the dlrmap, §1.11); the
// return-path router resolves it back to our message id (step-044). State is the SMPP message_state;
// Stat is its textual form ("DELIVRD"…); ErrorCode is the receipt's network error. SubmitDate and
// DoneDate are the raw receipt timestamps when present. A DLR carries no message body — only receipt
// metadata — so there is no Body field. ReceivedAt is when the connector processed the receipt.
//
// DELIVERY GUARANTEE (consumers must handle both): (1) At-least-once — the same receipt may be
// published twice (a crash between publish and ack, then the SMSC retries). (2) No ordering — the
// connector publishes with a bounded worker pool, so receipts for one SMSCMessageID may arrive out of
// order even on the same partition. The DLR consumer (step-044) absorbs both through the CDR itself:
// each terminal receipt writes a versioned row whose ReplacingMergeTree rank decides the current
// state, so a duplicate same-state receipt collapses idempotently and a non-terminal receipt never
// supersedes a terminal one. Distinct terminal states (delivered vs failed/expired) are mutually
// exclusive for a well-behaved message; if an anomalous out-of-order pair ever occurred, the CDR rank
// (not arrival order) decides — a composite-rank refinement making delivered sticky is deferred (the
// CDR schema anticipates widening the rank at M4+).
type DLREvent struct {
	ConnectorID   uuid.UUID
	SMSCMessageID string
	State         uint8
	Stat          string
	ErrorCode     string
	SubmitDate    string
	DoneDate      string
	ReceivedAt    time.Time
}
