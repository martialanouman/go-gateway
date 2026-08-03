// Package pipeline is the MT processing pipeline: the message envelopes that travel the Kafka data
// plane, the ordered stages the router runs, and the WIRE codec that moves an envelope on and off a
// Kafka record. It sits above internal/storage/kafka (the transport) and uses internal/smpp for the
// esm_class UDH indicator and segment encoding; it never logs or spans a message body (invariant a).
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

// RoutedMT is ONE segment of a message after the router's pipeline: destination normalised to E.164,
// encoding resolved, a connector chosen, and the body carrying the segment's wire short_message
// (the concatenation UDH followed by the encoded content when the message spans several segments, the
// bare encoded content when it does not — internal/pipeline/encoding.Split produces it). It is carried
// on mt.routed, keyed by the logical message id so every segment of a message reaches the same
// connector bind in order (§7.3). A short message is a single segment (Seq 1, Count 1).
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
	// FallbackChain is the ordered list of connectors this message may be rerouted through when its
	// current target degrades (breaker open or a connector-health rejection), computed by the router
	// from the route's strategy (step-114/125). It travels in the fallback_chain HEADER (routing
	// metadata, never the body — invariant a), so the connector pool can reroute unilaterally without a
	// round-trip to the router. Empty means no reroute (a single terminal outcome).
	FallbackChain []uuid.UUID
	// ReplayedAt is set when this message was replayed from a dead-letter topic (step-129). The connector
	// pool's gateway max-age check uses max(SubmittedAt, ReplayedAt), so an operator replay after a long
	// outage is not instantly re-expired on the immutable SubmittedAt. Nil for a normal message.
	ReplayedAt *time.Time
	// SegmentSeq is this segment's 1-based position; SegmentCount is the total number of segments the
	// message was split into. All segments of a message share MessageID, ConnectorID and SegmentCount,
	// and are produced under the same partition key so they stay ordered on one bind.
	SegmentSeq   int
	SegmentCount int
	// HasUDH tells the connector to set esm_class's UDH indicator: Body begins with a concatenation
	// User Data Header. It is carried explicitly rather than derived from SegmentCount > 1 because a
	// client that pre-segments its own SMPP submit (UDHI already set on the inbound esm_class) travels
	// as a single record that nonetheless carries a UDH (SegmentCount 1, HasUDH true).
	HasUDH      bool
	SubmittedAt time.Time
	// Billable means a credit RESERVATION EXISTS for this message: the credit stage reserved against the
	// customer's balance, so connector-pool must capture (or release) it after the send (step-145/146). It
	// is false when there is nothing to settle — billing disabled for the customer, or a system message
	// produced straight to mt.routed by mo-dlr-router-svc (a STOP auto-reply, §6.20). connector-pool keys
	// the zero-billing-call skip off this flag, so it MUST mean "reservation exists", not merely "client
	// traffic". It is deliberately absent from InboundMT and every client-facing surface, so a client
	// cannot forge a settlement: the security boundary is who may produce to mt.routed, not a submitted flag.
	Billable bool
	// OwnerType is the balance owner the reservation was made against (customer | smpp_account), resolved
	// once by the credit stage from the customer's balance_scope and pinned here so connector-pool captures
	// the IDENTICAL balance key — a mid-flight scope change can never make reserve and capture disagree
	// (step-145). Empty when Billable is false (no reservation). owner_id is not carried: it is a
	// deterministic projection of OwnerType over the CustomerID/AccountID already on the record.
	OwnerType string
}

// OutcomeMT is the terminal outcome of ONE submitted segment, carried on mt.outcome for the CDR
// projection to turn into a row (step-201c, D1). It is the exact projection the connector pool used to
// write to ClickHouse itself at the submit site: identifiers, addressing, segment coordinates, the
// immutable accept time, the resolved lifecycle status with its gateway error code, and the billing
// settlement. Everything a CDR outcome row holds — and nothing else.
//
// It carries NO body, by construction and not by omission: the enroute/failed row stores no content
// (only the accepted row does, sealed by the ingest projection from mt.inbound), so there is no
// audited egress here at all (invariant a).
//
// Status is the CDR lifecycle status as a plain string (enroute | failed today), not an SMPP
// command_status: the connector owns the SMPP vocabulary and resolves it once, so the projection maps
// a status onto a row without re-deriving it — and a status it does not know is a corrupt record, not
// a silent rank-0 row. ErrorCode is a gateway code from the shared platform/errors contract, nil when
// the send succeeded. Billed/CreditsCharged are the settlement the connector captured; they are
// carried rather than recomputed because only the connector saw the reservation.
//
// DELIVERY GUARANTEE: at-least-once. The projection is idempotent — `cdr` is a
// ReplacingMergeTree keyed by the row's identity and versioned by the status rank, so a replayed
// outcome collapses onto the same row.
type OutcomeMT struct {
	MessageID    uuid.UUID
	TraceID      uuid.UUID
	AccountID    uuid.UUID
	CustomerID   uuid.UUID
	ConnectorID  uuid.UUID
	RouteID      *uuid.UUID
	From         string
	To           string
	Encoding     string // resolved: gsm7|ucs2|binary
	SegmentSeq   int
	SegmentCount int
	SubmittedAt  time.Time
	// Status is the CDR lifecycle status this outcome records: enroute for an accepted submit_sm, failed
	// for a permanent SMSC rejection.
	Status string
	// ErrorCode is the gateway error code for a failed outcome, nil for enroute.
	ErrorCode *string
	// Billed and CreditsCharged are the capture result for a sent billable message (step-146); false/nil
	// when nothing was captured (billing disabled, no reservation, or a fail-open capture).
	Billed         bool
	CreditsCharged *int32
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
