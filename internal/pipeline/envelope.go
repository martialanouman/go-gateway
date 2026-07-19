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
