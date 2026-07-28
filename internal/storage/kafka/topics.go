// Package kafka is the gateway's durable data plane (plan §1.6): a thin, opinionated wrapper over
// franz-go that fixes the two rules the whole pipeline depends on — a producer that is
// acks=all + idempotent, and a consumer that commits offsets only AFTER a record is processed
// (at-least-once). Callers get a Producer or a Consumer; the franz-go client stays encapsulated.
package kafka

// Topic names (plan §1.6). The MT walking skeleton (M2) uses only mt.inbound and mt.routed; the
// rest are declared here so every service names a topic through this one list, not a string
// literal.
const (
	// TopicMTInbound carries raw submissions (SMPP/REST), pre-routing. A REST 202 is earned by a
	// durable write here (§6.7). Partition key = a hash of the account.
	TopicMTInbound = "mt.inbound"
	// TopicMTRouted carries routed messages, one per logical message. Partition key = the logical
	// message id, so every UDH segment of a message lands on the same partition, in order (§7.3).
	TopicMTRouted = "mt.routed"
	// TopicMOInbound carries mobile-originated messages from the SMSC (M4).
	TopicMOInbound = "mo.inbound"
	// TopicMORouted carries mobile-originated messages after account resolution (M4): the delivery
	// intent step-048 consumes to hand the MO to the account's active bind or webhook. Partition key =
	// the resolved account, so one account's MO stay ordered.
	TopicMORouted = "mo.routed"
	// TopicDLREvents carries delivery-receipt events (M4).
	TopicDLREvents = "dlr.events"
	// TopicMTDeadLetter is the parking topic for MT messages that exhausted handling (M7+).
	TopicMTDeadLetter = "mt.dead-letter"
	// TopicMODeadLetter is the parking topic for MO messages that could not be delivered to the account
	// (no live bind and no active webhook), kept for the operator to replay (step-048).
	TopicMODeadLetter = "mo.dead-letter"
	// TopicDLRDeadLetter is the parking topic for DLR receipts that could not be delivered to the
	// account (step-048), mirroring mo.dead-letter.
	TopicDLRDeadLetter = "dlr.dead-letter"
	// TopicWebhookDeadLetter parks webhook events whose delivery was abandoned — retries exhausted or a
	// permanent rejection (step-048). Distinct from mo/dlr.dead-letter: those park an event that had no
	// delivery path at all, this parks one that reached the account's webhook but never got a 2xx.
	TopicWebhookDeadLetter = "webhook.dead-letter"
	// TopicMTReroutePark holds MT messages awaiting a fallback connector (M7).
	TopicMTReroutePark = "mt.reroute-park"
	// TopicMetricsStream feeds the real-time WebSocket metrics (M11).
	TopicMetricsStream = "metrics.stream"
)

// Header keys carried on every pipeline record (§7.3). They are identifiers only — the message
// body travels in the record value, NEVER in a header, so nothing loggable carries plaintext
// (invariant a).
const (
	HeaderTraceID       = "trace_id"
	HeaderMessageID     = "message_id"
	HeaderAccountID     = "account_id"
	HeaderCustomerID    = "customer_id"
	HeaderFallbackChain = "fallback_chain"
	// HeaderDeadLetterReason carries WHY a message was parked on a dead-letter topic (step-129). It is
	// stripped when the message is replayed, so a second dead-lettering records a fresh reason.
	HeaderDeadLetterReason = "dead_letter_reason"
	// HeaderReplayedAt is the RFC3339 time a dead-lettered message was replayed (step-129). The gateway
	// max-age check bases expiry on max(submitted_at, replayed_at), so a replay after a long outage is not
	// instantly re-expired on its immutable submitted_at.
	HeaderReplayedAt = "replayed_at"
)
