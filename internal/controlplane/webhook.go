package controlplane

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// WebhookEventType is the return-path event a webhook subscribes to (control_plane.webhooks.event_type):
// a mobile-originated message or a delivery receipt.
type WebhookEventType string

// The webhook event types.
const (
	WebhookEventMO  WebhookEventType = "mo"
	WebhookEventDLR WebhookEventType = "dlr"
)

// Valid reports whether t is a published webhook event type.
func (t WebhookEventType) Valid() bool {
	switch t {
	case WebhookEventMO, WebhookEventDLR:
		return true
	default:
		return false
	}
}

// WebhookStatus is a webhook's enablement state (control_plane.webhooks.status).
type WebhookStatus string

// The webhook enablement states.
const (
	WebhookActive   WebhookStatus = "active"
	WebhookDisabled WebhookStatus = "disabled"
)

// Valid reports whether s is a published webhook status.
func (s WebhookStatus) Valid() bool {
	switch s {
	case WebhookActive, WebhookDisabled:
		return true
	default:
		return false
	}
}

// Webhook is an account's return-path HTTP callback (control_plane.webhooks): the provider POSTs each
// MO or DLR to URL, signed with Secret. RetryPolicyJSON is the raw retry_policy_json; the webhook
// sender parses it (the control plane stays free of delivery semantics). One webhook per (account,
// event_type) — the unique key.
//
// Secret is SEALED, not hashed (ADR-0016): it is a secret the gateway REPLAYS — webhook.Sign needs it in
// clear on every delivery — so the sender opens it just before signing. It is never in clear at rest and
// never leaves the Admin API.
type Webhook struct {
	ID              uuid.UUID
	AccountID       uuid.UUID
	EventType       WebhookEventType
	URL             string
	Secret          SealedSecret
	RetryPolicyJSON json.RawMessage
	Status          WebhookStatus
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// NewWebhook is a webhook subscription to create. Secret is required: it is the HMAC key the receiver
// verifies each delivery with, and it is supplied by the operator rather than generated here — the
// contract makes it write-only, never returned. It arrives already sealed: the Admin API seals it, and
// nothing below that boundary ever holds the clear value.
type NewWebhook struct {
	AccountID       uuid.UUID
	EventType       WebhookEventType
	URL             string
	Secret          SealedSecret
	RetryPolicyJSON json.RawMessage
}

// WebhookPatch is a partial change; a nil field leaves its column alone. EventType is absent on
// purpose — it is the identity of the subscription (one per account and event type), not a setting,
// and changing it would be a different subscription under the same id.
type WebhookPatch struct {
	URL             *string
	Secret          *SealedSecret
	RetryPolicyJSON json.RawMessage
	Status          *WebhookStatus
}
