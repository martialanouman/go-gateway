package controlplane

import (
	"encoding/json"

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
type Webhook struct {
	ID              uuid.UUID
	AccountID       uuid.UUID
	EventType       WebhookEventType
	URL             string
	Secret          string
	RetryPolicyJSON json.RawMessage
	Status          WebhookStatus
}
