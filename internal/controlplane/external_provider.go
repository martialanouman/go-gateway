package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// ExternalBillingProvider is an external billing system's connection config (control_plane.
// external_billing_providers, §6.10). AuthConfig holds credentials as opaque JSON — MASKED on read, never
// returned to a client. mode/failure_policy reuse the ExternalBillingMode / BillingFailurePolicy enums.
type ExternalBillingProvider struct {
	ID                uuid.UUID
	Name              string
	BaseURL           string
	AuthConfig        []byte // jsonb credentials — masked on read
	Mode              string // balance_check | consume_delegate_async | consume_delegate_sync | both
	CacheTTLMs        int
	SyncCallTimeoutMs *int
	FailurePolicy     string // fail_open | fail_closed
	Status            string // active | disabled
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// NewExternalBillingProvider is the input to create a provider. Nil pointers apply the schema defaults
// (cache_ttl_ms 1000, fail_open).
type NewExternalBillingProvider struct {
	Name              string
	BaseURL           string
	AuthConfig        []byte
	Mode              string
	CacheTTLMs        *int
	SyncCallTimeoutMs *int
	FailurePolicy     *string
}

// ExternalBillingProviderPatch is a partial update; a nil field leaves its column unchanged.
type ExternalBillingProviderPatch struct {
	Name              *string
	BaseURL           *string
	AuthConfig        []byte
	Mode              *string
	CacheTTLMs        *int
	SyncCallTimeoutMs *int
	FailurePolicy     *string
	Status            *string
}
