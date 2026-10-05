package controlplane

import "github.com/google/uuid"

// RateLimit is the optional throughput ceiling configured for an entity (control_plane.rate_limits).
// Each field is a pointer because the column is individually nullable: nil means "no limit set for
// this dimension". A RateLimit only exists when a row is configured for the entity; the absence of a
// row is signalled by the caller, not by a zero value here.
type RateLimit struct {
	MaxPerSec     *int
	MaxPerDay     *int
	BurstCapacity *int
}

// RateLimitEntry is one configured limit with the entity it applies to (entity_type is one of
// smpp_account/connector/sender_id). The cold-loaded snapshot of the ingestion and the connector pool is
// built from a List of these, then indexed by (EntityType, EntityID). Sender is set for a sender_id entry
// only: admission knows a submission by its customer and source address, not by the sender's id.
type RateLimitEntry struct {
	EntityType string
	EntityID   uuid.UUID
	Sender     *SenderAddress
	Limit      RateLimit
}

// SenderAddress is the key a submission's sender ID is known by: its customer and the source address.
type SenderAddress struct {
	CustomerID uuid.UUID
	Address    string
}
