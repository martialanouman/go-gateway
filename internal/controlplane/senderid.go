package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// SenderID is a customer-level sender address awaiting or holding carrier approval
// (control_plane.sender_ids).
type SenderID struct {
	ID              uuid.UUID
	CustomerID      uuid.UUID
	Address         string
	Status          SenderIDStatus
	TrafficCategory TrafficCategory
	RateLimit       *SenderIDRateLimit
	CreatedBy       *uuid.UUID
	ApprovedAt      *time.Time
	// FirstUsedAt is set once a message from this address reached a carrier SMSC; a used sender ID can
	// only be disabled, never deleted (ADR-0023).
	FirstUsedAt *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// SenderIDRateLimit is a sender ID's own throughput limit, applied at admission next to its account's
// (ADR-0021 §3).
type SenderIDRateLimit struct {
	MaxPerSec     int
	BurstCapacity int
}

// SenderIDUse records that a customer submitted a message from address, at UsedAt.
type SenderIDUse struct {
	CustomerID uuid.UUID
	Address    string
	UsedAt     time.Time
}

// NewSenderID is the input to register a sender ID under a customer. It starts pending carrier
// approval (the DDL default), so no status is accepted here. A nil TrafficCategory is stored as
// marketing.
type NewSenderID struct {
	CustomerID      uuid.UUID
	Address         string
	TrafficCategory *TrafficCategory
	CreatedBy       *uuid.UUID
}

// SenderIDPatch is a partial update of a sender ID (contract schema SenderIdUpdate).
type SenderIDPatch struct {
	Status          *SenderIDStatus
	TrafficCategory *TrafficCategory
}
