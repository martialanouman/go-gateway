package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// SenderID is a customer-level sender address awaiting or holding carrier approval
// (control_plane.sender_ids).
type SenderID struct {
	ID         uuid.UUID
	CustomerID uuid.UUID
	Address    string
	Status     SenderIDStatus
	CreatedBy  *uuid.UUID
	ApprovedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// NewSenderID is the input to register a sender ID under a customer. It starts pending carrier
// approval (the DDL default), so no status is accepted here.
type NewSenderID struct {
	CustomerID uuid.UUID
	Address    string
	CreatedBy  *uuid.UUID
}

// SenderIDPatch is a partial update of a sender ID. Per the contract's SenderIdUpdate, only the
// status is updatable.
type SenderIDPatch struct {
	Status *SenderIDStatus
}
