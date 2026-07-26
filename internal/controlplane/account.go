package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// Account is one technical integration of a customer (control_plane.smpp_accounts): its
// credentials, channels, throughput and session limits. A customer has one or more (ADR-0006).
type Account struct {
	ID              uuid.UUID
	CustomerID      uuid.UUID
	Name            string
	Status          AccountStatus
	SMPPEnabled     bool
	RESTEnabled     bool
	SenderIDPolicy  SenderIDPolicy
	QuerySMEnabled  bool
	CancelSMEnabled bool
	// AllowedBindTypes is a single bind kind despite the plural column name (see BindType).
	AllowedBindTypes BindType
	MaxSessions      int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// NewAccount is the input to create an SMPP account. Optional fields are pointers so the
// repository applies the schema default when they are nil.
type NewAccount struct {
	CustomerID       uuid.UUID
	Name             string
	SMPPEnabled      *bool
	RESTEnabled      *bool
	SenderIDPolicy   *SenderIDPolicy
	QuerySMEnabled   *bool
	CancelSMEnabled  *bool
	AllowedBindTypes *BindType
	MaxSessions      *int
}

// AccountSenderIDPolicy is the account -> (customer, sender-ID policy) projection the sender-ID
// authorization stage snapshots at startup (step-060): the policy is per account, but the registered
// sender IDs it is checked against are per customer.
type AccountSenderIDPolicy struct {
	AccountID  uuid.UUID
	CustomerID uuid.UUID
	Policy     SenderIDPolicy
}

// AccountPatch is a partial update of an account. Per the contract's SmppAccountUpdate, only the
// name and status are updatable here; channels and session limits have their own endpoints.
type AccountPatch struct {
	Name   *string
	Status *AccountStatus
}

// AccountFilter selects and paginates an account listing.
type AccountFilter struct {
	CustomerID *uuid.UUID
	GroupID    *uuid.UUID
	Status     *AccountStatus
	After      uuid.UUID
	Limit      int
}
