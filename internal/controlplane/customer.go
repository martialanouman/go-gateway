package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// Customer is a commercial account owner (control_plane.customers). It carries the commercial
// relationship — balances, rate plan, sender IDs, content policy — while the technical integration
// lives on its SMPP accounts (ADR-0006).
type Customer struct {
	ID                        uuid.UUID
	Name                      string
	Status                    CustomerStatus
	GroupID                   *uuid.UUID
	RatePlanID                *uuid.UUID
	BillingEnabled            bool
	BillingMode               *BillingMode
	OverdraftEnabled          bool
	OverdraftLimit            *int
	CreditLimit               *int
	CreditLimitIsHard         bool
	ExternalBillingProviderID *uuid.UUID
	BalanceScope              BalanceScope
	MoBillingFloor            *int
	ContentStorage            ContentStorage
	ContentRetentionDays      *int
	ContentKeyID              *uuid.UUID
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

// NewCustomer is the input to create a customer. It has no status: creation is always active
// (the DDL default). Optional enum fields are pointers so the repository can apply the schema
// default when they are nil.
type NewCustomer struct {
	Name                 string
	GroupID              *uuid.UUID
	RatePlanID           *uuid.UUID
	BillingEnabled       bool
	BillingMode          *BillingMode
	OverdraftEnabled     bool
	OverdraftLimit       *int
	CreditLimit          *int
	CreditLimitIsHard    bool
	BalanceScope         *BalanceScope
	MoBillingFloor       *int
	ContentStorage       *ContentStorage
	ContentRetentionDays *int
}

// CustomerPatch is a partial update of a customer. A nil field is left unchanged. group_id is
// absent on purpose — group membership is changed through its own endpoint (out of M1 scope).
type CustomerPatch struct {
	Name              *string
	Status            *CustomerStatus
	RatePlanID        *uuid.UUID
	BillingEnabled    *bool
	BillingMode       *BillingMode
	OverdraftEnabled  *bool
	OverdraftLimit    *int
	CreditLimit       *int
	CreditLimitIsHard *bool
	MoBillingFloor    *int
	// ExternalBillingProviderID assigns an external billing provider (§6.10, step-148). nil leaves it
	// unchanged; the COALESCE update cannot clear it back to null (a documented follow-up).
	ExternalBillingProviderID *uuid.UUID
	ContentStorage            *ContentStorage
	ContentRetentionDays      *int
}

// CustomerFilter selects and paginates a customer listing. After is the keyset position (exclusive
// lower bound on id); Limit is the page size already clamped by the handler.
type CustomerFilter struct {
	GroupID *uuid.UUID
	Status  *CustomerStatus
	After   uuid.UUID
	Limit   int
}
