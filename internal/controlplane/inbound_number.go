package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// InboundNumber is a shortcode, long code or alphanumeric sender the provider owns for the return
// path (control_plane.inbound_numbers). ConnectorID names the SMSC link that delivers its MO;
// AccountID, when set, dedicates the number to one SMPP account, and a nil AccountID means the
// number is shared and resolved by keywords (step-041/045). Both FKs are ON DELETE SET NULL.
type InboundNumber struct {
	ID          uuid.UUID
	Address     string
	NumberType  NumberType
	CountryCode string
	MCCMNC      *string
	ConnectorID *uuid.UUID
	AccountID   *uuid.UUID
	Status      InboundNumberStatus
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewInboundNumber is the input to create an inbound number. Status takes the DDL default ('active'),
// so it is not accepted here. MCCMNC, ConnectorID and AccountID are optional (nil leaves the column
// NULL). (Address, CountryCode) is the unique key, so a duplicate is a conflict (409).
type NewInboundNumber struct {
	Address     string
	NumberType  NumberType
	CountryCode string
	MCCMNC      *string
	ConnectorID *uuid.UUID
	AccountID   *uuid.UUID
}

// InboundNumberPatch is a partial update of an inbound number. Per the contract's InboundNumberUpdate
// only these four fields are updatable: the identity fields (address, country_code) are immutable
// once set, and account_id is changed only through Assign. A nil field is left unchanged.
type InboundNumberPatch struct {
	NumberType  *NumberType
	MCCMNC      *string
	ConnectorID *uuid.UUID
	Status      *InboundNumberStatus
}
