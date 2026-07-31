package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// RatePlan is a pricing plan (control_plane.rate_plans, §3.1): integer credits PER SEGMENT (never monetary),
// keyed by destination/sender in the JSON maps. billing_mode/charge_on/status are the plan's policy knobs.
type RatePlan struct {
	ID                  uuid.UUID
	Name                string
	CreditsPerSegmentMT []byte // jsonb: integer credits per MT segment, by MCC-MNC/country + sender type
	CreditsPerSegmentMO []byte // jsonb: integer credits per MO segment
	BillingMode         string // prepaid | postpaid | either
	ChargeOn            string // submission | delivery
	Status              string // active | disabled
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// NewRatePlan is the input to create a rate plan. Nil enum pointers apply the schema default (either / submission).
type NewRatePlan struct {
	Name                string
	CreditsPerSegmentMT []byte
	CreditsPerSegmentMO []byte
	BillingMode         *string
	ChargeOn            *string
}

// RatePlanPatch is a partial update; a nil field (or nil JSON) leaves its column unchanged.
type RatePlanPatch struct {
	Name                *string
	CreditsPerSegmentMT []byte
	CreditsPerSegmentMO []byte
	BillingMode         *string
	ChargeOn            *string
	Status              *string
}
