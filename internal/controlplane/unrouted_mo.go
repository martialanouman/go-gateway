package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// UnroutedReason says why an MO resolved to no account (control_plane.unrouted_mo.reason). It makes
// the operator queue actionable: unknown number, a disabled number, or a shared number no keyword
// matched.
type UnroutedReason string

// The unrouted reasons.
const (
	UnroutedUnknownNumber  UnroutedReason = "unknown_number"
	UnroutedNumberDisabled UnroutedReason = "number_disabled"
	UnroutedNoKeywordMatch UnroutedReason = "no_keyword_match"
)

// Valid reports whether r is a published unrouted reason.
func (r UnroutedReason) Valid() bool {
	switch r {
	case UnroutedUnknownNumber, UnroutedNumberDisabled, UnroutedNoKeywordMatch:
		return true
	default:
		return false
	}
}

// UnroutedMO is a mobile-originated message the router could not assign to an account
// (control_plane.unrouted_mo), kept for the operator to see and fix the config. It NEVER carries the
// message body (invariant a): only routing metadata. ConnectorID and InboundNumberID are optional
// (an unknown number leaves InboundNumberID nil).
type UnroutedMO struct {
	ID              uuid.UUID
	ReceivedAt      time.Time
	ConnectorID     *uuid.UUID
	InboundNumberID *uuid.UUID
	SourceAddr      string
	DestAddr        string
	SegmentCount    int
	Encoding        string
	Reason          UnroutedReason
}

// NewUnroutedMO is the input to record an unrouted MO.
type NewUnroutedMO struct {
	ReceivedAt      time.Time
	ConnectorID     *uuid.UUID
	InboundNumberID *uuid.UUID
	SourceAddr      string
	DestAddr        string
	SegmentCount    int
	Encoding        string
	Reason          UnroutedReason
}

// UnroutedMOKey is a keyset pagination position for list-unrouted-mo: the (received_at, id) of a
// page's last row. Both are immutable per row and form the table's sort key, so together they order
// rows deterministically.
type UnroutedMOKey struct {
	ReceivedAt time.Time
	ID         uuid.UUID
}
