package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// InboundKeyword maps a keyword on a SHARED inbound number to the account that should receive the
// matching MO (control_plane.inbound_keywords). It only has an effect on a shared number
// (inbound_numbers.account_id IS NULL): on a dedicated number the account is already fixed, so the
// row is inert rather than rejected — the runtime MO resolution (step-045) consults keywords only for
// shared numbers. Priority orders evaluation (lower first, via inbound_keywords_lookup_idx); AccountID
// is NOT NULL (a keyword with no target is meaningless).
type InboundKeyword struct {
	ID              uuid.UUID
	InboundNumberID uuid.UUID
	Keyword         string
	MatchType       MatchType
	AccountID       uuid.UUID
	Priority        int
	Status          InboundKeywordStatus
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// NewInboundKeyword is the input to create a keyword. InboundNumberID is taken from the request path.
// Status takes the DDL default ('active') and is not accepted here. MatchType and Priority are
// resolved to their DDL defaults ('prefix', 0) by the handler when the request omits them, so both
// arrive here already set.
type NewInboundKeyword struct {
	InboundNumberID uuid.UUID
	Keyword         string
	MatchType       MatchType
	AccountID       uuid.UUID
	Priority        int
}

// InboundKeywordPatch is a partial update of a keyword (contract InboundKeywordUpdate). A nil field is
// left unchanged. InboundNumberID is immutable: a keyword does not move between numbers.
type InboundKeywordPatch struct {
	Keyword   *string
	MatchType *MatchType
	AccountID *uuid.UUID
	Priority  *int
	Status    *InboundKeywordStatus
}
