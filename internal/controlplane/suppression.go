package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// Suppression is an opt-out entry: a destination MSISDN that must not receive MT within a scope
// (§6.20). ScopeID is nil for the platform scope, the owning inbound_number/smpp_account/customer id
// otherwise. MSISDN is E.164-normalized at write.
type Suppression struct {
	ID        uuid.UUID
	Scope     SuppressionScope
	ScopeID   *uuid.UUID
	MSISDN    string
	Source    SuppressionSource
	Reason    *string
	CreatedAt time.Time
}

// NewSuppression is the input to write a suppression (§6.20). ScopeID is nil for the platform scope.
// MSISDN must be E.164-normalized (the schema enforces the canonical form).
type NewSuppression struct {
	Scope   SuppressionScope
	ScopeID *uuid.UUID
	MSISDN  string
	Source  SuppressionSource
	Reason  *string
}

// SuppressionFilter selects and paginates a suppression listing (Admin, step-064). All fields are
// optional filters; After/Limit drive keyset pagination on the id.
type SuppressionFilter struct {
	Scope   *SuppressionScope
	ScopeID *uuid.UUID
	MSISDN  *string
	After   uuid.UUID
	Limit   int
}

// NewOptOutKeyword is the input to create an opt-out keyword (Admin, step-064). MatchType is optional
// (the schema defaults to exact).
type NewOptOutKeyword struct {
	CountryCode       *string
	Keyword           string
	Action            OptOutAction
	MatchType         *OptOutMatchType
	AutoReplyTemplate *string
}

// OptOutKeywordPatch is a partial update of an opt-out keyword (Admin, step-064). A nil field is left
// unchanged.
type OptOutKeywordPatch struct {
	Keyword           *string
	Action            *OptOutAction
	MatchType         *OptOutMatchType
	AutoReplyTemplate *string
	Status            *OptOutKeywordStatus
}

// OptOutKeyword is an inbound keyword (STOP, START, HELP…) that toggles suppression or triggers a
// help reply (§6.20). CountryCode nil applies to all countries. AutoReplyTemplate, when set, is sent
// back as an MT that is never billed.
type OptOutKeyword struct {
	ID                uuid.UUID
	CountryCode       *string
	Keyword           string
	Action            OptOutAction
	MatchType         OptOutMatchType
	AutoReplyTemplate *string
	Status            OptOutKeywordStatus
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
