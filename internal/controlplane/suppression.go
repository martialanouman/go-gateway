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
