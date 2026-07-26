package controlplane

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// AntispamRuleType is the family of an anti-spam rule (control_plane.antispam_rules.rule_type). M5
// implements content_blacklist and duplicate; velocity and reputation land in step-066.
type AntispamRuleType string

// The anti-spam rule types.
const (
	AntispamContentBlacklist AntispamRuleType = "content_blacklist"
	AntispamDuplicate        AntispamRuleType = "duplicate"
	AntispamVelocity         AntispamRuleType = "velocity"
	AntispamReputation       AntispamRuleType = "reputation"
)

// Valid reports whether t is a published rule type.
func (t AntispamRuleType) Valid() bool {
	switch t {
	case AntispamContentBlacklist, AntispamDuplicate, AntispamVelocity, AntispamReputation:
		return true
	default:
		return false
	}
}

// AntispamScope is a rule's scope (control_plane.antispam_rules.scope), resolved most-specific first:
// smpp_account, then customer, then global.
type AntispamScope string

// The anti-spam rule scopes.
const (
	AntispamScopeGlobal   AntispamScope = "global"
	AntispamScopeCustomer AntispamScope = "customer"
	AntispamScopeAccount  AntispamScope = "smpp_account"
)

// Valid reports whether s is a published rule scope.
func (s AntispamScope) Valid() bool {
	switch s {
	case AntispamScopeGlobal, AntispamScopeCustomer, AntispamScopeAccount:
		return true
	default:
		return false
	}
}

// AntispamAction is what a matched rule does (control_plane.antispam_rules.action): block the message
// (a rejection), flag it (a non-blocking annotation), or throttle its sender (a slowdown signal).
type AntispamAction string

// The anti-spam rule actions.
const (
	AntispamActionBlock    AntispamAction = "block"
	AntispamActionFlag     AntispamAction = "flag"
	AntispamActionThrottle AntispamAction = "throttle"
)

// Valid reports whether a is a published rule action.
func (a AntispamAction) Valid() bool {
	switch a {
	case AntispamActionBlock, AntispamActionFlag, AntispamActionThrottle:
		return true
	default:
		return false
	}
}

// AntispamRuleStatus is a rule's config status (control_plane.antispam_rules.status).
type AntispamRuleStatus string

// The anti-spam rule statuses.
const (
	AntispamRuleActive   AntispamRuleStatus = "active"
	AntispamRuleDisabled AntispamRuleStatus = "disabled"
)

// Valid reports whether s is a published rule status.
func (s AntispamRuleStatus) Valid() bool {
	switch s {
	case AntispamRuleActive, AntispamRuleDisabled:
		return true
	default:
		return false
	}
}

// AntispamRule is one anti-spam rule (§6.20). ConfigJSON is a rule-type-specific object: a
// content_blacklist carries {"patterns": [...regex]}, a duplicate carries {"window_seconds": N}.
type AntispamRule struct {
	ID         uuid.UUID
	RuleType   AntispamRuleType
	Scope      AntispamScope
	ScopeID    *uuid.UUID
	ConfigJSON json.RawMessage
	Action     AntispamAction
	Status     AntispamRuleStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
