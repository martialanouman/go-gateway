package controlplane

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// SenderRewriteScope is a rewrite rule's scope (control_plane.sender_id_rewrite_rules.scope), resolved
// connector, then smpp_account, then customer, then platform (§6.16).
type SenderRewriteScope string

// The sender rewrite scopes.
const (
	RewriteScopeConnector SenderRewriteScope = "connector"
	RewriteScopeAccount   SenderRewriteScope = "smpp_account"
	RewriteScopeCustomer  SenderRewriteScope = "customer"
	RewriteScopePlatform  SenderRewriteScope = "platform"
)

// SenderRewriteType is how a matched rule rewrites the source address.
type SenderRewriteType string

// The sender rewrite types.
const (
	RewriteStatic       SenderRewriteType = "static"
	RewriteFallbackPool SenderRewriteType = "fallback_pool"
	RewriteTruncate     SenderRewriteType = "truncate"
	RewriteSanitize     SenderRewriteType = "sanitize"
)

// SenderRewriteRule is one sender-ID rewrite rule (§6.16). Only the fields its RewriteType names are
// read by the evaluation; the others may hold leftovers of an earlier type.
type SenderRewriteRule struct {
	ID                 uuid.UUID
	Scope              SenderRewriteScope
	ScopeID            *uuid.UUID
	Direction          string
	MatchSenderPattern *string
	MatchDestPattern   *string
	RewriteType        SenderRewriteType
	RewriteTo          *string
	FallbackPool       []string
	MaxLength          *int32
	SanitizeCharset    json.RawMessage
	Priority           int32
	Reason             *string
	Status             string
	CreatedBy          *uuid.UUID
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// NewSenderRewriteRule is the input to create a rewrite rule. Direction is absent: only mt rules are
// evaluated, and the column defaults to it.
type NewSenderRewriteRule struct {
	Scope              SenderRewriteScope
	ScopeID            *uuid.UUID
	MatchSenderPattern *string
	MatchDestPattern   *string
	RewriteType        SenderRewriteType
	RewriteTo          *string
	FallbackPool       []string
	MaxLength          *int32
	SanitizeCharset    json.RawMessage
	Priority           int32
	Reason             *string
}

// SenderRewriteRulePatch is a partial update of a rewrite rule: a nil field is left unchanged. Scope,
// scope_id and direction are immutable.
type SenderRewriteRulePatch struct {
	MatchSenderPattern *string
	MatchDestPattern   *string
	RewriteType        *SenderRewriteType
	RewriteTo          *string
	FallbackPool       []string
	MaxLength          *int32
	SanitizeCharset    json.RawMessage
	Priority           *int32
	Reason             *string
	Status             *string
}

// SenderRewriteFilter narrows the rule list; a nil field does not filter.
type SenderRewriteFilter struct {
	Scope   *SenderRewriteScope
	ScopeID *uuid.UUID
}
