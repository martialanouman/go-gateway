package controlplane

// CustomerStatus is the lifecycle state of a customer (control_plane.customers.status).
type CustomerStatus string

// The customer lifecycle states, ordered least-to-most restrictive for EffectiveAccountStatus.
const (
	CustomerActive    CustomerStatus = "active"
	CustomerSuspended CustomerStatus = "suspended"
	CustomerClosed    CustomerStatus = "closed"
)

// Valid reports whether s is a published customer status.
func (s CustomerStatus) Valid() bool {
	switch s {
	case CustomerActive, CustomerSuspended, CustomerClosed:
		return true
	default:
		return false
	}
}

// Rank orders the statuses from least to most restrictive (active < suspended < closed). It backs
// the effective-status rule of ADR-0006, where the more restrictive of customer and account wins.
func (s CustomerStatus) Rank() int { return statusRank(string(s)) }

// AccountStatus is the lifecycle state of an SMPP account (control_plane.smpp_accounts.status). It
// shares its values with CustomerStatus but is a distinct type: the effective status of an account
// is the more restrictive of the two, and the type system should not let them be confused.
type AccountStatus string

// The account lifecycle states, ordered least-to-most restrictive.
const (
	AccountActive    AccountStatus = "active"
	AccountSuspended AccountStatus = "suspended"
	AccountClosed    AccountStatus = "closed"
)

// Valid reports whether s is a published account status.
func (s AccountStatus) Valid() bool {
	switch s {
	case AccountActive, AccountSuspended, AccountClosed:
		return true
	default:
		return false
	}
}

// Rank orders the statuses from least to most restrictive (active < suspended < closed).
func (s AccountStatus) Rank() int { return statusRank(string(s)) }

// statusRank maps the shared active/suspended/closed vocabulary to a restrictiveness order. An
// unknown value ranks highest (most restrictive): failing closed is the safe direction.
func statusRank(s string) int {
	switch s {
	case "active":
		return 0
	case "suspended":
		return 1
	case "closed":
		return 2
	default:
		return 3
	}
}

// BillingMode is the MT billing model of a customer (control_plane.customers.billing_mode). It is
// nullable in the schema; a nil *BillingMode means "unset".
type BillingMode string

// The MT billing models.
const (
	BillingPrepaid  BillingMode = "prepaid"
	BillingPostpaid BillingMode = "postpaid"
)

// Valid reports whether m is a published billing mode.
func (m BillingMode) Valid() bool {
	switch m {
	case BillingPrepaid, BillingPostpaid:
		return true
	default:
		return false
	}
}

// BalanceScope names who owns a customer's balances (control_plane.customers.balance_scope).
type BalanceScope string

// The balance ownership scopes.
const (
	BalanceScopeCustomer    BalanceScope = "customer"
	BalanceScopeSMPPAccount BalanceScope = "smpp_account"
)

// Valid reports whether s is a published balance scope.
func (s BalanceScope) Valid() bool {
	switch s {
	case BalanceScopeCustomer, BalanceScopeSMPPAccount:
		return true
	default:
		return false
	}
}

// ContentStorage is a customer's body-storage policy (control_plane.customers.content_storage).
type ContentStorage string

// The content-storage policies.
const (
	ContentInherit         ContentStorage = "inherit"
	ContentOff             ContentStorage = "off"
	ContentStoredPlaintext ContentStorage = "stored_plaintext"
	ContentStoredEncrypted ContentStorage = "stored_encrypted"
)

// Valid reports whether c is a published content-storage policy.
func (c ContentStorage) Valid() bool {
	switch c {
	case ContentInherit, ContentOff, ContentStoredPlaintext, ContentStoredEncrypted:
		return true
	default:
		return false
	}
}

// BindType is an SMPP bind kind (control_plane.smpp_accounts.allowed_bind_types and
// control_plane.smsc_connectors.bind_type).
//
// Note: on an account the column is named allowed_bind_types (plural) but holds a single value, in
// both the DDL and the OpenAPI contract. It is a scalar, not a set — do not "fix" it to a slice.
type BindType string

// The SMPP bind kinds.
const (
	BindTX  BindType = "tx"
	BindRX  BindType = "rx"
	BindTRX BindType = "trx"
)

// Valid reports whether b is a published bind type.
func (b BindType) Valid() bool {
	switch b {
	case BindTX, BindRX, BindTRX:
		return true
	default:
		return false
	}
}

// SenderIDPolicy is an account's sender-ID enforcement policy
// (control_plane.smpp_accounts.sender_id_policy).
type SenderIDPolicy string

// The sender-ID policies.
const (
	SenderIDStrict               SenderIDPolicy = "strict"
	SenderIDAllowUnregisteredNum SenderIDPolicy = "allow_unregistered_numeric"
	SenderIDPolicyDisabled       SenderIDPolicy = "disabled"
)

// Valid reports whether p is a published sender-ID policy.
func (p SenderIDPolicy) Valid() bool {
	switch p {
	case SenderIDStrict, SenderIDAllowUnregisteredNum, SenderIDPolicyDisabled:
		return true
	default:
		return false
	}
}

// SenderIDStatus is the carrier-approval state of a sender ID (control_plane.sender_ids.status).
type SenderIDStatus string

// The sender-ID approval states.
const (
	SenderIDPendingCarrierApproval SenderIDStatus = "pending_carrier_approval"
	SenderIDActive                 SenderIDStatus = "active"
	SenderIDDisabled               SenderIDStatus = "disabled"
)

// Valid reports whether s is a published sender-ID status.
func (s SenderIDStatus) Valid() bool {
	switch s {
	case SenderIDPendingCarrierApproval, SenderIDActive, SenderIDDisabled:
		return true
	default:
		return false
	}
}

// CredentialType discriminates the two credential kinds an account has
// (control_plane.credentials.type). Exactly one row of each kind may exist per account.
type CredentialType string

// The credential kinds.
const (
	// #nosec G101 -- an enum discriminator matching the DDL, not a credential.
	CredentialSMPPBind CredentialType = "smpp_bind"
	CredentialAPIKey   CredentialType = "api_key"
)

// Valid reports whether t is a published credential type.
func (t CredentialType) Valid() bool {
	switch t {
	case CredentialSMPPBind, CredentialAPIKey:
		return true
	default:
		return false
	}
}

// CredentialStatus is a credential's lifecycle state (control_plane.credentials.status).
type CredentialStatus string

// The credential lifecycle states.
const (
	CredentialActive   CredentialStatus = "active"
	CredentialDisabled CredentialStatus = "disabled"
	CredentialRevoked  CredentialStatus = "revoked"
)

// Valid reports whether s is a published credential status.
func (s CredentialStatus) Valid() bool {
	switch s {
	case CredentialActive, CredentialDisabled, CredentialRevoked:
		return true
	default:
		return false
	}
}

// DistributionStrategy is a route's connector-selection strategy
// (control_plane.routes.distribution_strategy). Only 'static' is used at M1; the rest are reserved
// for later milestones and validated here so the type stays complete.
type DistributionStrategy string

// The distribution strategies.
const (
	DistributionStatic           DistributionStrategy = "static"
	DistributionRoundRobin       DistributionStrategy = "round_robin"
	DistributionWeighted         DistributionStrategy = "weighted"
	DistributionFailoverPriority DistributionStrategy = "failover_priority"
	DistributionLeastLoaded      DistributionStrategy = "least_loaded"
	DistributionHashBased        DistributionStrategy = "hash_based"
)

// Valid reports whether d is a published distribution strategy.
func (d DistributionStrategy) Valid() bool {
	switch d {
	case DistributionStatic, DistributionRoundRobin, DistributionWeighted,
		DistributionFailoverPriority, DistributionLeastLoaded, DistributionHashBased:
		return true
	default:
		return false
	}
}

// RouteStatus is a route's enablement state (control_plane.routes.status).
type RouteStatus string

// The route enablement states.
const (
	RouteActive   RouteStatus = "active"
	RouteDisabled RouteStatus = "disabled"
)

// Valid reports whether s is a published route status.
func (s RouteStatus) Valid() bool {
	switch s {
	case RouteActive, RouteDisabled:
		return true
	default:
		return false
	}
}

// NumberType is the kind of an inbound number (control_plane.inbound_numbers.number_type): a short
// code, a long code, or an alphanumeric sender the provider owns for the return path.
type NumberType string

// The inbound-number kinds.
const (
	NumberShortcode    NumberType = "shortcode"
	NumberLongcode     NumberType = "longcode"
	NumberAlphanumeric NumberType = "alphanumeric"
)

// Valid reports whether t is a published number type.
func (t NumberType) Valid() bool {
	switch t {
	case NumberShortcode, NumberLongcode, NumberAlphanumeric:
		return true
	default:
		return false
	}
}

// InboundNumberStatus is an inbound number's enablement state
// (control_plane.inbound_numbers.status).
type InboundNumberStatus string

// The inbound-number enablement states.
const (
	InboundNumberActive   InboundNumberStatus = "active"
	InboundNumberDisabled InboundNumberStatus = "disabled"
)

// Valid reports whether s is a published inbound-number status.
func (s InboundNumberStatus) Valid() bool {
	switch s {
	case InboundNumberActive, InboundNumberDisabled:
		return true
	default:
		return false
	}
}

// MatchType is how an inbound keyword is matched against a shared number's MO text
// (control_plane.inbound_keywords.match_type): an exact word, a leading prefix, or a regexp.
type MatchType string

// The inbound-keyword match modes.
const (
	MatchExact  MatchType = "exact"
	MatchPrefix MatchType = "prefix"
	MatchRegex  MatchType = "regex"
)

// Valid reports whether m is a published match type.
func (m MatchType) Valid() bool {
	switch m {
	case MatchExact, MatchPrefix, MatchRegex:
		return true
	default:
		return false
	}
}

// InboundKeywordStatus is an inbound keyword's enablement state
// (control_plane.inbound_keywords.status). It shares its values with InboundNumberStatus but is a
// distinct type: the two are not interchangeable and the type system should keep them apart.
type InboundKeywordStatus string

// The inbound-keyword enablement states.
const (
	InboundKeywordActive   InboundKeywordStatus = "active"
	InboundKeywordDisabled InboundKeywordStatus = "disabled"
)

// Valid reports whether s is a published inbound-keyword status.
func (s InboundKeywordStatus) Valid() bool {
	switch s {
	case InboundKeywordActive, InboundKeywordDisabled:
		return true
	default:
		return false
	}
}

// ConnectorStatus is an SMSC connector's coarse config status
// (control_plane.smsc_connectors.status). It is distinct from runtime health (link_status,
// breaker_state), which is never conflated with it.
type ConnectorStatus string

// The connector config statuses.
const (
	ConnectorActive   ConnectorStatus = "active"
	ConnectorDegraded ConnectorStatus = "degraded"
	ConnectorDisabled ConnectorStatus = "disabled"
)

// Valid reports whether s is a published connector status.
func (s ConnectorStatus) Valid() bool {
	switch s {
	case ConnectorActive, ConnectorDegraded, ConnectorDisabled:
		return true
	default:
		return false
	}
}

// SuppressionScope is the scope a suppression applies to (control_plane.suppressions.scope). A MT is
// blocked if the destination is suppressed in ANY applicable scope (§6.20).
type SuppressionScope string

// The suppression scopes, from narrowest to widest.
const (
	SuppressionScopeInboundNumber SuppressionScope = "inbound_number"
	SuppressionScopeAccount       SuppressionScope = "smpp_account"
	SuppressionScopeCustomer      SuppressionScope = "customer"
	SuppressionScopePlatform      SuppressionScope = "platform"
)

// Valid reports whether s is a published suppression scope.
func (s SuppressionScope) Valid() bool {
	switch s {
	case SuppressionScopeInboundNumber, SuppressionScopeAccount, SuppressionScopeCustomer, SuppressionScopePlatform:
		return true
	default:
		return false
	}
}

// SuppressionSource records how a suppression was created (control_plane.suppressions.source).
type SuppressionSource string

// The suppression sources.
const (
	SuppressionSourceMOStop    SuppressionSource = "mo_stop"
	SuppressionSourceAdmin     SuppressionSource = "admin"
	SuppressionSourceImport    SuppressionSource = "import"
	SuppressionSourceCarrier   SuppressionSource = "carrier"
	SuppressionSourceRegulator SuppressionSource = "regulator"
)

// Valid reports whether s is a published suppression source.
func (s SuppressionSource) Valid() bool {
	switch s {
	case SuppressionSourceMOStop, SuppressionSourceAdmin, SuppressionSourceImport,
		SuppressionSourceCarrier, SuppressionSourceRegulator:
		return true
	default:
		return false
	}
}

// OptOutAction is what an opt-out keyword does when matched (control_plane.opt_out_keywords.action).
type OptOutAction string

// The opt-out keyword actions.
const (
	OptOutActionSuppress   OptOutAction = "suppress"
	OptOutActionUnsuppress OptOutAction = "unsuppress"
	OptOutActionHelp       OptOutAction = "help"
)

// Valid reports whether a is a published opt-out action.
func (a OptOutAction) Valid() bool {
	switch a {
	case OptOutActionSuppress, OptOutActionUnsuppress, OptOutActionHelp:
		return true
	default:
		return false
	}
}

// OptOutMatchType is how an opt-out keyword matches an inbound message
// (control_plane.opt_out_keywords.match_type).
type OptOutMatchType string

// The opt-out keyword match types.
const (
	OptOutMatchExact  OptOutMatchType = "exact"
	OptOutMatchPrefix OptOutMatchType = "prefix"
)

// Valid reports whether m is a published opt-out match type.
func (m OptOutMatchType) Valid() bool {
	switch m {
	case OptOutMatchExact, OptOutMatchPrefix:
		return true
	default:
		return false
	}
}

// OptOutKeywordStatus is an opt-out keyword's config status (control_plane.opt_out_keywords.status).
type OptOutKeywordStatus string

// The opt-out keyword statuses.
const (
	OptOutKeywordActive   OptOutKeywordStatus = "active"
	OptOutKeywordDisabled OptOutKeywordStatus = "disabled"
)

// Valid reports whether s is a published opt-out keyword status.
func (s OptOutKeywordStatus) Valid() bool {
	switch s {
	case OptOutKeywordActive, OptOutKeywordDisabled:
		return true
	default:
		return false
	}
}
