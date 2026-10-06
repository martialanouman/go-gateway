// Package antispam is the real implementation behind the frozen pipeline.anti_spam stage. It
// evaluates a message against the active anti-spam rules — content blacklists (precompiled regex,
// matched in memory), duplicates (a fingerprint recorded in Redis with a TTL), velocity (sliding
// window per source/account, atomic Lua), reputation (a per-source score) and category_mismatch (the
// traffic against its sender's declared category, ADR-0020 §5) — and reports the action to take: block,
// flag or throttle. Rules are recompiled on each config invalidation (Holder) and resolved most specific
// first (account, then customer, then global).
//
// Content and category_mismatch rules are always enforced. The Redis-backed rules FAIL OPEN (§1.5, availability first): a
// store fault flags the message rather than blocking it, and never errors, while the content rules
// stay in force. This is the opposite of the rate-limit stage, which is fail-closed (M6).
//
// Invariant (a): the message body is read in memory only. It is never logged, never stored; the
// duplicate fingerprint is a one-way hash of (scope, destination, body) — never the body itself.
package antispam

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
)

// RuleLister loads the active anti-spam rules. *postgres.AntispamRuleRepo satisfies it.
type RuleLister interface {
	ListActive(ctx context.Context) ([]cp.AntispamRule, error)
}

// StateStore is the shared Redis-backed anti-spam state: duplicate fingerprints, sliding-window
// velocity counters, and reputation scores. *RedisState satisfies it. A nil store disables every
// Redis-backed rule (content rules still apply).
type StateStore interface {
	Seen(ctx context.Context, fingerprint string, messageID uuid.UUID, window time.Duration) (bool, error)
	Hit(ctx context.Context, key string, messageID uuid.UUID, window time.Duration) (int, error)
	Reputation(ctx context.Context, source string) (score int, found bool, err error)
	// CountCategoryMismatch counts messageID once in the sender's recent matches; counted is false for a
	// message already counted (a redelivery).
	CountCategoryMismatch(ctx context.Context, messageID, customerID uuid.UUID, address string) (counted bool, err error)
}

// Metric counts anti-spam events with bounded labels — never the body or a MSISDN (invariant a). A
// nil metric defaults to a no-op.
type Metric interface {
	// FailOpen records that a Redis-backed check could not run and the message was let through
	// (flagged) rather than blocked (§1.5: velocity anti-spam is fail-open).
	FailOpen()
	// CategoryMismatch records that a category_mismatch rule matched, with the action it took.
	CategoryMismatch(action cp.AntispamAction)
}

type noopMetric struct{}

func (noopMetric) FailOpen()                          {}
func (noopMetric) CategoryMismatch(cp.AntispamAction) {}

type contentRule struct {
	action   cp.AntispamAction
	patterns []*regexp.Regexp
}

type duplicateRule struct {
	action cp.AntispamAction
	window time.Duration
}

// velocityRule counts events per source or per account in a sliding window; over max triggers action.
type velocityRule struct {
	action   cp.AntispamAction
	max      int
	window   time.Duration
	bySource bool // key dimension: true = per source (From), false = per account
}

type reputationRule struct {
	action   cp.AntispamAction
	minScore int
}

// categoryRule checks a sender's traffic against the category it declares (ADR-0020 §5).
type categoryRule struct {
	action       cp.AntispamAction
	otpCode      *regexp.Regexp
	otpMaxLength int
	promoMarkers []string // lower-cased
}

// Engine evaluates a message against the compiled rules. It is immutable after New (safe for
// concurrent reads); the Redis-backed checks (duplicate, velocity, reputation) delegate to the state
// store and FAIL OPEN — a store fault flags the message rather than blocking it (§1.5).
type Engine struct {
	content    map[string][]contentRule
	dup        map[string]duplicateRule  // most-specific duplicate rule per scope key
	velocity   map[string]velocityRule   // most-specific velocity rule per scope key
	reputation map[string]reputationRule // most-specific reputation rule per scope key
	category   map[string]categoryRule   // most-specific category_mismatch rule per scope key
	state      StateStore
	metric     Metric
	logger     *slog.Logger
}

// New compiles the active rules into an engine. A content rule with an invalid regex, or a rule with
// an unparseable config, is dropped with a warning rather than failing the whole load — one bad admin
// row must not disable anti-spam entirely. state may be nil, which disables every Redis-backed rule
// (content rules still apply); a nil metric defaults to a no-op.
func New(ctx context.Context, lister RuleLister, state StateStore, metric Metric, logger *slog.Logger) (*Engine, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if metric == nil {
		metric = noopMetric{}
	}
	rules, err := lister.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("antispam: load rules: %w", err)
	}

	e := &Engine{
		content:    make(map[string][]contentRule),
		dup:        make(map[string]duplicateRule),
		velocity:   make(map[string]velocityRule),
		reputation: make(map[string]reputationRule),
		category:   make(map[string]categoryRule),
		state:      state,
		metric:     metric,
		logger:     logger,
	}
	for _, r := range rules {
		key := scopeKey(r.Scope, r.ScopeID)
		switch r.RuleType {
		case cp.AntispamContentBlacklist:
			if cr, ok := compileContent(logger, r); ok {
				e.content[key] = append(e.content[key], cr)
			}
		case cp.AntispamDuplicate:
			// One rule per scope key (rules are ordered, so the first is deterministic); a single check
			// per scope avoids conflicting windows for the same fingerprint.
			if dr, ok := compileDuplicate(logger, r); ok {
				if _, exists := e.dup[key]; !exists {
					e.dup[key] = dr
				}
			}
		case cp.AntispamVelocity:
			if vr, ok := compileVelocity(logger, r); ok {
				if _, exists := e.velocity[key]; !exists {
					e.velocity[key] = vr
				}
			}
		case cp.AntispamReputation:
			if rr, ok := compileReputation(logger, r); ok {
				if _, exists := e.reputation[key]; !exists {
					e.reputation[key] = rr
				}
			}
		case cp.AntispamCategoryMismatch:
			if cr, ok := compileCategory(logger, r); ok {
				if _, exists := e.category[key]; !exists {
					e.category[key] = cr
				}
			}
		}
	}
	return e, nil
}

// Evaluate returns the action to take for a message from the given sender (from), declared under
// category, and (accountID, customerID) to dest with the given body. Content and category_mismatch rules
// (static, in memory) are always enforced. The
// Redis-backed rules — duplicate, velocity, reputation — are evaluated most-specific first and FAIL
// OPEN: a store fault does not block or error, it flags the message (§1.5, availability first) while
// the content rules stay in force. The returned action is the most restrictive that applied. The
// error return is retained for interface stability; it is currently always nil.
func (e *Engine) Evaluate(ctx context.Context, messageID, accountID, customerID uuid.UUID, from string, category cp.TrafficCategory, dest string, body []byte) (cp.AntispamAction, error) {
	scopes := []string{scopeKey(cp.AntispamScopeAccount, &accountID), scopeKey(cp.AntispamScopeCustomer, &customerID), globalKey}

	action := contentAction(e.content, scopes, body)
	if mismatch := e.categoryMismatch(scopes, category, body); mismatch != "" {
		if e.countMismatch(ctx, messageID, customerID, from) {
			e.metric.CategoryMismatch(mismatch)
		}
		action = moreRestrictive(action, mismatch)
	}

	// A content block is the most restrictive outcome — no Redis-backed rule can change it, and
	// skipping them avoids side-effecting state (a fingerprint / velocity hit) for a rejected message.
	if action == cp.AntispamActionBlock || e.state == nil {
		return action, nil
	}

	// Key the velocity and reputation on the CANONICAL source, the same form the MO path records
	// (e164.NormalizeAddr), so a sender's MT and MO traffic — and two spellings of one MSISDN — share
	// one counter instead of splitting silently.
	source := e164.NormalizeAddr(from)

	failedOpen := false
	fail := func() { failedOpen = true }

	// Each Redis-backed rule can only raise the action; a block from any of them is terminal, so stop
	// once reached to avoid side-effecting the remaining checks for an already-rejected message.
	if action = moreRestrictive(action, e.evalDuplicate(ctx, scopes, messageID, dest, body, fail)); action != cp.AntispamActionBlock {
		if action = moreRestrictive(action, e.evalVelocity(ctx, scopes, messageID, source, accountID, fail)); action != cp.AntispamActionBlock {
			action = moreRestrictive(action, e.evalReputation(ctx, scopes, source, fail))
		}
	}

	// Fail open: a Redis fault let a Redis-backed rule through. Flag the message (a metric and, if no
	// stricter action applied, the flag action) so the pass is observable — never block on it.
	if failedOpen {
		e.metric.FailOpen()
		action = moreRestrictive(action, cp.AntispamActionFlag)
	}
	return action, nil
}

// categoryMismatch returns the action of the most-specific category_mismatch rule when the body does not
// look like the sender's declared category, or "".
func (e *Engine) categoryMismatch(scopes []string, category cp.TrafficCategory, body []byte) cp.AntispamAction {
	for _, sk := range scopes {
		cr, ok := e.category[sk]
		if !ok {
			continue
		}
		if cr.mismatches(category, body) {
			return cr.action
		}
		return ""
	}
	return ""
}

// urlPattern also catches a link without a scheme ("bit.ly/x9"): a dotted name ending in a 2+ letter
// label, followed by a path or the end of a word.
var urlPattern = regexp.MustCompile(`(?i)(https?://|www\.)\S|\b[a-z][a-z0-9-]*(\.[a-z0-9-]+)*\.[a-z]{2,}(/|$)`)

// mismatches says whether body does not look like category. Marketing, the most constrained category, is
// never checked; neither is a message with no category.
func (c categoryRule) mismatches(category cp.TrafficCategory, body []byte) bool {
	switch category {
	case cp.TrafficOTP:
		return !c.otpCode.Match(joinDigitGroups(body)) || urlPattern.Match(body) ||
			utf8.RuneCount(body) > c.otpMaxLength
	case cp.TrafficTransactional:
		lower := strings.ToLower(string(body))
		for _, m := range c.promoMarkers {
			if strings.Contains(lower, m) {
				return true
			}
		}
	}
	return false
}

// joinDigitGroups drops a single space or dash between two digits, so a code written "123 456" or
// "123-456" is read as the 6-digit code it is.
func joinDigitGroups(body []byte) []byte {
	out := make([]byte, 0, len(body))
	for i, b := range body {
		if (b == ' ' || b == '-') && i > 0 && i+1 < len(body) && isDigit(body[i-1]) && isDigit(body[i+1]) {
			continue
		}
		out = append(out, b)
	}
	return out
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// countMismatch adds the match to the sender's recent counter and reports whether the metric should count
// it too: not for a redelivered message, already counted. It fails open — the counter is a signal for the
// operator, and a store fault must not hold the message back nor hide the match from the metric.
func (e *Engine) countMismatch(ctx context.Context, messageID, customerID uuid.UUID, from string) bool {
	if e.state == nil {
		return true
	}
	counted, err := e.state.CountCategoryMismatch(ctx, messageID, customerID, from)
	if err != nil {
		e.logger.WarnContext(ctx, "antispam: category mismatch count failed", "err", err)
		return true
	}
	return counted
}

// evalDuplicate returns the action of the most-specific applicable duplicate rule when the message is
// a duplicate, or "". The fingerprint is namespaced by the rule's scope key, so a tenant-scoped rule
// deduplicates only within its own tenant. On a store fault it calls fail and returns "".
func (e *Engine) evalDuplicate(ctx context.Context, scopes []string, messageID uuid.UUID, dest string, body []byte, fail func()) cp.AntispamAction {
	for _, sk := range scopes {
		dr, ok := e.dup[sk]
		if !ok {
			continue
		}
		seen, err := e.state.Seen(ctx, fingerprint(sk, dest, body), messageID, dr.window)
		if err != nil {
			e.logger.WarnContext(ctx, "antispam: duplicate check failed open", "err", err)
			fail()
			return ""
		}
		if seen {
			return dr.action
		}
		return ""
	}
	return ""
}

// evalVelocity returns the action of the most-specific applicable velocity rule when the source (or
// account) exceeds its sliding-window limit, or "". The counter key is namespaced by the rule's scope
// so tenants are isolated; a global "by source" rule shares the key inbound MO counting writes to.
func (e *Engine) evalVelocity(ctx context.Context, scopes []string, messageID uuid.UUID, from string, accountID uuid.UUID, fail func()) cp.AntispamAction {
	for _, sk := range scopes {
		vr, ok := e.velocity[sk]
		if !ok {
			continue
		}
		n, err := e.state.Hit(ctx, velocityKey(sk, vr.bySource, from, accountID), messageID, vr.window)
		if err != nil {
			e.logger.WarnContext(ctx, "antispam: velocity check failed open", "err", err)
			fail()
			return ""
		}
		if n > vr.max {
			return vr.action
		}
		return ""
	}
	return ""
}

// evalReputation returns the action of the most-specific applicable reputation rule when the source's
// score is below the rule's threshold, or "". An unscored source is neutral (passes).
func (e *Engine) evalReputation(ctx context.Context, scopes []string, from string, fail func()) cp.AntispamAction {
	for _, sk := range scopes {
		rr, ok := e.reputation[sk]
		if !ok {
			continue
		}
		score, found, err := e.state.Reputation(ctx, from)
		if err != nil {
			e.logger.WarnContext(ctx, "antispam: reputation check failed open", "err", err)
			fail()
			return ""
		}
		if found && score < rr.minScore {
			return rr.action
		}
		return ""
	}
	return ""
}

// velocityKey namespaces a velocity counter by the rule's scope and its counting dimension. A global
// "by source" rule keys on "global:source:<from>", the exact key inbound MO counting writes to
// (RecordMOSource), so a sender's MT and MO traffic share one window.
func velocityKey(scopeKey string, bySource bool, from string, accountID uuid.UUID) string {
	if bySource {
		return scopeKey + ":source:" + from
	}
	return scopeKey + ":account:" + accountID.String()
}

// MOSourceVelocityKey is the counter key inbound MO records into: a source's global velocity, so a
// global "by source" MT rule counts the sender's MT and MO traffic together. The source is
// canonicalized (e164.NormalizeAddr) to match the form the MT path keys on.
func MOSourceVelocityKey(from string) string {
	return globalKey + ":source:" + e164.NormalizeAddr(from)
}

// contentAction returns the action for the body: the most-specific scope that has any matching
// content rule wins (account > customer > global), and within that scope the most restrictive action
// among the matching rules (block > throttle > flag). Empty when nothing matches.
func contentAction(byScope map[string][]contentRule, scopes []string, body []byte) cp.AntispamAction {
	for _, sk := range scopes {
		var best cp.AntispamAction
		for _, cr := range byScope[sk] {
			if cr.matches(body) {
				best = moreRestrictive(best, cr.action)
			}
		}
		if best != "" {
			return best
		}
	}
	return ""
}

func (c contentRule) matches(body []byte) bool {
	for _, re := range c.patterns {
		if re.Match(body) {
			return true
		}
	}
	return false
}

// moreRestrictive returns the more restrictive of two actions: block > throttle > flag > none.
func moreRestrictive(a, b cp.AntispamAction) cp.AntispamAction {
	if severity(b) > severity(a) {
		return b
	}
	return a
}

func severity(a cp.AntispamAction) int {
	switch a {
	case cp.AntispamActionBlock:
		return 3
	case cp.AntispamActionThrottle:
		return 2
	case cp.AntispamActionFlag:
		return 1
	default:
		return 0
	}
}

const globalKey = "global"

// scopeKey namespaces a rule by its scope so two scopes' rules never collide.
func scopeKey(scope cp.AntispamScope, scopeID *uuid.UUID) string {
	if scope == cp.AntispamScopeGlobal || scopeID == nil {
		return globalKey
	}
	return string(scope) + ":" + scopeID.String()
}

// fingerprint is the duplicate key: a one-way hash of (scope key, destination, body). The scope key
// namespaces the hash so a tenant-scoped rule never deduplicates across tenants. The body never
// appears in the key (invariant a).
func fingerprint(scopeKey, dest string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(scopeKey))
	h.Write([]byte{0})
	h.Write([]byte(dest))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// --- config parsing ---

type contentConfig struct {
	Patterns []string `json:"patterns"`
}

func compileContent(logger *slog.Logger, r cp.AntispamRule) (contentRule, bool) {
	var cfg contentConfig
	if err := json.Unmarshal(r.ConfigJSON, &cfg); err != nil {
		logger.Warn("antispam: dropping content rule with bad config", "rule_id", r.ID, "err", err)
		return contentRule{}, false
	}
	patterns := make([]*regexp.Regexp, 0, len(cfg.Patterns))
	for _, p := range cfg.Patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			logger.Warn("antispam: dropping content rule with invalid regex", "rule_id", r.ID, "err", err)
			return contentRule{}, false
		}
		patterns = append(patterns, re)
	}
	if len(patterns) == 0 {
		return contentRule{}, false
	}
	return contentRule{action: r.Action, patterns: patterns}, true
}

type duplicateConfig struct {
	WindowSeconds int `json:"window_seconds"`
}

func compileDuplicate(logger *slog.Logger, r cp.AntispamRule) (duplicateRule, bool) {
	var cfg duplicateConfig
	if err := json.Unmarshal(r.ConfigJSON, &cfg); err != nil {
		logger.Warn("antispam: dropping duplicate rule with bad config", "rule_id", r.ID, "err", err)
		return duplicateRule{}, false
	}
	if cfg.WindowSeconds <= 0 {
		logger.Warn("antispam: dropping duplicate rule with non-positive window", "rule_id", r.ID)
		return duplicateRule{}, false
	}
	return duplicateRule{action: r.Action, window: time.Duration(cfg.WindowSeconds) * time.Second}, true
}

type velocityConfig struct {
	Max           int    `json:"max"`
	WindowSeconds int    `json:"window_seconds"`
	By            string `json:"by"` // "source" (default) or "account"
}

func compileVelocity(logger *slog.Logger, r cp.AntispamRule) (velocityRule, bool) {
	var cfg velocityConfig
	if err := json.Unmarshal(r.ConfigJSON, &cfg); err != nil {
		logger.Warn("antispam: dropping velocity rule with bad config", "rule_id", r.ID, "err", err)
		return velocityRule{}, false
	}
	if cfg.Max <= 0 || cfg.WindowSeconds <= 0 {
		logger.Warn("antispam: dropping velocity rule with non-positive max/window", "rule_id", r.ID)
		return velocityRule{}, false
	}
	// A window longer than the record TTL cannot be counted reliably (older events are already gone).
	if time.Duration(cfg.WindowSeconds)*time.Second > recordMaxTTL {
		logger.Warn("antispam: dropping velocity rule whose window exceeds the max retention", "rule_id", r.ID)
		return velocityRule{}, false
	}
	return velocityRule{
		action:   r.Action,
		max:      cfg.Max,
		window:   time.Duration(cfg.WindowSeconds) * time.Second,
		bySource: cfg.By != "account", // default and any non-"account" value means per source
	}, true
}

// ValidateRuleConfig validates a rule's config_json for its type, so the Admin API (step-067) rejects
// a bad rule at write time instead of the engine silently dropping it at load. It is the single
// source of truth for what a well-formed rule config is. An empty/"{}" config is valid only for types
// that need no parameters (none currently — all require a field), so a missing field is an error.
func ValidateRuleConfig(ruleType cp.AntispamRuleType, config json.RawMessage) error {
	switch ruleType {
	case cp.AntispamContentBlacklist:
		var cfg contentConfig
		if err := json.Unmarshal(config, &cfg); err != nil {
			return fmt.Errorf("content rule: invalid config: %w", err)
		}
		if len(cfg.Patterns) == 0 {
			return errors.New("content rule: at least one pattern is required")
		}
		for _, p := range cfg.Patterns {
			if _, err := regexp.Compile(p); err != nil {
				return fmt.Errorf("content rule: pattern %q does not compile: %w", p, err)
			}
		}
	case cp.AntispamDuplicate:
		var cfg duplicateConfig
		if err := json.Unmarshal(config, &cfg); err != nil {
			return fmt.Errorf("duplicate rule: invalid config: %w", err)
		}
		if cfg.WindowSeconds <= 0 {
			return errors.New("duplicate rule: window_seconds must be positive")
		}
	case cp.AntispamVelocity:
		var cfg velocityConfig
		if err := json.Unmarshal(config, &cfg); err != nil {
			return fmt.Errorf("velocity rule: invalid config: %w", err)
		}
		if cfg.Max <= 0 || cfg.WindowSeconds <= 0 {
			return errors.New("velocity rule: max and window_seconds must be positive")
		}
		if time.Duration(cfg.WindowSeconds)*time.Second > recordMaxTTL {
			return fmt.Errorf("velocity rule: window_seconds must not exceed %d", int(recordMaxTTL.Seconds()))
		}
		if cfg.By != "" && cfg.By != "source" && cfg.By != "account" {
			return errors.New(`velocity rule: "by" must be "source" or "account"`)
		}
	case cp.AntispamCategoryMismatch:
		if _, err := parseCategoryConfig(config); err != nil {
			return err
		}
	case cp.AntispamReputation:
		var cfg reputationConfig
		if err := json.Unmarshal(config, &cfg); err != nil {
			return fmt.Errorf("reputation rule: invalid config: %w", err)
		}
		// min_score is required but may be any integer (reputation scores can be negative): its absence
		// would silently make the rule a no-op, so reject it.
		if cfg.MinScore == nil {
			return errors.New("reputation rule: min_score is required")
		}
	default:
		return fmt.Errorf("unknown rule type %q", ruleType)
	}
	return nil
}

type reputationConfig struct {
	MinScore *int `json:"min_score"`
}

func compileReputation(logger *slog.Logger, r cp.AntispamRule) (reputationRule, bool) {
	var cfg reputationConfig
	if err := json.Unmarshal(r.ConfigJSON, &cfg); err != nil {
		logger.Warn("antispam: dropping reputation rule with bad config", "rule_id", r.ID, "err", err)
		return reputationRule{}, false
	}
	if cfg.MinScore == nil {
		logger.Warn("antispam: dropping reputation rule without min_score", "rule_id", r.ID)
		return reputationRule{}, false
	}
	return reputationRule{action: r.Action, minScore: *cfg.MinScore}, true
}

// maxOTPCodeDigits bounds the code length a rule may ask for, far below RE2's repeat limit of 1000: past
// it, building the code pattern panics, and one bad row would stop the router.
const maxOTPCodeDigits = 32

type categoryConfig struct {
	OTPCodeMinDigits *int     `json:"otp_code_min_digits"`
	OTPCodeMaxDigits *int     `json:"otp_code_max_digits"`
	OTPMaxLength     *int     `json:"otp_max_length"`
	PromoMarkers     []string `json:"promo_markers"`
}

// parseCategoryConfig applies the ADR-0020 §5 defaults (a 4-8 digit code, 160 characters, no marker) and
// rejects what would make the rule meaningless.
func parseCategoryConfig(raw json.RawMessage) (categoryRule, error) {
	var cfg categoryConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return categoryRule{}, fmt.Errorf("category_mismatch rule: invalid config: %w", err)
	}
	minDigits, maxDigits, maxLength := 4, 8, 160
	if cfg.OTPCodeMinDigits != nil {
		minDigits = *cfg.OTPCodeMinDigits
	}
	if cfg.OTPCodeMaxDigits != nil {
		maxDigits = *cfg.OTPCodeMaxDigits
	}
	if cfg.OTPMaxLength != nil {
		maxLength = *cfg.OTPMaxLength
	}
	if minDigits <= 0 || maxDigits < minDigits || maxDigits > maxOTPCodeDigits {
		return categoryRule{}, fmt.Errorf("category_mismatch rule: otp code digits must satisfy 0 < min <= max <= %d", maxOTPCodeDigits)
	}
	if maxLength <= 0 {
		return categoryRule{}, errors.New("category_mismatch rule: otp_max_length must be positive")
	}
	markers := make([]string, 0, len(cfg.PromoMarkers))
	for _, m := range cfg.PromoMarkers {
		if strings.TrimSpace(m) == "" {
			return categoryRule{}, errors.New("category_mismatch rule: a promo marker must not be empty")
		}
		markers = append(markers, strings.ToLower(m))
	}
	code := regexp.MustCompile(fmt.Sprintf(`(?:^|\D)\d{%d,%d}(?:\D|$)`, minDigits, maxDigits))
	return categoryRule{otpCode: code, otpMaxLength: maxLength, promoMarkers: markers}, nil
}

func compileCategory(logger *slog.Logger, r cp.AntispamRule) (categoryRule, bool) {
	cr, err := parseCategoryConfig(r.ConfigJSON)
	if err != nil {
		logger.Warn("antispam: dropping category_mismatch rule with bad config", "rule_id", r.ID, "err", err)
		return categoryRule{}, false
	}
	cr.action = r.Action
	return cr, true
}

// Holder keeps the current Engine behind an atomic pointer: built at boot, swapped on each config
// invalidation, so a rule created from the dashboard reaches the next message.
type Holder struct {
	engine atomic.Pointer[Engine]
}

// Store swaps in a freshly built engine.
func (h *Holder) Store(e *Engine) { h.engine.Store(e) }

// Evaluate checks against the current engine.
func (h *Holder) Evaluate(ctx context.Context, messageID, accountID, customerID uuid.UUID, from string, category cp.TrafficCategory, dest string, body []byte) (cp.AntispamAction, error) {
	return h.engine.Load().Evaluate(ctx, messageID, accountID, customerID, from, category, dest, body)
}
