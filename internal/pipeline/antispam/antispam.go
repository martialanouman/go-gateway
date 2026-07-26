// Package antispam is the real implementation behind the frozen pipeline.anti_spam stage. It
// evaluates a message against the active anti-spam rules — content blacklists (precompiled regex,
// matched in memory), duplicates (a fingerprint recorded in Redis with a TTL), velocity (sliding
// window per source/account, atomic Lua) and reputation (a per-source score) — and reports the action
// to take: block, flag or throttle. Rules are compiled once at startup and resolved most specific
// first (account, then customer, then global).
//
// Content rules are always enforced. The Redis-backed rules FAIL OPEN (§1.5, availability first): a
// store fault flags the message rather than blocking it, and never errors, while the content rules
// stay in force. This is the opposite of the rate-limit stage, which is fail-closed (M6).
//
// Invariant (a): the message body is read in memory only. It is never logged, never stored; the
// duplicate fingerprint is a one-way hash of (scope, destination, body) — never the body itself.
package antispam

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

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
	Seen(ctx context.Context, fingerprint string, window time.Duration) (bool, error)
	Hit(ctx context.Context, key string, window time.Duration) (int, error)
	Reputation(ctx context.Context, source string) (score int, found bool, err error)
}

// Metric counts anti-spam events with bounded labels — never the body or a MSISDN (invariant a). A
// nil metric defaults to a no-op.
type Metric interface {
	// FailOpen records that a Redis-backed check could not run and the message was let through
	// (flagged) rather than blocked (§1.5: velocity anti-spam is fail-open).
	FailOpen()
}

type noopMetric struct{}

func (noopMetric) FailOpen() {}

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

// Engine evaluates a message against the compiled rules. It is immutable after New (safe for
// concurrent reads); the Redis-backed checks (duplicate, velocity, reputation) delegate to the state
// store and FAIL OPEN — a store fault flags the message rather than blocking it (§1.5).
type Engine struct {
	content    map[string][]contentRule
	dup        map[string]duplicateRule  // most-specific duplicate rule per scope key
	velocity   map[string]velocityRule   // most-specific velocity rule per scope key
	reputation map[string]reputationRule // most-specific reputation rule per scope key
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
		}
	}
	return e, nil
}

// Evaluate returns the action to take for a message from the given sender (from) and (accountID,
// customerID) to dest with the given body. Content rules (static, in memory) are always enforced. The
// Redis-backed rules — duplicate, velocity, reputation — are evaluated most-specific first and FAIL
// OPEN: a store fault does not block or error, it flags the message (§1.5, availability first) while
// the content rules stay in force. The returned action is the most restrictive that applied. The
// error return is retained for interface stability; it is currently always nil.
func (e *Engine) Evaluate(ctx context.Context, accountID, customerID uuid.UUID, from, dest string, body []byte) (cp.AntispamAction, error) {
	scopes := []string{scopeKey(cp.AntispamScopeAccount, &accountID), scopeKey(cp.AntispamScopeCustomer, &customerID), globalKey}

	action := contentAction(e.content, scopes, body)

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
	if action = moreRestrictive(action, e.evalDuplicate(ctx, scopes, dest, body, fail)); action != cp.AntispamActionBlock {
		if action = moreRestrictive(action, e.evalVelocity(ctx, scopes, source, accountID, fail)); action != cp.AntispamActionBlock {
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

// evalDuplicate returns the action of the most-specific applicable duplicate rule when the message is
// a duplicate, or "". The fingerprint is namespaced by the rule's scope key, so a tenant-scoped rule
// deduplicates only within its own tenant. On a store fault it calls fail and returns "".
func (e *Engine) evalDuplicate(ctx context.Context, scopes []string, dest string, body []byte, fail func()) cp.AntispamAction {
	for _, sk := range scopes {
		dr, ok := e.dup[sk]
		if !ok {
			continue
		}
		seen, err := e.state.Seen(ctx, fingerprint(sk, dest, body), dr.window)
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
func (e *Engine) evalVelocity(ctx context.Context, scopes []string, from string, accountID uuid.UUID, fail func()) cp.AntispamAction {
	for _, sk := range scopes {
		vr, ok := e.velocity[sk]
		if !ok {
			continue
		}
		n, err := e.state.Hit(ctx, velocityKey(sk, vr.bySource, from, accountID), vr.window)
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
