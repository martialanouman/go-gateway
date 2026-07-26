// Package antispam is the real implementation behind the frozen pipeline.anti_spam stage (step-065).
// It evaluates a message against the active anti-spam rules — content blacklists (precompiled regex,
// matched in memory) and duplicates (a fingerprint recorded in Redis with a TTL) — and reports the
// action to take: block, flag or throttle. Rules are compiled once at startup and resolved most
// specific first (account, then customer, then global). Velocity and reputation land in step-066.
//
// Invariant (a): the message body is read in memory only. It is never logged, never stored, and the
// duplicate fingerprint is a one-way hash of (destination, body) — never the body itself.
package antispam

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// RuleLister loads the active anti-spam rules. *postgres.AntispamRuleRepo satisfies it.
type RuleLister interface {
	ListActive(ctx context.Context) ([]cp.AntispamRule, error)
}

// DuplicateChecker records a message fingerprint and reports whether it was already present within
// the window (an atomic SET NX EX). *RedisDuplicateChecker satisfies it.
type DuplicateChecker interface {
	Seen(ctx context.Context, fingerprint string, window time.Duration) (bool, error)
}

type contentRule struct {
	action   cp.AntispamAction
	patterns []*regexp.Regexp
}

type duplicateRule struct {
	action cp.AntispamAction
	window time.Duration
}

// Engine evaluates a message against the compiled rules. It is immutable after New (safe for
// concurrent reads); the duplicate check delegates to Redis.
type Engine struct {
	content map[string][]contentRule
	dup     map[string]duplicateRule // most-specific duplicate rule per scope key
	checker DuplicateChecker
	logger  *slog.Logger
}

// New compiles the active rules into an engine. A content rule with an invalid regex, or a rule with
// an unparseable config, is dropped with a warning rather than failing the whole load — one bad
// admin row must not disable anti-spam entirely. checker may be nil, which disables duplicate rules.
func New(ctx context.Context, lister RuleLister, checker DuplicateChecker, logger *slog.Logger) (*Engine, error) {
	if logger == nil {
		logger = slog.Default()
	}
	rules, err := lister.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("antispam: load rules: %w", err)
	}

	e := &Engine{
		content: make(map[string][]contentRule),
		dup:     make(map[string]duplicateRule),
		checker: checker,
		logger:  logger,
	}
	for _, r := range rules {
		key := scopeKey(r.Scope, r.ScopeID)
		switch r.RuleType {
		case cp.AntispamContentBlacklist:
			cr, ok := compileContent(logger, r)
			if ok {
				e.content[key] = append(e.content[key], cr)
			}
		case cp.AntispamDuplicate:
			dr, ok := compileDuplicate(logger, r)
			if ok {
				// Keep only the first (rules are ordered so this is deterministic); a single duplicate rule
				// per scope avoids conflicting SET NX windows for the same fingerprint.
				if _, exists := e.dup[key]; !exists {
					e.dup[key] = dr
				}
			}
		default:
			// velocity / reputation are step-066.
		}
	}
	return e, nil
}

// Evaluate returns the action to take for a message from (accountID, customerID) to dest with the
// given body. It resolves the most-specific content rule that matches and the most-specific
// applicable duplicate rule, and returns the most restrictive of the two actions (block > throttle >
// flag). A Redis fault on the duplicate check is returned as an error for the caller to treat as
// transient — the message is neither passed nor blocked on an infrastructure blip.
func (e *Engine) Evaluate(ctx context.Context, accountID, customerID uuid.UUID, dest string, body []byte) (cp.AntispamAction, error) {
	scopes := []string{scopeKey(cp.AntispamScopeAccount, &accountID), scopeKey(cp.AntispamScopeCustomer, &customerID), globalKey}

	action := contentAction(e.content, scopes, body)

	// A content block is already the most restrictive outcome — no duplicate check can change it, and
	// skipping it avoids posting a fingerprint key for a message we reject anyway.
	if action == cp.AntispamActionBlock || e.checker == nil {
		return action, nil
	}

	// The most-specific applicable duplicate rule wins; check its fingerprint once (a single SET NX so
	// two rules never fight over the same key). The fingerprint is namespaced by that rule's scope key,
	// so an account/customer rule deduplicates only within its own tenant — a global rule shares the
	// "global" namespace by design (platform-wide dedup).
	for _, sk := range scopes {
		dr, ok := e.dup[sk]
		if !ok {
			continue
		}
		seen, err := e.checker.Seen(ctx, fingerprint(sk, dest, body), dr.window)
		if err != nil {
			return "", fmt.Errorf("antispam: duplicate check: %w", err)
		}
		if seen {
			action = moreRestrictive(action, dr.action)
		}
		break
	}

	return action, nil
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
