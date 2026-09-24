// Package senderrewrite evaluates the sender-ID rewrite rules of §6.16: connector-pool applies them just
// before the submit_sm, and the Admin API's test operation runs one rule through the same code, so an
// operator sees exactly what the pool would send.
package senderrewrite

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// rule is a rewrite rule with its patterns compiled once.
type rule struct {
	cp.SenderRewriteRule
	sender, dest *regexp.Regexp
	allowed      string
}

// Snapshot is an immutable, precedence-ordered set of the active mt rules, read lock-free.
type Snapshot struct {
	byConnector, byAccount, byCustomer map[uuid.UUID][]rule
	platform                           []rule
}

// Build compiles the active mt rules into a Snapshot, each scope's rules in priority-then-id order. A
// rule whose stored pattern does not compile — the Admin API refuses one, a hand-written SQL row may
// not — is skipped and named in the log.
func Build(rules []cp.SenderRewriteRule, logger *slog.Logger) *Snapshot {
	rules = slices.Clone(rules)
	slices.SortStableFunc(rules, func(a, b cp.SenderRewriteRule) int {
		return cmp.Or(cmp.Compare(a.Priority, b.Priority), bytes.Compare(a.ID[:], b.ID[:]))
	})
	s := &Snapshot{
		byConnector: map[uuid.UUID][]rule{},
		byAccount:   map[uuid.UUID][]rule{},
		byCustomer:  map[uuid.UUID][]rule{},
	}
	for _, r := range rules {
		if r.Status != "active" || r.Direction != "mt" {
			continue
		}
		c, err := compile(r)
		if err == nil && r.Scope != cp.RewriteScopePlatform && r.ScopeID == nil {
			err = fmt.Errorf("a %s rule without its scope_id", r.Scope)
		}
		if err != nil {
			if logger != nil {
				logger.Warn("sender rewrite rule skipped", "rule_id", r.ID, "err", err)
			}
			continue
		}
		switch r.Scope {
		case cp.RewriteScopePlatform:
			s.platform = append(s.platform, c)
		case cp.RewriteScopeConnector:
			s.byConnector[*r.ScopeID] = append(s.byConnector[*r.ScopeID], c)
		case cp.RewriteScopeAccount:
			s.byAccount[*r.ScopeID] = append(s.byAccount[*r.ScopeID], c)
		case cp.RewriteScopeCustomer:
			s.byCustomer[*r.ScopeID] = append(s.byCustomer[*r.ScopeID], c)
		}
	}
	return s
}

// Rewrite returns the source address to put on the wire: the output of the first matching rule, scope
// first (connector, account, customer, platform), priority second. A nil Snapshot rewrites nothing.
func (s *Snapshot) Rewrite(connectorID, accountID, customerID uuid.UUID, from, to string, messageID uuid.UUID) string {
	if s == nil {
		return from
	}
	for _, rules := range [][]rule{s.byConnector[connectorID], s.byAccount[accountID], s.byCustomer[customerID], s.platform} {
		for _, r := range rules {
			if r.matches(from, to) {
				return r.apply(from, messageID)
			}
		}
	}
	return from
}

// EvalRule runs one rule, whatever its status, against a sample: the Admin API's test operation. err
// reports a pattern that does not compile.
func EvalRule(r cp.SenderRewriteRule, from, to string, messageID uuid.UUID) (string, bool, error) {
	c, err := compile(r)
	if err != nil {
		return "", false, err
	}
	if !c.matches(from, to) {
		return from, false, nil
	}
	return c.apply(from, messageID), true, nil
}

// MaxSenderOctets is the longest source_addr SMPP carries: a 21-octet C-Octet String, NUL included.
const MaxSenderOctets = 20

// compile anchors each pattern — a rule matches the WHOLE address, never a substring of it — and refuses
// a sender the wire cannot carry: an over-long source_addr is a PDU the SMSC may answer by dropping the
// bind, which would open the breaker for all of the connector's traffic.
func compile(r cp.SenderRewriteRule) (rule, error) {
	c := rule{SenderRewriteRule: r}
	if r.RewriteType == cp.RewriteStatic && r.RewriteTo != nil && len(*r.RewriteTo) > MaxSenderOctets {
		return rule{}, fmt.Errorf("rewrite_to is %d octets, over the %d source_addr carries", len(*r.RewriteTo), MaxSenderOctets)
	}
	if r.RewriteType == cp.RewriteFallbackPool {
		for _, s := range r.FallbackPool {
			if len(s) > MaxSenderOctets {
				return rule{}, fmt.Errorf("fallback pool entry %q is over the %d octets source_addr carries", s, MaxSenderOctets)
			}
		}
	}
	var set struct{ Allowed string }
	if len(r.SanitizeCharset) > 0 && json.Unmarshal(r.SanitizeCharset, &set) == nil {
		c.allowed = set.Allowed
	}
	var err error
	if r.MatchSenderPattern != nil {
		if c.sender, err = regexp.Compile(`^(?:` + *r.MatchSenderPattern + `)$`); err != nil {
			return rule{}, err
		}
	}
	if r.MatchDestPattern != nil {
		if c.dest, err = regexp.Compile(`^(?:` + *r.MatchDestPattern + `)$`); err != nil {
			return rule{}, err
		}
	}
	return c, nil
}

func (r rule) matches(from, to string) bool {
	return (r.sender == nil || r.sender.MatchString(from)) && (r.dest == nil || r.dest.MatchString(to))
}

// apply produces the rewritten address. A rewrite that would leave nothing to send keeps the original.
func (r rule) apply(from string, messageID uuid.UUID) string {
	var out string
	switch r.RewriteType {
	case cp.RewriteStatic:
		if r.RewriteTo != nil {
			out = *r.RewriteTo
		}
	case cp.RewriteFallbackPool:
		// Hashing the message id rather than turning a shared counter: every segment of a message gets
		// the same sender, a redelivery or a reroute gets it again, and no pod needs another's state.
		if len(r.FallbackPool) > 0 {
			h := fnv.New32a()
			_, _ = h.Write(messageID[:])
			out = r.FallbackPool[int(h.Sum32())%len(r.FallbackPool)]
		}
	case cp.RewriteTruncate:
		out = from
		if r.MaxLength != nil && utf8.RuneCountInString(from) > int(*r.MaxLength) {
			out = string([]rune(from)[:*r.MaxLength])
		}
	case cp.RewriteSanitize:
		out = sanitize(from, r.allowed)
	}
	if out == "" {
		return from
	}
	return out
}

// sanitize keeps the characters allowed lists one by one — ASCII letters and digits when it is empty.
func sanitize(from, allowed string) string {
	keep := func(c rune) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' }
	if allowed != "" {
		keep = func(c rune) bool { return strings.ContainsRune(allowed, c) }
	}
	return strings.Map(func(c rune) rune {
		if keep(c) {
			return c
		}
		return -1
	}, from)
}

// Holder keeps the current Snapshot behind an atomic pointer: a config-sync invalidation swaps a new one
// in whole, the send path reads it lock-free. Before the first Store it rewrites nothing.
type Holder struct {
	snap atomic.Pointer[Snapshot]
}

// Store swaps in a freshly built Snapshot.
func (h *Holder) Store(s *Snapshot) { h.snap.Store(s) }

// Rewrite rewrites through the current Snapshot.
func (h *Holder) Rewrite(connectorID, accountID, customerID uuid.UUID, from, to string, messageID uuid.UUID) string {
	return h.snap.Load().Rewrite(connectorID, accountID, customerID, from, to, messageID)
}
