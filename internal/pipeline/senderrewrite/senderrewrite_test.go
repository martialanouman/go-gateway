package senderrewrite_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderrewrite"
)

var (
	connectorID = uuid.New()
	accountID   = uuid.New()
	customerID  = uuid.New()
	messageID   = uuid.New()
)

func ptr[T any](v T) *T { return &v }

func static(scope cp.SenderRewriteScope, scopeID *uuid.UUID, priority int32, to string) cp.SenderRewriteRule {
	return cp.SenderRewriteRule{
		ID: uuid.New(), Scope: scope, ScopeID: scopeID, Direction: "mt", Status: "active",
		RewriteType: cp.RewriteStatic, RewriteTo: ptr(to), Priority: priority,
	}
}

func rewrite(s *senderrewrite.Snapshot, from string) string {
	return s.Rewrite(connectorID, accountID, customerID, from, "2250700000001", messageID)
}

// The priorities run against the precedence, so a sort on priority alone — or on the scope name —
// would pick another rule at each step.
func TestRewritePrecedenceIsScopeFirst(t *testing.T) {
	rules := []cp.SenderRewriteRule{
		static(cp.RewriteScopePlatform, nil, 1, "PLATFORM"),
		static(cp.RewriteScopeCustomer, &customerID, 2, "CUSTOMER"),
		static(cp.RewriteScopeAccount, &accountID, 3, "ACCOUNT"),
		static(cp.RewriteScopeConnector, &connectorID, 4, "CONNECTOR"),
	}
	for _, want := range []string{"CONNECTOR", "ACCOUNT", "CUSTOMER", "PLATFORM"} {
		if got := rewrite(senderrewrite.Build(rules, nil), "ACME"); got != want {
			t.Fatalf("with %d rules: %q, want %q", len(rules), got, want)
		}
		rules = rules[:len(rules)-1]
	}
	if got := rewrite(senderrewrite.Build(nil, nil), "ACME"); got != "ACME" {
		t.Fatalf("no rule: %q, want the original", got)
	}
}

func TestRewriteWithinAScopeLowerPriorityFirst(t *testing.T) {
	rules := []cp.SenderRewriteRule{
		static(cp.RewriteScopeConnector, &connectorID, 20, "LATE"),
		static(cp.RewriteScopeConnector, &connectorID, 10, "EARLY"),
	}
	if got := rewrite(senderrewrite.Build(rules, nil), "ACME"); got != "EARLY" {
		t.Fatalf("%q, want EARLY", got)
	}
}

func TestRewriteIgnoresRulesOfOtherScopesIDs(t *testing.T) {
	other := uuid.New()
	rules := []cp.SenderRewriteRule{
		static(cp.RewriteScopeConnector, &other, 1, "OTHER-CONNECTOR"),
		static(cp.RewriteScopeAccount, &other, 1, "OTHER-ACCOUNT"),
		static(cp.RewriteScopeCustomer, &other, 1, "OTHER-CUSTOMER"),
	}
	if got := rewrite(senderrewrite.Build(rules, nil), "ACME"); got != "ACME" {
		t.Fatalf("%q, want the original", got)
	}
}

// The first matching rule wins even when it leaves the address unchanged: a lower rule never runs.
func TestRewriteFirstMatchStopsEvenWhenUnchanged(t *testing.T) {
	keep := static(cp.RewriteScopeConnector, &connectorID, 1, "ACME")
	rules := []cp.SenderRewriteRule{keep, static(cp.RewriteScopePlatform, nil, 1, "PLATFORM")}
	if got := rewrite(senderrewrite.Build(rules, nil), "ACME"); got != "ACME" {
		t.Fatalf("%q, want ACME", got)
	}
}

func TestRewriteSkipsDisabledAndMORules(t *testing.T) {
	disabled := static(cp.RewriteScopePlatform, nil, 1, "DISABLED")
	disabled.Status = "disabled"
	mo := static(cp.RewriteScopePlatform, nil, 1, "MO")
	mo.Direction = "mo"
	if got := rewrite(senderrewrite.Build([]cp.SenderRewriteRule{disabled, mo}, nil), "ACME"); got != "ACME" {
		t.Fatalf("%q, want the original", got)
	}
}

func TestRewriteSkipsAnUncompilableRuleAndSaysSo(t *testing.T) {
	bad := static(cp.RewriteScopeConnector, &connectorID, 1, "BAD")
	bad.MatchSenderPattern = ptr("[A-Z")
	var logs bytes.Buffer
	s := senderrewrite.Build([]cp.SenderRewriteRule{bad, static(cp.RewriteScopePlatform, nil, 1, "GOOD")},
		slog.New(slog.NewTextHandler(&logs, nil)))
	if got := rewrite(s, "ACME"); got != "GOOD" {
		t.Fatalf("%q, want GOOD", got)
	}
	if !strings.Contains(logs.String(), bad.ID.String()) {
		t.Fatalf("the skipped rule is not named in the log: %s", logs.String())
	}
}

func TestEvalRuleMatchesTheWholeAddress(t *testing.T) {
	r := static(cp.RewriteScopePlatform, nil, 1, "INFO")
	for _, tc := range []struct {
		sender, dest *string
		from, to     string
		matched      bool
	}{
		{nil, nil, "ACME", "2250700000001", true},
		{ptr("[0-9]+"), nil, "22507", "2250700000001", true},
		{ptr("[0-9]+"), nil, "+22507", "2250700000001", false},
		{nil, ptr("225"), "ACME", "2250700000001", false},
		{nil, ptr("225.*"), "ACME", "2250700000001", true},
		{nil, ptr("225.*|33.*"), "ACME", "3312345", true},
		{ptr("ACME"), ptr("33.*"), "ACME", "2250700000001", false},
	} {
		r.MatchSenderPattern, r.MatchDestPattern = tc.sender, tc.dest
		_, matched, err := senderrewrite.EvalRule(r, tc.from, tc.to, messageID)
		if err != nil || matched != tc.matched {
			t.Errorf("sender %v dest %v on %s→%s: matched=%v err=%v, want %v",
				deref(tc.sender), deref(tc.dest), tc.from, tc.to, matched, err, tc.matched)
		}
	}
}

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func TestEvalRuleTypes(t *testing.T) {
	base := cp.SenderRewriteRule{Direction: "mt", Status: "active"}
	for name, tc := range map[string]struct {
		rule cp.SenderRewriteRule
		from string
		want string
	}{
		"static":           {withType(base, func(r *cp.SenderRewriteRule) { r.RewriteType, r.RewriteTo = cp.RewriteStatic, ptr("INFO") }), "ACME", "INFO"},
		"truncate runes":   {withType(base, func(r *cp.SenderRewriteRule) { r.RewriteType, r.MaxLength = cp.RewriteTruncate, ptr[int32](4) }), "ÉCOLEPRO", "ÉCOL"},
		"truncate short":   {withType(base, func(r *cp.SenderRewriteRule) { r.RewriteType, r.MaxLength = cp.RewriteTruncate, ptr[int32](11) }), "ACME", "ACME"},
		"sanitize default": {withType(base, func(r *cp.SenderRewriteRule) { r.RewriteType = cp.RewriteSanitize }), "Ac-mé 9!", "Acm9"},
		"sanitize allowed": {withType(base, func(r *cp.SenderRewriteRule) {
			r.RewriteType, r.SanitizeCharset = cp.RewriteSanitize, []byte(`{"allowed": "A-"}`)
		}), "Ac-mé A", "A-A"},
		"sanitize to empty": {withType(base, func(r *cp.SenderRewriteRule) { r.RewriteType = cp.RewriteSanitize }), "!!!", "!!!"},
	} {
		out, matched, err := senderrewrite.EvalRule(tc.rule, tc.from, "2250700000001", messageID)
		if err != nil || !matched || out != tc.want {
			t.Errorf("%s: %q matched=%v err=%v, want %q", name, out, matched, err, tc.want)
		}
	}
}

func withType(r cp.SenderRewriteRule, f func(*cp.SenderRewriteRule)) cp.SenderRewriteRule {
	f(&r)
	return r
}

// Every segment of a message carries the same message_id, so they all get the same sender; different
// messages spread over the pool.
func TestEvalRuleFallbackPoolIsDeterministicPerMessage(t *testing.T) {
	r := cp.SenderRewriteRule{
		Direction: "mt", Status: "active", RewriteType: cp.RewriteFallbackPool, FallbackPool: []string{"A", "B", "C"},
	}
	first, _, _ := senderrewrite.EvalRule(r, "ACME", "225", messageID)
	for range 5 {
		if again, _, _ := senderrewrite.EvalRule(r, "ACME", "225", messageID); again != first {
			t.Fatalf("same message_id gave %q then %q", first, again)
		}
	}
	seen := map[string]bool{}
	for range 64 {
		out, _, _ := senderrewrite.EvalRule(r, "ACME", "225", uuid.New())
		seen[out] = true
	}
	if len(seen) != 3 {
		t.Fatalf("64 messages used %v, want all three senders", seen)
	}
}

func TestEvalRuleReportsAnUncompilablePattern(t *testing.T) {
	r := static(cp.RewriteScopePlatform, nil, 1, "INFO")
	r.MatchDestPattern = ptr("(225")
	if _, _, err := senderrewrite.EvalRule(r, "ACME", "225", messageID); err == nil {
		t.Fatal("no error for an uncompilable pattern")
	}
}

func TestHolderBeforeTheFirstStoreKeepsTheOriginal(t *testing.T) {
	var h senderrewrite.Holder
	if got := h.Rewrite(connectorID, accountID, customerID, "ACME", "225", messageID); got != "ACME" {
		t.Fatalf("%q, want ACME", got)
	}
	h.Store(senderrewrite.Build([]cp.SenderRewriteRule{static(cp.RewriteScopePlatform, nil, 1, "INFO")}, nil))
	if got := h.Rewrite(connectorID, accountID, customerID, "ACME", "225", messageID); got != "INFO" {
		t.Fatalf("%q, want INFO", got)
	}
}
