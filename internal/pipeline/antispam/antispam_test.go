package antispam_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
)

type fakeRuleLister struct{ rules []cp.AntispamRule }

func (f fakeRuleLister) ListActive(context.Context) ([]cp.AntispamRule, error) { return f.rules, nil }

// fakeChecker records seen fingerprints in memory, mimicking Redis SET NX EX: the first sighting is
// new, every later one is a duplicate. A preset err drives the transient-fault path.
type fakeChecker struct {
	seen map[string]bool
	err  error
	last string
}

func (c *fakeChecker) Seen(_ context.Context, fingerprint string, _ time.Duration) (bool, error) {
	if c.err != nil {
		return false, c.err
	}
	c.last = fingerprint
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	if c.seen[fingerprint] {
		return true, nil
	}
	c.seen[fingerprint] = true
	return false, nil
}

func contentRule(scope cp.AntispamScope, scopeID *uuid.UUID, action cp.AntispamAction, patterns ...string) cp.AntispamRule {
	cfg, _ := json.Marshal(map[string]any{"patterns": patterns})
	return cp.AntispamRule{ID: uuid.New(), RuleType: cp.AntispamContentBlacklist, Scope: scope, ScopeID: scopeID, ConfigJSON: cfg, Action: action, Status: cp.AntispamRuleActive}
}

func dupRule(scope cp.AntispamScope, scopeID *uuid.UUID, action cp.AntispamAction, windowSec int) cp.AntispamRule {
	cfg, _ := json.Marshal(map[string]any{"window_seconds": windowSec})
	return cp.AntispamRule{ID: uuid.New(), RuleType: cp.AntispamDuplicate, Scope: scope, ScopeID: scopeID, ConfigJSON: cfg, Action: action, Status: cp.AntispamRuleActive}
}

func engineWith(t *testing.T, checker antispam.DuplicateChecker, rules ...cp.AntispamRule) *antispam.Engine {
	t.Helper()
	e, err := antispam.New(context.Background(), fakeRuleLister{rules}, checker, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

func TestContentBlacklistBlocks(t *testing.T) {
	e := engineWith(t, nil, contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)\bviagra\b`, `(?i)loan`))

	action, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), "2250700000001", []byte("cheap VIAGRA now"))
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if action != cp.AntispamActionBlock {
		t.Errorf("action = %q, want block", action)
	}

	// A clean message matches nothing.
	action, _ = e.Evaluate(context.Background(), uuid.New(), uuid.New(), "2250700000001", []byte("your appointment is confirmed"))
	if action != "" {
		t.Errorf("clean message action = %q, want none", action)
	}
}

func TestContentFlagDoesNotBlock(t *testing.T) {
	e := engineWith(t, nil, contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag, `(?i)promo`))
	action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), "2250700000001", []byte("PROMO code inside"))
	if action != cp.AntispamActionFlag {
		t.Errorf("action = %q, want flag (non-blocking)", action)
	}
}

func TestDuplicateWithinWindow(t *testing.T) {
	checker := &fakeChecker{}
	e := engineWith(t, checker, dupRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, 60))

	acct, cust := uuid.New(), uuid.New()
	body := []byte("identical body")
	// First sighting: not a duplicate.
	if action, _ := e.Evaluate(context.Background(), acct, cust, "2250700000001", body); action != "" {
		t.Errorf("first sighting action = %q, want none", action)
	}
	// Second identical (dest+body) sighting within the window: the configured action.
	if action, _ := e.Evaluate(context.Background(), acct, cust, "2250700000001", body); action != cp.AntispamActionBlock {
		t.Errorf("duplicate action = %q, want block", action)
	}
	// A different destination is a different fingerprint, not a duplicate.
	if action, _ := e.Evaluate(context.Background(), acct, cust, "2250700000002", body); action != "" {
		t.Errorf("different-dest action = %q, want none", action)
	}
}

// TestDuplicateIsolatedPerAccountScope is the tenant-isolation guard: two accounts each with an
// account-scoped duplicate rule must not deduplicate against each other. Account Y's first send of a
// body already sent by account X (same dest) is NOT a duplicate — the fingerprint is namespaced by
// scope.
func TestDuplicateIsolatedPerAccountScope(t *testing.T) {
	acctX, acctY := uuid.New(), uuid.New()
	e := engineWith(t, &fakeChecker{},
		dupRule(cp.AntispamScopeAccount, &acctX, cp.AntispamActionBlock, 60),
		dupRule(cp.AntispamScopeAccount, &acctY, cp.AntispamActionBlock, 60),
	)
	body := []byte("Merci de votre visite")

	// X sends first: not a duplicate (posts X's key).
	if action, _ := e.Evaluate(context.Background(), acctX, uuid.New(), "2250700000001", body); action != "" {
		t.Fatalf("X first send action = %q, want none", action)
	}
	// Y sends the same body to the same dest: must NOT be a duplicate — it's Y's first send.
	if action, _ := e.Evaluate(context.Background(), acctY, uuid.New(), "2250700000001", body); action != "" {
		t.Errorf("Y first send action = %q, want none (cross-tenant isolation)", action)
	}
	// Y sending it AGAIN is a duplicate within Y's own scope.
	if action, _ := e.Evaluate(context.Background(), acctY, uuid.New(), "2250700000001", body); action != cp.AntispamActionBlock {
		t.Errorf("Y repeat action = %q, want block", action)
	}
}

// TestScopePrecedence: the most-specific matching content rule wins. An account flag rule outranks a
// global block rule for that account's messages.
func TestScopePrecedence(t *testing.T) {
	acct := uuid.New()
	e := engineWith(t, nil,
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)deal`),
		contentRule(cp.AntispamScopeAccount, &acct, cp.AntispamActionFlag, `(?i)deal`),
	)
	action, _ := e.Evaluate(context.Background(), acct, uuid.New(), "2250700000001", []byte("great DEAL"))
	if action != cp.AntispamActionFlag {
		t.Errorf("action = %q, want flag (account rule outranks global)", action)
	}
	// A different account falls through to the global block rule.
	action, _ = e.Evaluate(context.Background(), uuid.New(), uuid.New(), "2250700000001", []byte("great DEAL"))
	if action != cp.AntispamActionBlock {
		t.Errorf("action = %q, want block (global applies)", action)
	}
}

func TestDuplicateRedisFaultIsTransient(t *testing.T) {
	checker := &fakeChecker{err: context.DeadlineExceeded}
	e := engineWith(t, checker, dupRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, 60))
	if _, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), "2250700000001", []byte("x")); err == nil {
		t.Fatal("a Redis fault must surface as an error (transient), not a silent pass")
	}
}

// TestFingerprintIsNotTheBody reinforces invariant (a): the duplicate key handed to Redis is a hash,
// never the plaintext body.
func TestFingerprintIsNotTheBody(t *testing.T) {
	checker := &fakeChecker{}
	e := engineWith(t, checker, dupRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag, 60))
	const secret = "topsecretbody"
	if _, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), "2250700000001", []byte(secret)); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if checker.last == "" {
		t.Fatal("expected a fingerprint to be recorded")
	}
	if strings.Contains(checker.last, secret) {
		t.Errorf("fingerprint %q contains the plaintext body — invariant (a) violated", checker.last)
	}
}

// TestBadConfigRuleDropped: a rule with an invalid regex or config is dropped, not fatal — anti-spam
// keeps working with the remaining rules.
func TestBadConfigRuleDropped(t *testing.T) {
	e := engineWith(t, nil,
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `[unclosed`), // invalid regex
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)spam`),  // valid
	)
	if action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), "2250700000001", []byte("this is SPAM")); action != cp.AntispamActionBlock {
		t.Errorf("action = %q, want block (the valid rule still applies)", action)
	}
}
