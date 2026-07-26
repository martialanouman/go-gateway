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

const src = "22507000009" // a placeholder source for tests that do not exercise per-source rules

type fakeRuleLister struct{ rules []cp.AntispamRule }

func (f fakeRuleLister) ListActive(context.Context) ([]cp.AntispamRule, error) { return f.rules, nil }

// fakeState is an in-memory StateStore mimicking Redis: duplicate SET NX, a sliding-window hit
// counter, and a reputation map. A preset err drives the fail-open path on every operation.
type fakeState struct {
	seen     map[string]bool
	counts   map[string]int
	scores   map[string]int
	err      error
	lastDup  string
	lastHit  string
	hitCalls int
}

func newFakeState() *fakeState {
	return &fakeState{seen: map[string]bool{}, counts: map[string]int{}, scores: map[string]int{}}
}

func (s *fakeState) Seen(_ context.Context, fingerprint string, _ time.Duration) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.lastDup = fingerprint
	if s.seen[fingerprint] {
		return true, nil
	}
	s.seen[fingerprint] = true
	return false, nil
}

func (s *fakeState) Hit(_ context.Context, key string, _ time.Duration) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.lastHit = key
	s.hitCalls++
	s.counts[key]++
	return s.counts[key], nil
}

func (s *fakeState) Reputation(_ context.Context, source string) (int, bool, error) {
	if s.err != nil {
		return 0, false, s.err
	}
	score, ok := s.scores[source]
	return score, ok, nil
}

type fakeMetric struct{ failOpens int }

func (m *fakeMetric) FailOpen() { m.failOpens++ }

func contentRule(scope cp.AntispamScope, scopeID *uuid.UUID, action cp.AntispamAction, patterns ...string) cp.AntispamRule {
	cfg, _ := json.Marshal(map[string]any{"patterns": patterns})
	return cp.AntispamRule{ID: uuid.New(), RuleType: cp.AntispamContentBlacklist, Scope: scope, ScopeID: scopeID, ConfigJSON: cfg, Action: action, Status: cp.AntispamRuleActive}
}

func dupRule(scope cp.AntispamScope, scopeID *uuid.UUID, action cp.AntispamAction, windowSec int) cp.AntispamRule {
	cfg, _ := json.Marshal(map[string]any{"window_seconds": windowSec})
	return cp.AntispamRule{ID: uuid.New(), RuleType: cp.AntispamDuplicate, Scope: scope, ScopeID: scopeID, ConfigJSON: cfg, Action: action, Status: cp.AntispamRuleActive}
}

func velRule(scope cp.AntispamScope, scopeID *uuid.UUID, action cp.AntispamAction, max, windowSec int, by string) cp.AntispamRule {
	cfg, _ := json.Marshal(map[string]any{"max": max, "window_seconds": windowSec, "by": by})
	return cp.AntispamRule{ID: uuid.New(), RuleType: cp.AntispamVelocity, Scope: scope, ScopeID: scopeID, ConfigJSON: cfg, Action: action, Status: cp.AntispamRuleActive}
}

func repRule(scope cp.AntispamScope, scopeID *uuid.UUID, action cp.AntispamAction, minScore int) cp.AntispamRule {
	cfg, _ := json.Marshal(map[string]any{"min_score": minScore})
	return cp.AntispamRule{ID: uuid.New(), RuleType: cp.AntispamReputation, Scope: scope, ScopeID: scopeID, ConfigJSON: cfg, Action: action, Status: cp.AntispamRuleActive}
}

func engineWith(t *testing.T, state antispam.StateStore, metric antispam.Metric, rules ...cp.AntispamRule) *antispam.Engine {
	t.Helper()
	e, err := antispam.New(context.Background(), fakeRuleLister{rules}, state, metric, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

func TestContentBlacklistBlocks(t *testing.T) {
	e := engineWith(t, nil, nil, contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)\bviagra\b`, `(?i)loan`))

	action, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte("cheap VIAGRA now"))
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if action != cp.AntispamActionBlock {
		t.Errorf("action = %q, want block", action)
	}
	if action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte("your appointment is confirmed")); action != "" {
		t.Errorf("clean message action = %q, want none", action)
	}
}

func TestContentFlagDoesNotBlock(t *testing.T) {
	e := engineWith(t, nil, nil, contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag, `(?i)promo`))
	action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte("PROMO code inside"))
	if action != cp.AntispamActionFlag {
		t.Errorf("action = %q, want flag (non-blocking)", action)
	}
}

func TestDuplicateWithinWindow(t *testing.T) {
	e := engineWith(t, newFakeState(), nil, dupRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, 60))
	acct, cust := uuid.New(), uuid.New()
	body := []byte("identical body")
	if action, _ := e.Evaluate(context.Background(), acct, cust, src, "2250700000001", body); action != "" {
		t.Errorf("first sighting action = %q, want none", action)
	}
	if action, _ := e.Evaluate(context.Background(), acct, cust, src, "2250700000001", body); action != cp.AntispamActionBlock {
		t.Errorf("duplicate action = %q, want block", action)
	}
	if action, _ := e.Evaluate(context.Background(), acct, cust, src, "2250700000002", body); action != "" {
		t.Errorf("different-dest action = %q, want none", action)
	}
}

func TestDuplicateIsolatedPerAccountScope(t *testing.T) {
	acctX, acctY := uuid.New(), uuid.New()
	e := engineWith(t, newFakeState(), nil,
		dupRule(cp.AntispamScopeAccount, &acctX, cp.AntispamActionBlock, 60),
		dupRule(cp.AntispamScopeAccount, &acctY, cp.AntispamActionBlock, 60),
	)
	body := []byte("Merci de votre visite")
	if action, _ := e.Evaluate(context.Background(), acctX, uuid.New(), src, "2250700000001", body); action != "" {
		t.Fatalf("X first send action = %q, want none", action)
	}
	if action, _ := e.Evaluate(context.Background(), acctY, uuid.New(), src, "2250700000001", body); action != "" {
		t.Errorf("Y first send action = %q, want none (cross-tenant isolation)", action)
	}
	if action, _ := e.Evaluate(context.Background(), acctY, uuid.New(), src, "2250700000001", body); action != cp.AntispamActionBlock {
		t.Errorf("Y repeat action = %q, want block", action)
	}
}

func TestScopePrecedence(t *testing.T) {
	acct := uuid.New()
	e := engineWith(t, nil, nil,
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)deal`),
		contentRule(cp.AntispamScopeAccount, &acct, cp.AntispamActionFlag, `(?i)deal`),
	)
	if action, _ := e.Evaluate(context.Background(), acct, uuid.New(), src, "2250700000001", []byte("great DEAL")); action != cp.AntispamActionFlag {
		t.Errorf("action = %q, want flag (account rule outranks global)", action)
	}
	if action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte("great DEAL")); action != cp.AntispamActionBlock {
		t.Errorf("action = %q, want block (global applies)", action)
	}
}

// TestVelocityOverThreshold: once the sliding-window count exceeds max, the rule's action applies;
// under the threshold the message passes.
func TestVelocityOverThreshold(t *testing.T) {
	from := "22507000001"
	e := engineWith(t, newFakeState(), nil, velRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionThrottle, 2, 60, "source"))
	acct, cust := uuid.New(), uuid.New()

	// Hits 1 and 2 are within the limit (max=2).
	for i := 1; i <= 2; i++ {
		if action, _ := e.Evaluate(context.Background(), acct, cust, from, "2250700000001", []byte("hi")); action != "" {
			t.Errorf("hit %d action = %q, want none (under limit)", i, action)
		}
	}
	// Hit 3 exceeds max → throttle.
	if action, _ := e.Evaluate(context.Background(), acct, cust, from, "2250700000001", []byte("hi")); action != cp.AntispamActionThrottle {
		t.Errorf("over-limit action = %q, want throttle", action)
	}
}

// TestReputationBelowThreshold: a source scored below the rule's minimum triggers the action; an
// unscored source is neutral.
func TestReputationBelowThreshold(t *testing.T) {
	state := newFakeState()
	badSource := "22507000002"
	state.scores[badSource] = 10
	e := engineWith(t, state, nil, repRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, 50))

	if action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), badSource, "2250700000001", []byte("hi")); action != cp.AntispamActionBlock {
		t.Errorf("low-reputation action = %q, want block", action)
	}
	// An unscored source passes.
	if action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), "22507000003", "2250700000001", []byte("hi")); action != "" {
		t.Errorf("unscored source action = %q, want none", action)
	}
}

// TestFailOpenOnRedisFault is the load-bearing §1.5 guard: a Redis fault on a velocity/duplicate/
// reputation check does NOT block or error — the message passes, flagged, and the fail-open metric is
// counted. Static content rules stay in force.
func TestFailOpenOnRedisFault(t *testing.T) {
	metric := &fakeMetric{}
	state := &fakeState{err: context.DeadlineExceeded}
	e := engineWith(t, state, metric,
		velRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, 1, 60, "source"),
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)spam`),
	)

	// A clean message under a Redis fault: fail open → flagged, not blocked; metric counted.
	action, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte("hello"))
	if err != nil {
		t.Fatalf("fail-open must not return an error: %v", err)
	}
	if action != cp.AntispamActionFlag {
		t.Errorf("action = %q, want flag (fail open)", action)
	}
	if metric.failOpens == 0 {
		t.Error("a fail-open must be counted")
	}
	// Content rules still apply even under a Redis fault: a spam body is still blocked.
	if action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte("buy SPAM")); action != cp.AntispamActionBlock {
		t.Errorf("action = %q, want block (content stays in force under fail-open)", action)
	}
}

func TestFingerprintIsNotTheBody(t *testing.T) {
	state := newFakeState()
	e := engineWith(t, state, nil, dupRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag, 60))
	const secret = "topsecretbody"
	if _, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte(secret)); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if state.lastDup == "" {
		t.Fatal("expected a fingerprint to be recorded")
	}
	if strings.Contains(state.lastDup, secret) {
		t.Errorf("fingerprint %q contains the plaintext body — invariant (a) violated", state.lastDup)
	}
}

func TestBadConfigRuleDropped(t *testing.T) {
	e := engineWith(t, nil, nil,
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `[unclosed`),
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)spam`),
	)
	if action, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), src, "2250700000001", []byte("this is SPAM")); action != cp.AntispamActionBlock {
		t.Errorf("action = %q, want block (the valid rule still applies)", action)
	}
}

// TestMOSourceVelocityKeyMatchesGlobalSourceRule: the key inbound MO records into is exactly the key a
// global "by source" velocity rule reads, so MT and MO traffic share one window.
func TestMOSourceVelocityKeyMatchesGlobalSourceRule(t *testing.T) {
	from := "22507000004"
	state := newFakeState()
	e := engineWith(t, state, nil, velRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionThrottle, 10, 60, "source"))

	// An MT evaluation hits the source's velocity key…
	if _, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), from, "2250700000001", []byte("hi")); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	// …which must equal the exported MO-recording key (namespaced with the velocity prefix at the store
	// boundary, so compare the suffix the engine and the recorder agree on).
	if want := antispam.MOSourceVelocityKey(from); state.lastHit != want {
		t.Errorf("velocity key = %q, want %q (MO and MT must share the key)", state.lastHit, want)
	}
}

// TestVelocitySourceNormalized is the MAJEUR guard: an MT sent from a non-canonical source ("+225…")
// must key on the SAME velocity counter as the MO path records under the canonical form ("225…"), so a
// sender's traffic is never split by spelling.
func TestVelocitySourceNormalized(t *testing.T) {
	state := newFakeState()
	e := engineWith(t, state, nil, velRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionThrottle, 10, 60, "source"))

	// MT from the "+"-prefixed spelling.
	if _, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), "+2250700000001", "36000", []byte("hi")); err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	// The MO path records the canonical spelling.
	if want := antispam.MOSourceVelocityKey("2250700000001"); state.lastHit != want {
		t.Errorf("MT velocity key = %q, want %q — the two spellings must share one counter", state.lastHit, want)
	}
}
