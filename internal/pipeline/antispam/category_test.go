package antispam_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
)

func mismatchRule(scope cp.AntispamScope, scopeID *uuid.UUID, action cp.AntispamAction, cfg map[string]any) cp.AntispamRule {
	raw, _ := json.Marshal(cfg)
	return cp.AntispamRule{ID: uuid.New(), RuleType: cp.AntispamCategoryMismatch, Scope: scope, ScopeID: scopeID, ConfigJSON: raw, Action: action, Status: cp.AntispamRuleActive}
}

// TestCategoryMismatchChecksTrafficAgainstItsDeclaration: an OTP sender must carry a 4-8 digit code, no URL
// and a short body; a transactional one no promotional marker; a marketing one is never checked
// (ADR-0020 §5). The same body is judged by the category it is declared under.
func TestCategoryMismatchChecksTrafficAgainstItsDeclaration(t *testing.T) {
	state, metric := newFakeState(), &fakeMetric{}
	e := engineWith(t, state, metric, mismatchRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag,
		map[string]any{"promo_markers": []string{"Promo"}}))

	cases := []struct {
		name     string
		category cp.TrafficCategory
		body     string
		want     cp.AntispamAction
	}{
		{"otp with a code", cp.TrafficOTP, "Your code is 482913", ""},
		{"otp without a code", cp.TrafficOTP, "Welcome to the bank", cp.AntispamActionFlag},
		{"otp with a three-digit number only", cp.TrafficOTP, "Code 123", cp.AntispamActionFlag},
		{"otp with a nine-digit number only", cp.TrafficOTP, "Code 123456789", cp.AntispamActionFlag},
		{"otp with a nine-digit number first", cp.TrafficOTP, "123456789 is your code", cp.AntispamActionFlag},
		{"otp with four digits", cp.TrafficOTP, "Code:1234.", ""},
		{"otp with eight digits", cp.TrafficOTP, "Code 12345678", ""},
		{"otp grouped by a space", cp.TrafficOTP, "Your code: 123 456", ""},
		{"otp grouped by dashes", cp.TrafficOTP, "G-123-456 is your code", ""},
		{"otp with a url", cp.TrafficOTP, "Code 482913 at https://x.example", cp.AntispamActionFlag},
		{"otp with a plain http url", cp.TrafficOTP, "Code 482913 at HTTP://x.example", cp.AntispamActionFlag},
		{"otp with a www link", cp.TrafficOTP, "Code 482913 at www.x.example", cp.AntispamActionFlag},
		{"otp with a link without scheme", cp.TrafficOTP, "Code 482913, confirm on bit.ly/x9", cp.AntispamActionFlag},
		{"otp at the length limit", cp.TrafficOTP, "Code 482913 " + strings.Repeat("a", 148), ""},
		{"otp one past the length limit", cp.TrafficOTP, "Code 482913 " + strings.Repeat("a", 149), cp.AntispamActionFlag},
		{"otp long in bytes, short in characters", cp.TrafficOTP, "Code 482913 " + strings.Repeat("é", 100), ""},
		{"otp from a sender with no category", "", "Welcome", ""},
		{"transactional without marker", cp.TrafficTransactional, "Your parcel ships today", ""},
		{"transactional with a marker, any case", cp.TrafficTransactional, "PROMO: your parcel ships", cp.AntispamActionFlag},
		{"marketing is never checked", cp.TrafficMarketing, "PROMO at https://x.example", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e.Evaluate(context.Background(), uuid.New(), uuid.New(), uuid.New(), "BANK", tc.category, "2250700000001", []byte(tc.body))
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Evaluate(%q as %s) = %q, want %q", tc.body, tc.category, got, tc.want)
			}
		})
	}
}

// TestCategoryMismatchCountsEachMatchOnce: block is per rule, every match — flag or block — reaches the
// per-sender counter and the metric once, a redelivered message is not counted again, and a match of
// another rule reaches neither.
func TestCategoryMismatchCountsEachMatchOnce(t *testing.T) {
	customer, other := uuid.New(), uuid.New()
	state, metric := newFakeState(), &fakeMetric{}
	e := engineWith(t, state, metric,
		mismatchRule(cp.AntispamScopeCustomer, &customer, cp.AntispamActionBlock, map[string]any{}),
		mismatchRule(cp.AntispamScopeCustomer, &other, cp.AntispamActionFlag, map[string]any{}),
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag, `(?i)lottery`),
	)
	ctx := context.Background()

	replayed := uuid.New()
	for range 2 {
		if got, _ := e.Evaluate(ctx, replayed, uuid.New(), customer, "BANK", cp.TrafficOTP, "2250700000001", []byte("no code here")); got != cp.AntispamActionBlock {
			t.Fatalf("a block-configured mismatch = %q, want block", got)
		}
	}
	if got, _ := e.Evaluate(ctx, uuid.New(), uuid.New(), other, "SHOP", cp.TrafficOTP, "2250700000001", []byte("no code here")); got != cp.AntispamActionFlag {
		t.Fatalf("a flag-configured mismatch = %q, want flag", got)
	}
	_, _ = e.Evaluate(ctx, uuid.New(), uuid.New(), customer, "PROMO", cp.TrafficMarketing, "2250700000001", []byte("lottery"))

	want := []cp.SenderAddress{{CustomerID: customer, Address: "BANK"}, {CustomerID: other, Address: "SHOP"}}
	if len(state.mismatches) != 2 || state.mismatches[0] != want[0] || state.mismatches[1] != want[1] {
		t.Fatalf("counted mismatches = %+v, want once each %+v", state.mismatches, want)
	}
	if len(metric.mismatches) != 2 || metric.mismatches[0] != cp.AntispamActionBlock || metric.mismatches[1] != cp.AntispamActionFlag {
		t.Fatalf("metric = %v, want one block then one flag", metric.mismatches)
	}
}

// TestCategoryMismatchUsesTheConfiguredLimits: the rule's own bounds replace the ADR defaults.
func TestCategoryMismatchUsesTheConfiguredLimits(t *testing.T) {
	e := engineWith(t, newFakeState(), &fakeMetric{}, mismatchRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag,
		map[string]any{"otp_code_min_digits": 6, "otp_code_max_digits": 6, "otp_max_length": 20}))
	for body, want := range map[string]cp.AntispamAction{
		"Code 123456":              "",
		"Code 12345":               cp.AntispamActionFlag,
		"Code 1234567":             cp.AntispamActionFlag,
		"Code 123456 is yours now": cp.AntispamActionFlag,
	} {
		if got, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), uuid.New(), "BANK", cp.TrafficOTP, "2250700000001", []byte(body)); got != want {
			t.Errorf("Evaluate(%q) = %q, want %q", body, got, want)
		}
	}
}

// TestCategoryMismatchResolvesTheMostSpecificScope: an account rule wins over its customer's and the
// global one, without falling back to them when it finds nothing.
func TestCategoryMismatchResolvesTheMostSpecificScope(t *testing.T) {
	account, customer := uuid.New(), uuid.New()
	e := engineWith(t, newFakeState(), &fakeMetric{},
		mismatchRule(cp.AntispamScopeAccount, &account, cp.AntispamActionFlag, map[string]any{}),
		mismatchRule(cp.AntispamScopeCustomer, &customer, cp.AntispamActionBlock, map[string]any{"promo_markers": []string{"sale"}}),
		mismatchRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, map[string]any{"promo_markers": []string{"sale"}}),
	)
	evaluate := func(accountID, customerID uuid.UUID) cp.AntispamAction {
		got, _ := e.Evaluate(context.Background(), uuid.New(), accountID, customerID, "SHOP", cp.TrafficTransactional, "2250700000001", []byte("big sale"))
		return got
	}
	if got := evaluate(account, customer); got != "" {
		t.Errorf("the account rule has no marker = %q, want nothing — no fallback to the customer's", got)
	}
	if got := evaluate(uuid.New(), customer); got != cp.AntispamActionBlock {
		t.Errorf("another account of the customer = %q, want the customer's block", got)
	}
}

// TestCategoryMismatchLeaksNothingOfTheBody: invariant (a) — the engine's logs carry neither the body nor
// the marker it matched, including when the counter store fails.
func TestCategoryMismatchLeaksNothingOfTheBody(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	state, metric := newFakeState(), &fakeMetric{}
	state.err = errFake
	e, err := antispam.New(context.Background(), fakeRuleLister{[]cp.AntispamRule{
		mismatchRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag, map[string]any{"promo_markers": []string{"jackpot"}}),
	}}, state, metric, logger)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), uuid.New(), "BANK", cp.TrafficTransactional, "2250700000001", []byte("topsecretbody jackpot"))
	if got != cp.AntispamActionFlag {
		t.Fatalf("Evaluate = %q, want flag — the leak check would be vacuous", got)
	}
	if logs.Len() == 0 {
		t.Fatal("the failed counter write logged nothing — the leak check would be vacuous")
	}
	for _, secret := range []string{"topsecretbody", "jackpot"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("the engine logged %q: %s", secret, logs.String())
		}
	}
	for _, label := range metric.mismatches {
		if label != cp.AntispamActionFlag && label != cp.AntispamActionBlock {
			t.Fatalf("metric label %q is not an action", label)
		}
	}
}

// TestCategoryMismatchConfigIsValidated: the Admin API rejects at write time what the engine would drop.
func TestCategoryMismatchConfigIsValidated(t *testing.T) {
	for _, bad := range []string{
		`{"otp_code_min_digits":0}`, `{"otp_code_min_digits":9,"otp_code_max_digits":8}`, `{"otp_code_max_digits":33}`,
		`{"otp_max_length":0}`, `{"promo_markers":[""]}`, `{"otp_min_digits":6}`, `[]`,
	} {
		if err := antispam.ValidateRuleConfig(cp.AntispamCategoryMismatch, json.RawMessage(bad)); err == nil {
			t.Errorf("config %s accepted, want rejected", bad)
		}
	}
	if err := antispam.ValidateRuleConfig(cp.AntispamCategoryMismatch, json.RawMessage(`{}`)); err != nil {
		t.Errorf("empty config = %v, want accepted (every field has a default)", err)
	}
}

var errFake = errors.New("redis down")

// TestCategoryMismatchRuleWithAnUnbuildableCodeIsDropped: a stored rule the write path would refuse must
// not stop the router — the engine drops it and keeps the others.
func TestCategoryMismatchRuleWithAnUnbuildableCodeIsDropped(t *testing.T) {
	e := engineWith(t, newFakeState(), &fakeMetric{},
		mismatchRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionFlag, map[string]any{"otp_code_max_digits": 2000}),
		contentRule(cp.AntispamScopeGlobal, nil, cp.AntispamActionBlock, `(?i)lottery`),
	)
	if got, _ := e.Evaluate(context.Background(), uuid.New(), uuid.New(), uuid.New(), "BANK", cp.TrafficOTP, "2250700000001", []byte("lottery")); got != cp.AntispamActionBlock {
		t.Fatalf("Evaluate = %q, want the content rule's block — the engine must survive the bad row", got)
	}
}
