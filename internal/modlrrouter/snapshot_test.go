package modlrrouter

import (
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

func silentLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestResolveDedicated: an MO to a dedicated number routes to that number's account and its customer.
func TestResolveDedicated(t *testing.T) {
	account, customer := uuid.New(), uuid.New()
	numID := uuid.New()
	snap := compileSnapshot(silentLogger(),
		[]cp.InboundNumber{{ID: numID, Address: "36000", Status: cp.InboundNumberActive, AccountID: &account}},
		nil,
		map[uuid.UUID]uuid.UUID{account: customer},
	)

	res := snap.resolve("36000", []byte("anything"))
	if !res.routed || res.accountID != account || res.customerID != customer {
		t.Errorf("resolve dedicated = %+v, want routed to account %s / customer %s", res, account, customer)
	}
	if res.inboundNumberID == nil || *res.inboundNumberID != numID {
		t.Errorf("inbound_number_id = %v, want %s", res.inboundNumberID, numID)
	}
}

// TestResolveSharedByKeyword: a shared number routes to the first keyword that matches by priority,
// across exact/prefix/regex.
func TestResolveSharedByKeyword(t *testing.T) {
	numID := uuid.New()
	acctInfo, acctStop, acctBal := uuid.New(), uuid.New(), uuid.New()
	cust := map[uuid.UUID]uuid.UUID{acctInfo: uuid.New(), acctStop: uuid.New(), acctBal: uuid.New()}
	snap := compileSnapshot(silentLogger(),
		[]cp.InboundNumber{{ID: numID, Address: "36000", Status: cp.InboundNumberActive}}, // shared (AccountID nil)
		[]cp.InboundKeyword{
			{ID: uuid.New(), InboundNumberID: numID, Keyword: "STOP", MatchType: cp.MatchExact, AccountID: acctStop, Priority: 0},
			{ID: uuid.New(), InboundNumberID: numID, Keyword: "INFO", MatchType: cp.MatchPrefix, AccountID: acctInfo, Priority: 10},
			{ID: uuid.New(), InboundNumberID: numID, Keyword: `^BAL[0-9]+$`, MatchType: cp.MatchRegex, AccountID: acctBal, Priority: 20},
		},
		cust,
	)

	cases := []struct {
		body string
		want uuid.UUID
	}{
		{"stop", acctStop},        // exact, case-insensitive
		{"  STOP ", acctStop},     // exact, trimmed
		{"info please", acctInfo}, // prefix, case-insensitive
		{"BAL123", acctBal},       // regex
	}
	for _, c := range cases {
		res := snap.resolve("36000", []byte(c.body))
		if !res.routed || res.accountID != c.want {
			t.Errorf("resolve(%q) = %+v, want routed to %s", c.body, res, c.want)
		}
	}

	// No keyword matches -> unrouted.
	res := snap.resolve("36000", []byte("gibberish"))
	if res.routed || res.reason != cp.UnroutedNoKeywordMatch {
		t.Errorf("resolve(no match) = %+v, want unrouted no_keyword_match", res)
	}
}

// TestResolvePriorityOrder: when two keywords could match, the lower-priority (evaluated first) wins.
func TestResolvePriorityOrder(t *testing.T) {
	numID := uuid.New()
	first, second := uuid.New(), uuid.New()
	snap := compileSnapshot(silentLogger(),
		[]cp.InboundNumber{{ID: numID, Address: "36000", Status: cp.InboundNumberActive}},
		[]cp.InboundKeyword{
			// Both are prefix "A"; the priority-0 one is listed first (ListAll orders by priority).
			{ID: uuid.New(), InboundNumberID: numID, Keyword: "A", MatchType: cp.MatchPrefix, AccountID: first, Priority: 0},
			{ID: uuid.New(), InboundNumberID: numID, Keyword: "A", MatchType: cp.MatchPrefix, AccountID: second, Priority: 10},
		},
		map[uuid.UUID]uuid.UUID{first: uuid.New(), second: uuid.New()},
	)
	if res := snap.resolve("36000", []byte("ABC")); res.accountID != first {
		t.Errorf("priority order not respected: routed to %s, want the priority-0 account %s", res.accountID, first)
	}
}

// TestResolveUnroutedCases: unknown number, disabled number.
func TestResolveUnroutedCases(t *testing.T) {
	numID := uuid.New()
	snap := compileSnapshot(silentLogger(),
		[]cp.InboundNumber{{ID: numID, Address: "36000", Status: cp.InboundNumberDisabled}},
		nil, map[uuid.UUID]uuid.UUID{},
	)

	if res := snap.resolve("99999", []byte("x")); res.routed || res.reason != cp.UnroutedUnknownNumber {
		t.Errorf("unknown number = %+v, want unrouted unknown_number", res)
	}
	if res := snap.resolve("36000", []byte("x")); res.routed || res.reason != cp.UnroutedNumberDisabled {
		t.Errorf("disabled number = %+v, want unrouted number_disabled", res)
	}
}

// TestResolveNormalizesLongcode: a long code stored in E.164 matches an MO addressed without the "+".
func TestResolveNormalizesLongcode(t *testing.T) {
	account := uuid.New()
	snap := compileSnapshot(silentLogger(),
		[]cp.InboundNumber{{ID: uuid.New(), Address: "+2250700000000", Status: cp.InboundNumberActive, AccountID: &account}},
		nil, map[uuid.UUID]uuid.UUID{account: uuid.New()},
	)
	if res := snap.resolve("2250700000000", []byte("x")); !res.routed || res.accountID != account {
		t.Errorf("longcode normalization failed: %+v", res)
	}
}

// TestCompileDropsInvalidRegex: a keyword with a bad regexp is dropped, not fatal.
func TestCompileDropsInvalidRegex(t *testing.T) {
	numID := uuid.New()
	snap := compileSnapshot(silentLogger(),
		[]cp.InboundNumber{{ID: numID, Address: "36000", Status: cp.InboundNumberActive}},
		[]cp.InboundKeyword{
			{ID: uuid.New(), InboundNumberID: numID, Keyword: "([", MatchType: cp.MatchRegex, AccountID: uuid.New()},
		},
		map[uuid.UUID]uuid.UUID{},
	)
	if got := len(snap.keywords[numID]); got != 0 {
		t.Errorf("invalid regex keyword kept: %d compiled, want 0", got)
	}
}
