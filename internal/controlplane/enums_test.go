package controlplane_test

import (
	"os"
	"regexp"
	"sort"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// TestEveryEnumValueMatchesTheDDLCheckConstraint is the highest-value test in the package: it makes
// the Go enum types and the database CHECK constraints provably identical rather than hopefully so.
// It reads the migration, extracts the `CHECK (<col> IN (...))` literals for each (table, column)
// an M1 enum maps to, and asserts the Go constants are exactly that set. A value renamed on either
// side — a contract bug per convention-style-go §2.3 — fails here.
func TestEveryEnumValueMatchesTheDDLCheckConstraint(t *testing.T) {
	const migration = "../../migrations/0001_init.up.sql"
	raw, err := os.ReadFile(migration)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	blocks := tableBlocks(string(raw))

	cases := []struct {
		table  string
		column string
		got    []string
	}{
		{"customers", "status", stringify(cp.CustomerActive, cp.CustomerSuspended, cp.CustomerClosed)},
		{"customers", "billing_mode", stringify(cp.BillingPrepaid, cp.BillingPostpaid)},
		{"customers", "balance_scope", stringify(cp.BalanceScopeCustomer, cp.BalanceScopeSMPPAccount)},
		{"customers", "content_storage", stringify(cp.ContentInherit, cp.ContentOff, cp.ContentStoredPlaintext, cp.ContentStoredEncrypted)},
		{"smpp_accounts", "status", stringify(cp.AccountActive, cp.AccountSuspended, cp.AccountClosed)},
		{"smpp_accounts", "sender_id_policy", stringify(cp.SenderIDStrict, cp.SenderIDAllowUnregisteredNum, cp.SenderIDPolicyDisabled)},
		{"smpp_accounts", "allowed_bind_types", stringify(cp.BindTX, cp.BindRX, cp.BindTRX)},
		{"credentials", "type", stringify(cp.CredentialSMPPBind, cp.CredentialAPIKey)},
		{"credentials", "status", stringify(cp.CredentialActive, cp.CredentialDisabled, cp.CredentialRevoked)},
		{"sender_ids", "status", stringify(cp.SenderIDPendingCarrierApproval, cp.SenderIDActive, cp.SenderIDDisabled)},
		{"smsc_connectors", "bind_type", stringify(cp.BindTX, cp.BindRX, cp.BindTRX)},
		{"smsc_connectors", "status", stringify(cp.ConnectorActive, cp.ConnectorDegraded, cp.ConnectorDisabled)},
		{"routes", "distribution_strategy", stringify(
			cp.DistributionStatic, cp.DistributionRoundRobin, cp.DistributionWeighted,
			cp.DistributionFailoverPriority, cp.DistributionLeastLoaded, cp.DistributionHashBased)},
		{"routes", "status", stringify(cp.RouteActive, cp.RouteDisabled)},
	}

	for _, tc := range cases {
		t.Run(tc.table+"."+tc.column, func(t *testing.T) {
			block, ok := blocks[tc.table]
			if !ok {
				t.Fatalf("no CREATE TABLE block for control_plane.%s in the migration", tc.table)
			}
			want := checkInValues(t, block, tc.column)
			if len(want) == 0 {
				t.Fatalf("no CHECK (%s IN (...)) found in table %s", tc.column, tc.table)
			}
			assertSameSet(t, want, tc.got)
		})
	}
}

// tableBlocks maps each control_plane table name to the body of its CREATE TABLE statement.
func tableBlocks(sql string) map[string]string {
	re := regexp.MustCompile(`(?s)CREATE TABLE control_plane\.(\w+)\s*\((.*?)\n\);`)
	out := map[string]string{}
	for _, m := range re.FindAllStringSubmatch(sql, -1) {
		out[m[1]] = m[2]
	}
	return out
}

// checkInValues extracts the quoted literals of a `CHECK (<column> IN ('a','b',...))` inside a
// table block.
func checkInValues(t *testing.T, block, column string) []string {
	t.Helper()
	re := regexp.MustCompile(`CHECK\s*\(\s*` + regexp.QuoteMeta(column) + `\s+IN\s*\(([^)]*)\)`)
	m := re.FindStringSubmatch(block)
	if m == nil {
		return nil
	}
	lit := regexp.MustCompile(`'([^']*)'`)
	var vals []string
	for _, q := range lit.FindAllStringSubmatch(m[1], -1) {
		vals = append(vals, q[1])
	}
	return vals
}

func assertSameSet(t *testing.T, want, got []string) {
	t.Helper()
	w := append([]string(nil), want...)
	g := append([]string(nil), got...)
	sort.Strings(w)
	sort.Strings(g)
	if len(w) != len(g) {
		t.Fatalf("value set differs:\n DDL:  %v\n Go:   %v", w, g)
	}
	for i := range w {
		if w[i] != g[i] {
			t.Fatalf("value set differs:\n DDL:  %v\n Go:   %v", w, g)
		}
	}
}

// stringify renders enum constants (any ~string type) to their wire strings.
func stringify[T ~string](vals ...T) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = string(v)
	}
	return out
}
