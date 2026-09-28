package main

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
)

var loadConnector = connectorSpec{Host: "smsc-simulator", Port: 2775, SystemID: "gateway", Password: "pw"}

func TestSeedLoadPreparesTheProductionPathAndHandsOutAFreshKey(t *testing.T) {
	f := newFakeAdmin(t)
	spec := loadSpec{BindPoolSize: 26, PortedShare: 0.2}

	key1, err := seedLoad(context.Background(), f.admin(), loadConnector, spec)
	if err != nil {
		t.Fatalf("premier seed-load : %v", err)
	}
	customerID := f.customerID(customerName)
	if b := f.billing[customerID]; !b.enabled || b.mode != "postpaid" || b.creditLimit != nil {
		t.Errorf("facturation %+v, want activée, postpaid, sans plafond", b)
	}
	if got := f.bindPoolSize(connectorName); got != 26 {
		t.Errorf("bind_pool_size %d, want 26", got)
	}
	if got := len(f.exactRoutes); got != 2000 {
		t.Fatalf("%d routes exactes, want 2000 (20 %% des 10 000 destinations de k6)", got)
	}
	first, last := f.exactRoutes[0], f.exactRoutes[1999]
	if first.MSISDN != "+2250700000000" || last.MSISDN != "+2250700001999" {
		t.Errorf("routes exactes %s … %s, want +2250700000000 … +2250700001999", first.MSISDN, last.MSISDN)
	}
	if first.TargetType != "connector" || first.TargetID != f.connectorID(connectorName) {
		t.Errorf("route exacte vise %s %s, want le connecteur %q", first.TargetType, first.TargetID, connectorName)
	}

	key2, err := seedLoad(context.Background(), f.admin(), loadConnector, spec)
	if err != nil {
		t.Fatalf("rejeu : %v", err)
	}
	if key1 == "" || key2 == key1 {
		t.Errorf("clés %q puis %q, want deux clés distinctes", key1, key2)
	}
	if got := f.activeAPIKeys(accountName); got != 1 {
		t.Errorf("%d clés api_key actives après rejeu, want 1 (l'ancienne révoquée)", got)
	}
	if !f.hasActiveCredential(accountName, smokeSystemID) {
		t.Errorf("la credential smpp_bind %q du smoke a été révoquée", smokeSystemID)
	}
}

func TestSeedLoadWithoutPortedShareImportsNothing(t *testing.T) {
	f := newFakeAdmin(t)
	if _, err := seedLoad(context.Background(), f.admin(), loadConnector, loadSpec{BindPoolSize: 1}); err != nil {
		t.Fatal(err)
	}
	if len(f.exactRoutes) != 0 {
		t.Errorf("%d routes exactes, want 0", len(f.exactRoutes))
	}
}

func TestSeedLoadRefusesAnAntispamRuleThatWouldFlagTheRun(t *testing.T) {
	for _, tc := range []struct {
		name, ruleType, scope, status string
		scoped, refused               bool
	}{
		{"duplicate global", "duplicate", "global", "active", false, true},
		{"velocity sur le client", "velocity", "customer", "active", true, true},
		{"duplicate désactivée", "duplicate", "global", "disabled", false, false},
		{"liste noire de contenu", "content_blacklist", "global", "active", false, false},
		{"velocity sur un autre client", "velocity", "customer", "active", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAdmin(t)
			if _, err := seed(context.Background(), f.admin(), loadConnector); err != nil {
				t.Fatal(err)
			}
			rule := fakeAntispamRule{ID: uuid.NewString(), RuleType: tc.ruleType, Scope: tc.scope, Action: "block", Status: tc.status}
			if tc.scope != "global" {
				other := uuid.NewString()
				if tc.scoped {
					other = f.customerID(customerName)
				}
				rule.ScopeID = &other
			}
			f.antispamRules = append(f.antispamRules, rule)

			_, err := seedLoad(context.Background(), f.admin(), loadConnector, loadSpec{BindPoolSize: 1})
			if tc.refused && (err == nil || !strings.Contains(err.Error(), rule.ID)) {
				t.Fatalf("err = %v, want un refus qui nomme la règle %s", err, rule.ID)
			}
			if !tc.refused && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}
