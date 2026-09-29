package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

var loadConnector = connectorSpec{Host: "smsc-simulator", Port: 2775, SystemID: "gateway", Password: "pw"}

func TestSeedLoadSpreadsTheRunOverSeveralCustomers(t *testing.T) {
	f := newFakeAdmin(t)
	spec := loadSpec{Customers: 3, BindPoolSize: 26, PortedShare: 0.2}

	keys1, err := seedLoad(context.Background(), f.admin(), loadConnector, spec)
	if err != nil {
		t.Fatalf("premier seed-load : %v", err)
	}
	if len(keys1) != 3 || keys1[0] == "" || keys1[0] == keys1[1] || keys1[1] == keys1[2] {
		t.Fatalf("clés %q, want trois clés distinctes, une par client", keys1)
	}
	for i := range 3 {
		name := fmt.Sprintf("load-%02d", i)
		if b := f.billing[f.customerID(name)]; !b.enabled || b.mode != "postpaid" || b.hard == nil || *b.hard {
			t.Errorf("%s : facturation %+v, want activée, postpaid, plafond non bloquant", name, b)
		}
		if got := f.senderStatus(name, senderAddr); got != "active" {
			t.Errorf("%s : sender ID %q, want active", name, got)
		}
	}
	if b := f.billing[f.customerID(customerName)]; b.enabled {
		t.Errorf("le client %q du smoke a été basculé en facturation", customerName)
	}
	if got := f.bindPoolSize(connectorName); got != 26 {
		t.Errorf("bind_pool_size %d, want 26", got)
	}
	if !f.autoReconnect(connectorName) {
		t.Error("auto-reconnexion coupée : au premier reset du pair, le connecteur se gare jusqu'à un rebind manuel (§6.13)")
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

	keys2, err := seedLoad(context.Background(), f.admin(), loadConnector, spec)
	if err != nil {
		t.Fatalf("rejeu : %v", err)
	}
	for i := range keys2 {
		if keys2[i] == keys1[i] {
			t.Errorf("client %d : clé inchangée au rejeu", i)
		}
	}
	if got := f.activeAPIKeys(loadAccountName); got != 3 {
		t.Errorf("%d clés api_key actives après rejeu, want 3 (une par client)", got)
	}
	if !f.hasActiveCredential(accountName, smokeSystemID) {
		t.Errorf("la credential smpp_bind %q du smoke a été révoquée", smokeSystemID)
	}
}

func TestSeedLoadRevivesARevokedKey(t *testing.T) {
	f := newFakeAdmin(t)
	spec := loadSpec{Customers: 2, BindPoolSize: 1}
	if _, err := seedLoad(context.Background(), f.admin(), loadConnector, spec); err != nil {
		t.Fatal(err)
	}
	f.revokeAPIKey(loadAccountName)

	keys, err := seedLoad(context.Background(), f.admin(), loadConnector, spec)
	if err != nil {
		t.Fatalf("seed-load après révocation : %v", err)
	}
	if len(keys) != 2 || f.activeAPIKeys(loadAccountName) != 2 {
		t.Errorf("clés %q, %d clés actives, want deux clés actives", keys, f.activeAPIKeys(loadAccountName))
	}
}

func TestSeedLoadWithoutPortedShareImportsNothing(t *testing.T) {
	f := newFakeAdmin(t)
	if _, err := seedLoad(context.Background(), f.admin(), loadConnector, loadSpec{Customers: 1, BindPoolSize: 1}); err != nil {
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
		{"velocity sur un client de charge", "velocity", "customer", "active", true, true},
		{"duplicate désactivée", "duplicate", "global", "disabled", false, false},
		{"liste noire de contenu", "content_blacklist", "global", "active", false, false},
		{"velocity sur un autre client", "velocity", "customer", "active", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAdmin(t)
			spec := loadSpec{Customers: 2, BindPoolSize: 1}
			if _, err := seedLoad(context.Background(), f.admin(), loadConnector, spec); err != nil {
				t.Fatal(err)
			}
			rule := fakeAntispamRule{ID: uuid.NewString(), RuleType: tc.ruleType, Scope: tc.scope, Status: tc.status}
			if tc.scope != "global" {
				other := f.customerID(customerName)
				if tc.scoped {
					other = f.customerID("load-01")
				}
				rule.ScopeID = &other
			}
			f.antispamRules = append(f.antispamRules, rule)

			_, err := seedLoad(context.Background(), f.admin(), loadConnector, spec)
			if tc.refused && (err == nil || !strings.Contains(err.Error(), rule.ID)) {
				t.Fatalf("err = %v, want un refus qui nomme la règle %s", err, rule.ID)
			}
			if !tc.refused && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}
