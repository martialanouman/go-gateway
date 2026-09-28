package main

import (
	"context"
	"strings"
	"testing"
)

func TestSeedCreatesTheControlPlaneThenReplaysWithoutWriting(t *testing.T) {
	f := newFakeAdmin(t)
	spec := connectorSpec{Host: "smsc-simulator", Port: 2775, SystemID: "gateway", Password: "pw"}

	id1, err := seed(context.Background(), f.admin(), spec)
	if err != nil {
		t.Fatalf("premier seed : %v", err)
	}
	if id1 == "" || f.connectorID(connectorName) != id1 {
		t.Fatalf("connector_id %q, want the id of connector %q", id1, connectorName)
	}
	if got := f.routeTarget(routeName); got != id1 {
		t.Errorf("route %q vise %q, want %q", routeName, got, id1)
	}
	if got := f.senderStatus(customerName, senderAddr); got != "active" {
		t.Errorf("sender ID %q : statut %q, want active", senderAddr, got)
	}
	if !f.hasActiveCredential(accountName, smokeSystemID) {
		t.Errorf("aucune credential smpp_bind %q active sur le compte %q", smokeSystemID, accountName)
	}

	wantConnector := fakeConnectorCreate{
		Name: connectorName, Host: "smsc-simulator", Port: 2775, BindType: "trx", SystemID: "gateway", Password: "pw",
	}
	if f.lastConnector != wantConnector {
		t.Errorf("connecteur créé %+v, want %+v", f.lastConnector, wantConnector)
	}
	if got := f.lastAccount; got.AllowedBindTypes != "trx" || got.MaxSessions != 2 {
		t.Errorf("compte créé : allowed_bind_types %q, max_sessions %d, want trx, 2", got.AllowedBindTypes, got.MaxSessions)
	}
	if got := f.lastCredential; got.Type != "smpp_bind" || got.SystemID == nil || *got.SystemID != smokeSystemID {
		t.Errorf("credential créée %+v, want smpp_bind %q", got, smokeSystemID)
	}

	before := f.writes
	id2, err := seed(context.Background(), f.admin(), spec)
	if err != nil {
		t.Fatalf("rejeu : %v", err)
	}
	if id2 != id1 {
		t.Errorf("rejeu : connector_id %q, want %q", id2, id1)
	}
	if f.writes != before {
		t.Errorf("rejeu : %d écritures, want 0", f.writes-before)
	}
}

func TestSeedActivatesAPendingSenderID(t *testing.T) {
	f := newFakeAdmin(t)
	f.preloadPendingSender(customerName, senderAddr)
	if _, err := seed(context.Background(), f.admin(), connectorSpec{Host: "h", Port: 1, SystemID: "s", Password: "p"}); err != nil {
		t.Fatal(err)
	}
	if got := f.senderStatus(customerName, senderAddr); got != "active" {
		t.Errorf("statut %q, want active", got)
	}
}

func TestFindSmokeAccountReturnsTheAccountSeedCreated(t *testing.T) {
	f := newFakeAdmin(t)
	spec := connectorSpec{Host: "h", Port: 1, SystemID: "s", Password: "p"}
	if _, err := seed(context.Background(), f.admin(), spec); err != nil {
		t.Fatal(err)
	}
	accountID, err := findSmokeAccount(context.Background(), f.admin())
	if err != nil {
		t.Fatalf("findSmokeAccount : %v", err)
	}
	if want := f.accountID(accountName); accountID != want {
		t.Errorf("account_id %q, want %q", accountID, want)
	}
}

func TestFindSmokeAccountFailsWithoutTheCustomer(t *testing.T) {
	f := newFakeAdmin(t)
	if _, err := findSmokeAccount(context.Background(), f.admin()); err == nil {
		t.Fatal("err = nil, want a not-found error")
	}
}

func TestSeedSurfacesAnAdminRefusal(t *testing.T) {
	f := newFakeAdmin(t)
	a := f.admin()
	a.token = "wrong"
	if _, err := seed(context.Background(), a, connectorSpec{Host: "h", Port: 1, SystemID: "s", Password: "p"}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v, want the 401", err)
	}
}
