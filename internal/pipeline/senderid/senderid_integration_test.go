package senderid_test

import (
	"context"
	"errors"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestSnapshotAgainstPostgres proves the snapshot loads real control-plane rows: only the customer's
// ACTIVE sender IDs. It exercises the ListActive query end to end (catching a mis-qualified schema or
// projection).
func TestSnapshotAgainstPostgres(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	customers := postgres.NewCustomerRepo(pool)
	senderIDs := postgres.NewSenderIDRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "SenderIDCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}

	// One active sender ID (approved) and one still pending — only the active one must authorize.
	active, err := senderIDs.Create(ctx, cp.NewSenderID{CustomerID: customer.ID, Address: "BANK"})
	if err != nil {
		t.Fatalf("create sender id: %v", err)
	}
	activeStatus := cp.SenderIDActive
	if _, err := senderIDs.Update(ctx, customer.ID, active.ID, cp.SenderIDPatch{Status: &activeStatus}); err != nil {
		t.Fatalf("activate sender id: %v", err)
	}
	if _, err := senderIDs.Create(ctx, cp.NewSenderID{CustomerID: customer.ID, Address: "PENDING"}); err != nil {
		t.Fatalf("create pending sender id: %v", err)
	}

	a, err := senderid.LoadSnapshot(ctx, senderIDs)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if _, err := a.Authorize(ctx, customer.ID, "BANK"); err != nil {
		t.Errorf("active registered sender = %v, want authorized", err)
	}
	if _, err := a.Authorize(ctx, customer.ID, "PENDING"); !errors.Is(err, errs.ErrSenderIDNotAuthorized) {
		t.Errorf("pending sender = %v, want rejected (not active)", err)
	}
	if _, err := a.Authorize(ctx, customer.ID, "36000"); !errors.Is(err, errs.ErrSenderIDNotAuthorized) {
		t.Errorf("unregistered numeric = %v, want rejected", err)
	}
}
