package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestContentAccessAuditRecord: each content:read access appends one audit row with its operator, message and
// outcome — the fact of access, never any plaintext.
func TestContentAccessAuditRecord(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentAccessAuditRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('audit-cust') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	msgID := uuid.New()

	if err := repo.Record(ctx, cp.ContentAccess{Operator: "op-token", MessageID: msgID, CustomerID: &customerID, Outcome: cp.ContentAccessGranted}); err != nil {
		t.Fatalf("record granted: %v", err)
	}
	if err := repo.Record(ctx, cp.ContentAccess{Operator: "op-token", MessageID: uuid.New(), Outcome: cp.ContentAccessNotFound}); err != nil {
		t.Fatalf("record not_found (nil customer): %v", err)
	}

	var (
		operator string
		outcome  string
		gotCust  *uuid.UUID
	)
	if err := pool.QueryRow(ctx, `SELECT operator, outcome, customer_id FROM control_plane.content_access_audit WHERE message_id = $1`, msgID).
		Scan(&operator, &outcome, &gotCust); err != nil {
		t.Fatalf("read audit row: %v", err)
	}
	if operator != "op-token" || outcome != string(cp.ContentAccessGranted) || gotCust == nil || *gotCust != customerID {
		t.Errorf("audit row = {op:%q outcome:%q cust:%v}, want op-token/granted/%s", operator, outcome, gotCust, customerID)
	}

	// The bad outcome value is rejected by the CHECK.
	if err := repo.Record(ctx, cp.ContentAccess{Operator: "op", MessageID: uuid.New(), Outcome: "bogus"}); err == nil {
		t.Error("recording an invalid outcome should be rejected by the CHECK constraint")
	}
}
