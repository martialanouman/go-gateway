package postgres_test

import (
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestSenderIDRepoCreateWithoutCategoryIsMarketing: the Admin API fills the default before the repository
// sees it, so only a caller that leaves the category nil reaches this one.
func TestSenderIDRepoCreateWithoutCategoryIsMarketing(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := t.Context()
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "CategoryCo-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	s, err := postgres.NewSenderIDRepo(pool).Create(ctx, cp.NewSenderID{CustomerID: customer.ID, Address: "PROMO"})
	if err != nil {
		t.Fatalf("create sender id: %v", err)
	}
	if s.TrafficCategory != cp.TrafficMarketing {
		t.Fatalf("category = %q, want marketing", s.TrafficCategory)
	}
}
