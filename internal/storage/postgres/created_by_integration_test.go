package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestCreatedByStoresAnOperatorIDNoTableKnows: created_by is the BFF's operator id (ADR-0019), which lives
// in the BFF's own database. The gateway holds no operators table, so any uuid must be written and read back.
func TestCreatedByStoresAnOperatorIDNoTableKnows(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "created-by-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}

	for name, create := range map[string]func(operator *uuid.UUID) (*uuid.UUID, error){
		"customer group": func(operator *uuid.UUID) (*uuid.UUID, error) {
			g, err := postgres.NewCustomerGroupRepo(pool).Create(ctx,
				cp.NewCustomerGroup{Name: uniqueGroupName("CreatedBy"), CreatedBy: operator})
			return g.CreatedBy, err
		},
		"sender rewrite rule": func(operator *uuid.UUID) (*uuid.UUID, error) {
			r, err := postgres.NewSenderRewriteRuleRepo(pool).Create(ctx, cp.NewSenderRewriteRule{
				Scope: cp.RewriteScopePlatform, RewriteType: cp.RewriteSanitize, Priority: 100, CreatedBy: operator,
			})
			return r.CreatedBy, err
		},
		"sender id": func(operator *uuid.UUID) (*uuid.UUID, error) {
			s, err := postgres.NewSenderIDRepo(pool).Create(ctx,
				cp.NewSenderID{CustomerID: customer.ID, Address: "C" + uuid.NewString()[:8], CreatedBy: operator})
			return s.CreatedBy, err
		},
		"routing script": func(operator *uuid.UUID) (*uuid.UUID, error) {
			s := draftScript("created-by-"+uuid.NewString(), "src")
			s.CreatedBy = operator
			saved, err := postgres.NewRoutingScriptRepo(pool).Create(ctx, s)
			return saved.CreatedBy, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			operator := uuid.New()
			got, err := create(&operator)
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if got == nil || *got != operator {
				t.Errorf("created_by = %v, want %s", got, operator)
			}

			got, err = create(nil)
			if err != nil {
				t.Fatalf("Create() without an operator error = %v", err)
			}
			if got != nil {
				t.Errorf("created_by = %s, want NULL", got)
			}
		})
	}
}
