package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestCustomerGroupRepoRoundTrip exercises create, read, update and delete against a real database,
// so the sqlc mapping and the pgtype conversions are validated end to end rather than by hand.
func TestCustomerGroupRepoRoundTrip(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerGroupRepo(pool)
	ctx := context.Background()

	desc := "West-African carriers"
	created, err := repo.Create(ctx, cp.NewCustomerGroup{Name: uniqueGroupName("Roundtrip"), Description: &desc})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID == uuid.Nil {
		t.Fatal("Create() returned the nil UUID")
	}
	if created.Status != cp.CustomerGroupActive {
		t.Errorf("status = %q, want active (the schema default)", created.Status)
	}
	if created.Description == nil || *created.Description != desc {
		t.Errorf("description = %v, want %q", created.Description, desc)
	}

	got, err := repo.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Name != created.Name {
		t.Errorf("Get().Name = %q, want %q", got.Name, created.Name)
	}

	renamed := uniqueGroupName("Renamed")
	archived := cp.CustomerGroupArchived
	updated, err := repo.Update(ctx, created.ID, cp.CustomerGroupPatch{Name: &renamed, Status: &archived})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.Name != renamed {
		t.Errorf("Update().Name = %q, want %q", updated.Name, renamed)
	}
	if updated.Status != cp.CustomerGroupArchived {
		t.Errorf("Update().Status = %q, want archived", updated.Status)
	}
	// A patch leaving description nil must not blank the column.
	if updated.Description == nil || *updated.Description != desc {
		t.Errorf("description = %v after a patch that did not mention it, want %q unchanged",
			updated.Description, desc)
	}

	if err := repo.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.Get(ctx, created.ID); !isNotFound(err) {
		t.Errorf("Get() after Delete error = %v, want not_found", err)
	}
}

// TestCustomerGroupRepoDeleteMissingIsNotFound: deleting nothing is not_found, so the handler can
// answer 404 rather than a silent 204.
func TestCustomerGroupRepoDeleteMissingIsNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerGroupRepo(pool)

	if err := repo.Delete(context.Background(), uuid.New()); !isNotFound(err) {
		t.Errorf("Delete(unknown) error = %v, want not_found", err)
	}
}

// TestCustomerGroupRepoDuplicateNameIsConflict: customer_groups.name is UNIQUE, and the handler
// answers 409 off this code. Renaming onto a taken name is the same conflict, which is why
// update-customer-group declares 409 too.
func TestCustomerGroupRepoDuplicateNameIsConflict(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerGroupRepo(pool)
	ctx := context.Background()

	taken := uniqueGroupName("Taken")
	if _, err := repo.Create(ctx, cp.NewCustomerGroup{Name: taken}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if _, err := repo.Create(ctx, cp.NewCustomerGroup{Name: taken}); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("Create(duplicate name) error = %v, want conflict", err)
	}

	other, err := repo.Create(ctx, cp.NewCustomerGroup{Name: uniqueGroupName("Other")})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := repo.Update(ctx, other.ID, cp.CustomerGroupPatch{Name: &taken}); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("Update(onto a taken name) error = %v, want conflict", err)
	}
}

// TestDeleteCustomerGroupDetachesItsCustomersAndDeletesNone is the §6.17 rule, and it is asserted by
// what the delete PRESERVES: counting the rows that vanished would pass just as well against a table
// that was empty to begin with. Both customers must still be there, with group_id cleared by the
// schema's ON DELETE SET NULL — never by an application-side cascade.
func TestDeleteCustomerGroupDetachesItsCustomersAndDeletesNone(t *testing.T) {
	pool := pgtest.Pool(t)
	groups := postgres.NewCustomerGroupRepo(pool)
	customers := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	group, err := groups.Create(ctx, cp.NewCustomerGroup{Name: uniqueGroupName("Doomed")})
	if err != nil {
		t.Fatalf("Create(group) error = %v", err)
	}

	first, err := customers.Create(ctx, cp.NewCustomer{Name: uniqueGroupName("First"), GroupID: &group.ID})
	if err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}
	second, err := customers.Create(ctx, cp.NewCustomer{Name: uniqueGroupName("Second"), GroupID: &group.ID})
	if err != nil {
		t.Fatalf("Create(second) error = %v", err)
	}

	if err := groups.Delete(ctx, group.ID); err != nil {
		t.Fatalf("Delete(group) error = %v", err)
	}

	for _, want := range []cp.Customer{first, second} {
		got, err := customers.Get(ctx, want.ID)
		if err != nil {
			t.Fatalf("customer %s is gone after deleting its group: %v — the delete must detach, never cascade",
				want.Name, err)
		}
		if got.GroupID != nil {
			t.Errorf("customer %s still has group_id = %v after its group was deleted, want NULL",
				want.Name, *got.GroupID)
		}
	}
}

// TestCustomerGroupRepoListFiltersByStatus: the ?status= filter of list-customer-groups. A nil
// filter returns both, so a filter that silently ignored its argument would still be caught.
func TestCustomerGroupRepoListFiltersByStatus(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerGroupRepo(pool)
	ctx := context.Background()

	shelved, err := repo.Create(ctx, cp.NewCustomerGroup{Name: uniqueGroupName("Zulu")})
	if err != nil {
		t.Fatalf("Create(shelved) error = %v", err)
	}
	live, err := repo.Create(ctx, cp.NewCustomerGroup{Name: uniqueGroupName("Alpha")})
	if err != nil {
		t.Fatalf("Create(live) error = %v", err)
	}
	archived := cp.CustomerGroupArchived
	if _, err := repo.Update(ctx, shelved.ID, cp.CustomerGroupPatch{Status: &archived}); err != nil {
		t.Fatalf("Update(shelved) error = %v", err)
	}

	all, err := repo.List(ctx, cp.CustomerGroupFilter{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if !containsGroup(all, live.ID) || !containsGroup(all, shelved.ID) {
		t.Error("List() with no filter omitted one of the two groups")
	}

	active := cp.CustomerGroupActive
	onlyActive, err := repo.List(ctx, cp.CustomerGroupFilter{Status: &active})
	if err != nil {
		t.Fatalf("List(active) error = %v", err)
	}
	if !containsGroup(onlyActive, live.ID) {
		t.Error("List(active) omitted the active group")
	}
	if containsGroup(onlyActive, shelved.ID) {
		t.Error("List(active) returned the archived group — the status filter is not applied")
	}
}

// TestSetCustomerGroupDrivesBothGroupFilters is the end-to-end the task file asks for: membership
// changed through its own path is what the two ?groupId= filters resolve against. The fixture is
// asymmetric on purpose — with both customers inside the group, a filter returning everything would
// pass.
func TestSetCustomerGroupDrivesBothGroupFilters(t *testing.T) {
	pool := pgtest.Pool(t)
	groups := postgres.NewCustomerGroupRepo(pool)
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	ctx := context.Background()

	group, err := groups.Create(ctx, cp.NewCustomerGroup{Name: uniqueGroupName("Filtered")})
	if err != nil {
		t.Fatalf("Create(group) error = %v", err)
	}
	inside, err := customers.Create(ctx, cp.NewCustomer{Name: uniqueGroupName("Inside"), GroupID: &group.ID})
	if err != nil {
		t.Fatalf("Create(inside) error = %v", err)
	}
	outside, err := customers.Create(ctx, cp.NewCustomer{Name: uniqueGroupName("Outside")})
	if err != nil {
		t.Fatalf("Create(outside) error = %v", err)
	}

	insideAccount := newAccountFor(t, accounts, inside.ID)
	outsideAccount := newAccountFor(t, accounts, outside.ID)

	assertGroupFilters(ctx, t, customers, accounts, group.ID,
		[]uuid.UUID{inside.ID}, []uuid.UUID{insideAccount})

	// Joining the group moves the customer AND its account into both filters.
	if _, err := customers.SetGroup(ctx, outside.ID, &group.ID); err != nil {
		t.Fatalf("SetGroup(outside -> group) error = %v", err)
	}
	assertGroupFilters(ctx, t, customers, accounts, group.ID,
		[]uuid.UUID{inside.ID, outside.ID}, []uuid.UUID{insideAccount, outsideAccount})

	// Clearing it detaches without deleting: the customer survives, outside every group filter.
	if _, err := customers.SetGroup(ctx, inside.ID, nil); err != nil {
		t.Fatalf("SetGroup(inside -> null) error = %v", err)
	}
	detached, err := customers.Get(ctx, inside.ID)
	if err != nil {
		t.Fatalf("customer is gone after being detached: %v", err)
	}
	if detached.GroupID != nil {
		t.Errorf("group_id = %v after SetGroup(nil), want NULL", *detached.GroupID)
	}
	assertGroupFilters(ctx, t, customers, accounts, group.ID,
		[]uuid.UUID{outside.ID}, []uuid.UUID{outsideAccount})
}

// TestSetCustomerGroupUnknownGroupIsValidation: the FK to customer_groups is what rejects an unknown
// group, translated to 422 — which is why set-customer-group declares it and the handler runs no
// pre-flight existence check that a concurrent delete could invalidate anyway.
func TestSetCustomerGroupUnknownGroupIsValidation(t *testing.T) {
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: uniqueGroupName("Orphan")})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	unknown := uuid.New()
	if _, err := customers.SetGroup(ctx, customer.ID, &unknown); !errors.Is(err, errs.ErrValidation) {
		t.Errorf("SetGroup(unknown group) error = %v, want validation", err)
	}
}

// TestSetCustomerGroupUnknownCustomerIsNotFound: no row updated is a missing customer, not a silent
// success.
func TestSetCustomerGroupUnknownCustomerIsNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)

	if _, err := customers.SetGroup(context.Background(), uuid.New(), nil); !isNotFound(err) {
		t.Errorf("SetGroup(unknown customer) error = %v, want not_found", err)
	}
}

// assertGroupFilters checks both ?groupId= filters at once: list-customers and list-smpp-accounts
// resolve the same membership, and a change must move both or neither.
func assertGroupFilters(ctx context.Context, t *testing.T, customers *postgres.CustomerRepo,
	accounts *postgres.AccountRepo, groupID uuid.UUID, wantCustomers, wantAccounts []uuid.UUID,
) {
	t.Helper()

	page, err := customers.List(ctx, cp.CustomerFilter{GroupID: &groupID, Limit: 50})
	if err != nil {
		t.Fatalf("customers.List(groupId) error = %v", err)
	}
	got := make([]uuid.UUID, 0, len(page.Items))
	for _, c := range page.Items {
		got = append(got, c.ID)
	}
	assertSameIDs(t, "list-customers?groupId", got, wantCustomers)

	accountPage, err := accounts.List(ctx, cp.AccountFilter{GroupID: &groupID, Limit: 50})
	if err != nil {
		t.Fatalf("accounts.List(groupId) error = %v", err)
	}
	gotAccounts := make([]uuid.UUID, 0, len(accountPage.Items))
	for _, a := range accountPage.Items {
		gotAccounts = append(gotAccounts, a.ID)
	}
	assertSameIDs(t, "list-smpp-accounts?groupId", gotAccounts, wantAccounts)
}

func assertSameIDs(t *testing.T, what string, got, want []uuid.UUID) {
	t.Helper()
	inGot := make(map[uuid.UUID]bool, len(got))
	for _, id := range got {
		inGot[id] = true
	}
	for _, id := range want {
		if !inGot[id] {
			t.Errorf("%s: %s is missing from the result", what, id)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%s: returned %d rows, want %d — the filter let a non-member through",
			what, len(got), len(want))
	}
}

// newAccountFor gives a customer one SMPP account, so the ?groupId= filter of list-smpp-accounts —
// which resolves through customers.group_id — has something to return.
func newAccountFor(t *testing.T, accounts *postgres.AccountRepo, customerID uuid.UUID) uuid.UUID {
	t.Helper()
	account, err := accounts.Create(context.Background(), cp.NewAccount{
		CustomerID: customerID,
		Name:       uniqueGroupName("app"),
	})
	if err != nil {
		t.Fatalf("Create(account) error = %v", err)
	}
	return account.ID
}

func containsGroup(groups []cp.CustomerGroup, id uuid.UUID) bool {
	for _, g := range groups {
		if g.ID == id {
			return true
		}
	}
	return false
}

// uniqueGroupName keeps a UNIQUE name from colliding with a sibling test: the container is shared by
// the whole package, so a fixed literal makes the second test to run fail on a conflict it did not
// mean to exercise.
func uniqueGroupName(prefix string) string {
	return prefix + "-" + uuid.NewString()
}
