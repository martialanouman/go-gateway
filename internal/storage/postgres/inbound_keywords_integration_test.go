package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// keywordDeps builds the repos and a valid (numberID, accountID) pair for the keyword tests. The
// number and account are the two FK targets every keyword needs. address must be unique per test:
// pgtest.Pool shares one database across the package, and inbound_numbers_uq (address, country_code)
// would otherwise collide between tests.
func keywordDeps(t *testing.T, address string) (*postgres.InboundKeywordRepo, uuid.UUID, uuid.UUID) {
	t.Helper()
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	numbers := postgres.NewInboundNumberRepo(pool)
	repo := postgres.NewInboundKeywordRepo(pool)
	ctx := context.Background()

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "KeywordCo-" + address})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	acct, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "keyword-app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	num, err := numbers.Create(ctx, cp.NewInboundNumber{Address: address, NumberType: cp.NumberShortcode, CountryCode: "FR"})
	if err != nil {
		t.Fatalf("create inbound number: %v", err)
	}
	return repo, num.ID, acct.ID
}

// TestInboundKeywordRepoCRUD walks create -> list -> update against PostgreSQL, checking the DDL
// default status ('active') comes back and a partial update leaves untouched fields alone.
func TestInboundKeywordRepoCRUD(t *testing.T) {
	repo, numberID, accountID := keywordDeps(t, "38000")
	ctx := context.Background()

	created, err := repo.Create(ctx, cp.NewInboundKeyword{
		InboundNumberID: numberID,
		Keyword:         "INFO",
		MatchType:       cp.MatchPrefix,
		AccountID:       accountID,
		Priority:        0,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Status != cp.InboundKeywordActive {
		t.Errorf("status = %q, want active (the DDL default)", created.Status)
	}
	if created.MatchType != cp.MatchPrefix || created.Keyword != "INFO" {
		t.Errorf("created = %+v, want keyword INFO / match_type prefix", created)
	}

	list, err := repo.ListByInboundNumber(ctx, numberID)
	if err != nil {
		t.Fatalf("ListByInboundNumber() error = %v", err)
	}
	if len(list) != 1 || list[0].ID != created.ID {
		t.Errorf("List = %+v, want the one created keyword", list)
	}

	disabled := cp.InboundKeywordDisabled
	newKw := "HELP"
	updated, err := repo.Update(ctx, numberID, created.ID, cp.InboundKeywordPatch{Keyword: &newKw, Status: &disabled})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.Keyword != "HELP" || updated.Status != cp.InboundKeywordDisabled {
		t.Errorf("updated = %+v, want keyword HELP / status disabled", updated)
	}
	// match_type and account_id must survive a keyword+status patch.
	if updated.MatchType != cp.MatchPrefix || updated.AccountID != accountID {
		t.Errorf("untouched fields changed on a partial patch: %+v", updated)
	}
}

// TestInboundKeywordRepoListOrderedByPriority proves ListByInboundNumber returns keywords by ascending
// priority, matching the MO evaluation order.
func TestInboundKeywordRepoListOrderedByPriority(t *testing.T) {
	repo, numberID, accountID := keywordDeps(t, "38001")
	ctx := context.Background()

	for _, kw := range []struct {
		word     string
		priority int
	}{{"THIRD", 30}, {"FIRST", 10}, {"SECOND", 20}} {
		if _, err := repo.Create(ctx, cp.NewInboundKeyword{
			InboundNumberID: numberID, Keyword: kw.word, MatchType: cp.MatchExact,
			AccountID: accountID, Priority: kw.priority,
		}); err != nil {
			t.Fatalf("create %s: %v", kw.word, err)
		}
	}

	list, err := repo.ListByInboundNumber(ctx, numberID)
	if err != nil {
		t.Fatalf("List error = %v", err)
	}
	got := []string{list[0].Keyword, list[1].Keyword, list[2].Keyword}
	want := []string{"FIRST", "SECOND", "THIRD"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("order = %v, want %v (ascending priority)", got, want)
			break
		}
	}
}

// TestInboundKeywordRepoListAll proves ListAll returns active keywords across numbers for the MO
// snapshot, and excludes disabled ones.
func TestInboundKeywordRepoListAll(t *testing.T) {
	repo, numberID, accountID := keywordDeps(t, "39000")
	ctx := context.Background()

	active, err := repo.Create(ctx, cp.NewInboundKeyword{
		InboundNumberID: numberID, Keyword: "INFO", MatchType: cp.MatchPrefix, AccountID: accountID,
	})
	if err != nil {
		t.Fatalf("create active: %v", err)
	}
	disabledKw, err := repo.Create(ctx, cp.NewInboundKeyword{
		InboundNumberID: numberID, Keyword: "OLD", MatchType: cp.MatchExact, AccountID: accountID,
	})
	if err != nil {
		t.Fatalf("create disabled: %v", err)
	}
	disabled := cp.InboundKeywordDisabled
	if _, err := repo.Update(ctx, numberID, disabledKw.ID, cp.InboundKeywordPatch{Status: &disabled}); err != nil {
		t.Fatalf("disable: %v", err)
	}

	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	var sawActive, sawDisabled bool
	for _, kw := range all {
		if kw.ID == active.ID {
			sawActive = true
		}
		if kw.ID == disabledKw.ID {
			sawDisabled = true
		}
	}
	if !sawActive {
		t.Error("ListAll must include the active keyword")
	}
	if sawDisabled {
		t.Error("ListAll must exclude the disabled keyword")
	}
}

// TestInboundKeywordRepoScopedToNumber proves a keyword is only visible and mutable within its own
// number: listing a different number omits it, and updating/deleting it under the wrong number is a
// not-found.
func TestInboundKeywordRepoScopedToNumber(t *testing.T) {
	repo, numberID, accountID := keywordDeps(t, "38002")
	ctx := context.Background()

	kw, err := repo.Create(ctx, cp.NewInboundKeyword{
		InboundNumberID: numberID, Keyword: "INFO", MatchType: cp.MatchPrefix, AccountID: accountID,
	})
	if err != nil {
		t.Fatalf("create keyword: %v", err)
	}

	otherNumber := uuid.New()
	empty, err := repo.ListByInboundNumber(ctx, otherNumber)
	if err != nil {
		t.Fatalf("List(other) error = %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("List(other number) = %+v, want empty", empty)
	}

	newKw := "HELP"
	if _, err := repo.Update(ctx, otherNumber, kw.ID, cp.InboundKeywordPatch{Keyword: &newKw}); !isNotFound(err) {
		t.Errorf("Update under wrong number err = %v, want not_found", err)
	}
	if err := repo.Delete(ctx, otherNumber, kw.ID); !isNotFound(err) {
		t.Errorf("Delete under wrong number err = %v, want not_found", err)
	}
}

// TestInboundKeywordRepoDeleteAndUpdateMissing report ErrNotFound (404) for an unknown keyword id.
func TestInboundKeywordRepoDeleteAndUpdateMissing(t *testing.T) {
	repo, numberID, _ := keywordDeps(t, "38003")
	ctx := context.Background()

	if err := repo.Delete(ctx, numberID, uuid.New()); !isNotFound(err) {
		t.Errorf("Delete(unknown) err = %v, want not_found", err)
	}
	newKw := "HELP"
	if _, err := repo.Update(ctx, numberID, uuid.New(), cp.InboundKeywordPatch{Keyword: &newKw}); !isNotFound(err) {
		t.Errorf("Update(unknown) err = %v, want not_found", err)
	}
}
