package adminapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

type transferFixture struct {
	rdb      *goredis.Client
	repo     *postgres.BillingRepo
	customer cp.Customer
	accounts *fakeAccountStore
	src, dst uuid.UUID
}

func newTransferFixture(t *testing.T, funded int) transferFixture {
	t.Helper()
	pool := pgtest.Pool(t)
	ctx := context.Background()
	f := transferFixture{rdb: redistest.Client(t), repo: postgres.NewBillingRepo(pool), accounts: newFakeAccountStore()}
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name, balance_scope)
		VALUES ('transfer-test', 'smpp_account') RETURNING id`).Scan(&f.customer.ID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	f.customer.BalanceScope = cp.BalanceScopeSMPPAccount
	f.customer.UpdatedAt = time.Now().UTC()
	for _, id := range []*uuid.UUID{&f.src, &f.dst} {
		if err := pool.QueryRow(ctx, `INSERT INTO control_plane.smpp_accounts (customer_id, name) VALUES ($1, $2) RETURNING id`,
			f.customer.ID, uuid.NewString()).Scan(id); err != nil {
			t.Fatalf("seed account: %v", err)
		}
		f.accounts.byID[*id] = cp.Account{ID: *id, CustomerID: f.customer.ID}
	}
	if _, _, err := f.repo.Topup(ctx, cp.LedgerEntry{
		OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: f.src, Direction: cp.BillingDirectionMT,
		CustomerID: f.customer.ID, AccountID: &f.src, EntryType: cp.EntryTopup, Credits: funded,
	}); err != nil {
		t.Fatalf("seed balance: %v", err)
	}
	return f
}

func (f transferFixture) source() billing.Owner {
	return billing.Owner{Type: cp.OwnerTypeSMPPAccount, ID: f.src, CustomerID: f.customer.ID, AccountID: &f.src}
}

func (f transferFixture) transfer(t *testing.T, cache adminapi.BalanceCacheInvalidator, credits int) int {
	t.Helper()
	api := newTestAPIWith(t, adminapi.Deps{
		Customers: fakeBillingCustomerStore{c: f.customer}, Accounts: f.accounts, Billing: f.repo, BalanceCache: cache,
	})
	body := `{"credits":` + strconv.Itoa(credits) + `,"direction":"mt","from_owner_id":"` + f.src.String() +
		`","to_owner_id":"` + f.dst.String() + `","idempotency_key":"` + uuid.NewString() + `"}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customers/"+f.customer.ID.String()+"/billing/transfer", body))
	return w.Code
}

type redisInvalidator struct{ rdb *goredis.Client }

func (c redisInvalidator) Del(ctx context.Context, keys ...string) error {
	return billing.InvalidateBalanceCaches(ctx, c.rdb, keys...)
}

type failingInvalidator struct{}

func (failingInvalidator) Del(context.Context, ...string) error {
	return errors.New("redis unreachable")
}

// heldReserveStore holds the first reserve's durable commit until released: its credit is debited in Redis
// and not yet in the durable balance.
type heldReserveStore struct {
	billing.LedgerStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *heldReserveStore) RecordDurable(ctx context.Context, entry cp.LedgerEntry) (int, bool, error) {
	first := false
	s.once.Do(func() { first = true })
	if first {
		close(s.entered)
		<-s.release
	}
	return s.LedgerStore.RecordDurable(ctx, entry)
}

// TestTransferCountsReservesInFlight: the source's 100 credits are reserved in Redis, the reserve not yet
// durable. A transfer guarded by the durable balance alone would move the same 100 credits a second time.
func TestTransferCountsReservesInFlight(t *testing.T) {
	f := newTransferFixture(t, 100)
	store := &heldReserveStore{LedgerStore: f.repo, entered: make(chan struct{}), release: make(chan struct{})}
	var released sync.Once
	unblock := func() { released.Do(func() { close(store.release) }) }
	t.Cleanup(unblock)
	acc := billing.New(f.rdb, store, billing.WithHoldTTL(time.Minute))

	reserved := make(chan error, 1)
	go func() {
		_, err := acc.Reserve(context.Background(), f.source(), uuid.New(), 100)
		reserved <- err
	}()
	select {
	case <-store.entered:
	case err := <-reserved:
		t.Fatalf("Reserve returned before its durable write: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Reserve never reached its durable write")
	}

	code := f.transfer(t, redisInvalidator{f.rdb}, 100)
	unblock()
	if err := <-reserved; err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if code != http.StatusPaymentRequired {
		t.Fatalf("transfer of 100 while 100 are reserved = %d, want 402", code)
	}
}

// TestTransferLowersTheSourceCacheBeforeItsCommit: the invalidation after a transfer's commit can fail, and
// until it lands the source's warm cache still shows the credit just moved away.
func TestTransferLowersTheSourceCacheBeforeItsCommit(t *testing.T) {
	f := newTransferFixture(t, 100)
	acc := billing.New(f.rdb, f.repo, billing.WithHoldTTL(time.Minute))
	ctx := context.Background()
	if _, err := acc.Reserve(ctx, f.source(), uuid.New(), 1); err != nil {
		t.Fatalf("warming Reserve: %v", err)
	}

	if code := f.transfer(t, failingInvalidator{}, 99); code != http.StatusOK {
		t.Fatalf("transfer of 99 = %d, want 200", code)
	}
	if _, err := acc.Reserve(ctx, f.source(), uuid.New(), 99); !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve of the 99 credits transferred away = %v, want ErrInsufficientCredit", err)
	}
}
