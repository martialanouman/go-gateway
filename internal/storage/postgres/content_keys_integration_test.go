package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestContentKeyCreateIfAbsentIsIdempotent: the first call creates the active key; a second returns the same
// key without creating a duplicate (created=false), preserving the one-active invariant.
func TestContentKeyCreateIfAbsentIsIdempotent(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentKeyRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ck-create') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	first, created, err := repo.CreateIfAbsent(ctx, customerID, []byte("wrapped-1"), "kek/1")
	if err != nil || !created {
		t.Fatalf("first CreateIfAbsent = (created=%v, %v), want created", created, err)
	}
	if first.Status != cp.ContentKeyActive {
		t.Fatalf("first key status = %q, want active", first.Status)
	}
	second, created2, err := repo.CreateIfAbsent(ctx, customerID, []byte("wrapped-2"), "kek/1")
	if err != nil || created2 {
		t.Fatalf("second CreateIfAbsent = (created=%v, %v), want not created", created2, err)
	}
	if second.ID != first.ID {
		t.Errorf("second key id = %s, want the first %s (duplicate active created)", second.ID, first.ID)
	}

	keys, err := repo.ListByCustomer(ctx, customerID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("customer has %d keys, want exactly 1 active", len(keys))
	}
}

// TestContentKeyRotateRetainsOldKey: rotation makes a new active key and demotes the old one to retired,
// keeping it (so old CDRs stay decryptable). customers.content_key_id points at the new active key.
func TestContentKeyRotateRetainsOldKey(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentKeyRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ck-rotate') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	old, _, err := repo.CreateIfAbsent(ctx, customerID, []byte("wrapped-old"), "kek/1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	fresh, err := repo.Rotate(ctx, customerID, []byte("wrapped-new"), "kek/1")
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if fresh.ID == old.ID || fresh.Status != cp.ContentKeyActive {
		t.Fatalf("rotated key = %s/%s, want a new active key", fresh.ID, fresh.Status)
	}

	// The active key is the fresh one.
	active, err := repo.GetActive(ctx, customerID)
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active.ID != fresh.ID {
		t.Errorf("active key = %s, want the freshly rotated %s", active.ID, fresh.ID)
	}
	// The old key is retained and retired (still decryptable), with retired_at set.
	oldReloaded, err := repo.GetByID(ctx, old.ID)
	if err != nil {
		t.Fatalf("get old by id: %v", err)
	}
	if oldReloaded.Status != cp.ContentKeyRetired {
		t.Errorf("old key status = %q, want retired", oldReloaded.Status)
	}
	if oldReloaded.RetiredAt == nil {
		t.Error("old key retired_at is nil, want a timestamp")
	}

	// customers.content_key_id points at the new active key.
	var pointed uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT content_key_id FROM control_plane.customers WHERE id = $1`, customerID).Scan(&pointed); err != nil {
		t.Fatalf("read content_key_id: %v", err)
	}
	if pointed != fresh.ID {
		t.Errorf("customers.content_key_id = %s, want %s", pointed, fresh.ID)
	}
}

// TestContentKeyOnlyOneActive: after several rotations the one-active partial unique index still holds —
// exactly one active key, the rest retired.
func TestContentKeyOnlyOneActive(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentKeyRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ck-oneactive') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	if _, _, err := repo.CreateIfAbsent(ctx, customerID, []byte("w0"), "kek/1"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := repo.Rotate(ctx, customerID, []byte("w"), "kek/1"); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
	var active int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.content_keys WHERE customer_id = $1 AND status = 'active'`, customerID).Scan(&active); err != nil {
		t.Fatalf("count active: %v", err)
	}
	if active != 1 {
		t.Errorf("active key count = %d, want 1", active)
	}
	keys, _ := repo.ListByCustomer(ctx, customerID)
	if len(keys) != 4 {
		t.Errorf("total keys = %d, want 4 (1 active + 3 retired)", len(keys))
	}
}

// TestContentKeyCreateIfAbsentConcurrent: many concurrent CreateIfAbsent on a fresh customer converge on a
// single active key — the customer-row lock serializes them so the one-active partial index is never
// violated (no 500, no duplicate). This is the load-bearing invariant the lock exists to protect.
func TestContentKeyCreateIfAbsentConcurrent(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentKeyRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ck-concurrent') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	const n = 8
	ids := make([]uuid.UUID, n)
	errsCh := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key, _, err := repo.CreateIfAbsent(ctx, customerID, []byte("w"), "kek/1")
			ids[i], errsCh[i] = key.ID, err
		}(i)
	}
	wg.Wait()

	for i, err := range errsCh {
		if err != nil {
			t.Fatalf("goroutine %d: CreateIfAbsent errored: %v", i, err)
		}
	}
	// Every caller observed the same single key...
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("goroutine %d got key %s, want the single active %s", i, ids[i], ids[0])
		}
	}
	// ...and the DB holds exactly one key for the customer.
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.content_keys WHERE customer_id = $1`, customerID).Scan(&count); err != nil {
		t.Fatalf("count keys: %v", err)
	}
	if count != 1 {
		t.Errorf("customer has %d keys after %d concurrent creates, want 1", count, n)
	}
}

// TestContentKeyDestroyByCustomerCryptoShreds: destroying a customer's keys marks every key destroyed, erases
// the wrapped key (unrecoverable), and clears the active pointer — without touching the keys' identities (the
// rows stay, so old CDR key_ids still resolve to a destroyed key). Idempotent.
func TestContentKeyDestroyByCustomerCryptoShreds(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentKeyRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ck-shred') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	// One active + one retired (after a rotation).
	if _, _, err := repo.CreateIfAbsent(ctx, customerID, []byte("wrapped-1"), "kek/1"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := repo.Rotate(ctx, customerID, []byte("wrapped-2"), "kek/1"); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	n, err := repo.DestroyByCustomer(ctx, customerID)
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if n != 2 {
		t.Errorf("destroyed %d keys, want 2 (active + retired)", n)
	}

	// Every key is destroyed with its wrapped key erased; the rows are still present (no CDR/key rewrite of ids).
	rows, err := repo.ListByCustomer(ctx, customerID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("keys after shred = %d, want 2 (rows retained)", len(rows))
	}
	for _, k := range rows {
		if k.Status != cp.ContentKeyDestroyed || k.DestroyedAt == nil || len(k.WrappedKey) != 0 {
			t.Errorf("key %s = {status:%q destroyed_at:%v wrapped_len:%d}, want destroyed + erased", k.ID, k.Status, k.DestroyedAt, len(k.WrappedKey))
		}
	}
	// The customer no longer points at an active key.
	var ptr *uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT content_key_id FROM control_plane.customers WHERE id = $1`, customerID).Scan(&ptr); err != nil {
		t.Fatalf("read content_key_id: %v", err)
	}
	if ptr != nil {
		t.Errorf("content_key_id = %v, want NULL after shred", ptr)
	}

	// Idempotent: a second destroy shreds nothing.
	if n2, err := repo.DestroyByCustomer(ctx, customerID); err != nil || n2 != 0 {
		t.Errorf("second destroy = (%d, %v), want (0, nil)", n2, err)
	}
}

// TestContentKeyDestroyVsCreateConcurrent: a crypto-shred racing a create/rotate must never leave an active,
// non-destroyed key behind — the customer-row lock serializes them. After the dust settles there is at most
// one non-destroyed key (a create that won the lock after the destroy), never a survivor of the shred.
func TestContentKeyDestroyVsCreateConcurrent(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentKeyRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ck-destroy-race') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	if _, _, err := repo.CreateIfAbsent(ctx, customerID, []byte("w0"), "kek/1"); err != nil {
		t.Fatalf("seed key: %v", err)
	}

	const n = 12
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, _ = repo.DestroyByCustomer(ctx, customerID)
			} else {
				_, _, _ = repo.CreateIfAbsent(ctx, customerID, []byte("w"), "kek/1")
			}
		}(i)
	}
	wg.Wait()

	// Whatever the interleaving, a shred must never be bypassed: there is at most one non-destroyed key, and
	// if one exists it is the active one (a create that ran strictly after the last destroy).
	var nonDestroyed, active int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.content_keys WHERE customer_id=$1 AND status != 'destroyed'`, customerID).Scan(&nonDestroyed); err != nil {
		t.Fatalf("count non-destroyed: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.content_keys WHERE customer_id=$1 AND status = 'active'`, customerID).Scan(&active); err != nil {
		t.Fatalf("count active: %v", err)
	}
	if nonDestroyed > 1 || nonDestroyed != active {
		t.Errorf("non-destroyed=%d active=%d, want at most one and all non-destroyed are active (no shred survivor)", nonDestroyed, active)
	}
	// A final destroy makes everything unreadable.
	if _, err := repo.DestroyByCustomer(ctx, customerID); err != nil {
		t.Fatalf("final destroy: %v", err)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM control_plane.content_keys WHERE customer_id=$1 AND status != 'destroyed'`, customerID).Scan(&remaining); err != nil {
		t.Fatalf("count after final destroy: %v", err)
	}
	if remaining != 0 {
		t.Errorf("after final destroy, %d keys survive, want 0", remaining)
	}
}

// TestContentKeyGetActiveNotFound: a customer with no key yet yields ErrNotFound, a legitimate "none" the
// caller turns into a create.
func TestContentKeyGetActiveNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewContentKeyRepo(pool)

	var customerID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.customers (name) VALUES ('ck-none') RETURNING id`).Scan(&customerID); err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	if _, err := repo.GetActive(ctx, customerID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("GetActive on keyless customer = %v, want not_found", err)
	}
}
