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
