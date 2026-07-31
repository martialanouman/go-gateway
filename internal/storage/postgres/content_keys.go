package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// ContentKeyRepo is the durable store for per-customer content keys (control_plane.content_keys, §6.14). It
// persists only the KMS-wrapped DEK and its metadata — it holds no plaintext key and does no crypto; the
// caller (billing-svc, which owns the KMS) generates and wraps the DEK before it reaches here. The
// content_keys_one_active_idx partial unique index is the source of truth for "at most one active key per
// customer"; this repo serializes concurrent creates/rotations on the customer row so that invariant holds
// without a lost update.
type ContentKeyRepo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// NewContentKeyRepo returns the content-key repository backed by pool.
func NewContentKeyRepo(pool *pgxpool.Pool) *ContentKeyRepo {
	return &ContentKeyRepo{pool: pool, q: sqlcgen.New(pool)}
}

// GetActive returns the customer's active content key, or ErrNotFound when the customer has none yet.
func (r *ContentKeyRepo) GetActive(ctx context.Context, customerID uuid.UUID) (cp.ContentKey, error) {
	row, err := r.q.GetActiveContentKey(ctx, customerID)
	if err != nil {
		return cp.ContentKey{}, translate("get active content key", err)
	}
	return contentKeyFromRow(row), nil
}

// GetByID returns a single content key regardless of status (the read path decrypts a CDR by its key_id, not
// by "the active key", so retired and destroyed keys must be fetchable).
func (r *ContentKeyRepo) GetByID(ctx context.Context, id uuid.UUID) (cp.ContentKey, error) {
	row, err := r.q.GetContentKeyByID(ctx, id)
	if err != nil {
		return cp.ContentKey{}, translate("get content key", err)
	}
	return contentKeyFromRow(row), nil
}

// ListByCustomer returns every key of a customer, newest first (active plus the retired history).
func (r *ContentKeyRepo) ListByCustomer(ctx context.Context, customerID uuid.UUID) ([]cp.ContentKey, error) {
	rows, err := r.q.ListContentKeysByCustomer(ctx, customerID)
	if err != nil {
		return nil, translate("list content keys", err)
	}
	out := make([]cp.ContentKey, 0, len(rows))
	for _, row := range rows {
		out = append(out, contentKeyFromRow(row))
	}
	return out, nil
}

// CreateIfAbsent returns the customer's active key, creating one from the supplied wrapped DEK only if none
// exists — the get-or-create used before encrypting a body. It locks the customer row first so two concurrent
// callers can't both insert (which the partial unique index would reject as a 500): the loser observes the
// winner's key inside the lock and returns it, discarding its own wrapped DEK. created reports whether this
// call inserted the key.
func (r *ContentKeyRepo) CreateIfAbsent(ctx context.Context, customerID uuid.UUID, wrapped []byte, keyRef string) (key cp.ContentKey, created bool, err error) {
	err = pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		qtx := r.q.WithTx(tx)
		if _, lerr := qtx.LockCustomerForContentKey(ctx, customerID); lerr != nil {
			return translate("lock customer", lerr)
		}
		if existing, gerr := qtx.GetActiveContentKey(ctx, customerID); gerr == nil {
			key, created = contentKeyFromRow(existing), false
			return nil
		} else if !errors.Is(gerr, pgx.ErrNoRows) {
			return translate("get active content key", gerr)
		}
		inserted, ierr := insertActiveKey(ctx, qtx, customerID, wrapped, keyRef)
		if ierr != nil {
			return ierr
		}
		key, created = inserted, true
		return nil
	})
	if err != nil {
		return cp.ContentKey{}, false, err
	}
	return key, created, nil
}

// Rotate retires the customer's active key (if any) and makes a new active key from the supplied wrapped DEK,
// in one transaction, then points the customer at it. The retired key is kept so CDRs written under it stay
// decryptable — rotation never destroys key material (that is crypto-shred, a separate deliberate step). The
// customer-row lock serializes it against concurrent creates/rotations.
func (r *ContentKeyRepo) Rotate(ctx context.Context, customerID uuid.UUID, wrapped []byte, keyRef string) (cp.ContentKey, error) {
	var key cp.ContentKey
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		qtx := r.q.WithTx(tx)
		if _, lerr := qtx.LockCustomerForContentKey(ctx, customerID); lerr != nil {
			return translate("lock customer", lerr)
		}
		if _, rerr := qtx.RetireActiveContentKey(ctx, customerID); rerr != nil {
			return translate("retire active content key", rerr)
		}
		inserted, ierr := insertActiveKey(ctx, qtx, customerID, wrapped, keyRef)
		if ierr != nil {
			return ierr
		}
		key = inserted
		return nil
	})
	if err != nil {
		return cp.ContentKey{}, err
	}
	return key, nil
}

// DestroyByCustomer crypto-shreds all of a customer's content keys: it marks every non-destroyed key
// destroyed and erases its wrapped key (so the data key is unrecoverable, even by the KMS), and clears the
// customer's active-key pointer — all in one transaction. The CDR's content_ciphertext is left untouched but
// becomes permanently undecryptable (no CDR rewrite). It returns how many keys were destroyed and is
// idempotent: a second call destroys nothing and returns 0.
func (r *ContentKeyRepo) DestroyByCustomer(ctx context.Context, customerID uuid.UUID) (int, error) {
	var destroyed int64
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		qtx := r.q.WithTx(tx)
		// Take the customer-row lock FIRST, exactly as CreateIfAbsent/Rotate do, so the shred serializes
		// against a concurrent create/rotate. Without it a create committing mid-shred could leave a freshly
		// inserted active key un-destroyed (its uncommitted INSERT is invisible to the destroy's UPDATE) — a
		// key surviving an erasure. With the lock, either the create finishes first (and is then destroyed) or
		// the destroy wins (and the next create makes a fresh key — the intended post-shred semantics).
		if _, lerr := qtx.LockCustomerForContentKey(ctx, customerID); lerr != nil {
			return translate("lock customer", lerr)
		}
		n, derr := qtx.DestroyContentKeysByCustomer(ctx, customerID)
		if derr != nil {
			return translate("destroy content keys", derr)
		}
		if cerr := qtx.ClearCustomerContentKey(ctx, customerID); cerr != nil {
			return translate("clear customer content key", cerr)
		}
		destroyed = n
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(destroyed), nil
}

// insertActiveKey inserts a new active key and points the customer at it. Shared by CreateIfAbsent and Rotate;
// must run inside a transaction that already holds the customer-row lock.
func insertActiveKey(ctx context.Context, qtx *sqlcgen.Queries, customerID uuid.UUID, wrapped []byte, keyRef string) (cp.ContentKey, error) {
	row, err := qtx.InsertContentKey(ctx, sqlcgen.InsertContentKeyParams{
		CustomerID: customerID, WrappedKey: wrapped, KmsKeyRef: keyRef,
	})
	if err != nil {
		return cp.ContentKey{}, translate("insert content key", err)
	}
	key := contentKeyFromRow(row)
	if _, err := qtx.SetCustomerContentKey(ctx, sqlcgen.SetCustomerContentKeyParams{ID: customerID, ContentKeyID: &key.ID}); err != nil {
		return cp.ContentKey{}, translate("set customer content key", err)
	}
	return key, nil
}

func contentKeyFromRow(row sqlcgen.ControlPlaneContentKey) cp.ContentKey {
	return cp.ContentKey{
		ID:          row.ID,
		CustomerID:  row.CustomerID,
		WrappedKey:  row.WrappedKey,
		KMSKeyRef:   row.KmsKeyRef,
		Status:      cp.ContentKeyStatus(row.Status),
		CreatedAt:   tsVal(row.CreatedAt),
		RetiredAt:   tsPtr(row.RetiredAt),
		DestroyedAt: tsPtr(row.DestroyedAt),
	}
}
