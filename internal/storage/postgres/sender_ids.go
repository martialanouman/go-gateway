package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// SenderIDRepo is the sender-IDs repository. It satisfies adminapi.SenderIDStore structurally.
type SenderIDRepo struct {
	q *sqlcgen.Queries
}

// NewSenderIDRepo returns the sender-IDs repository backed by pool.
func NewSenderIDRepo(pool *pgxpool.Pool) *SenderIDRepo {
	return &SenderIDRepo{q: sqlcgen.New(pool)}
}

// Create registers a sender ID under a customer. It starts pending carrier approval (the schema
// default). A duplicate address for the customer violates sender_ids_uq -> conflict (409); an
// unknown customer violates the FK -> validation (422).
func (r *SenderIDRepo) Create(ctx context.Context, in cp.NewSenderID) (cp.SenderID, error) {
	row, err := r.q.CreateSenderID(ctx, sqlcgen.CreateSenderIDParams{
		CustomerID: in.CustomerID,
		Address:    in.Address,
		CreatedBy:  in.CreatedBy,
	})
	if err != nil {
		return cp.SenderID{}, translate("create sender id", err)
	}
	return senderIDFromRow(row), nil
}

// ListByCustomer returns a customer's sender IDs.
func (r *SenderIDRepo) ListByCustomer(ctx context.Context, customerID uuid.UUID) ([]cp.SenderID, error) {
	rows, err := r.q.ListSenderIDsByCustomer(ctx, customerID)
	if err != nil {
		return nil, translate("list sender ids", err)
	}
	out := make([]cp.SenderID, 0, len(rows))
	for _, row := range rows {
		out = append(out, senderIDFromRow(row))
	}
	return out, nil
}

// Update changes a sender ID's status, scoped to its customer. A missing row is ErrNotFound.
func (r *SenderIDRepo) Update(ctx context.Context, customerID, senderID uuid.UUID, p cp.SenderIDPatch) (cp.SenderID, error) {
	row, err := r.q.UpdateSenderID(ctx, sqlcgen.UpdateSenderIDParams{
		CustomerID: customerID,
		ID:         senderID,
		Status:     strPtr(p.Status),
	})
	if err != nil {
		return cp.SenderID{}, translate("update sender id", err)
	}
	return senderIDFromRow(row), nil
}

// Delete removes a sender ID scoped to its customer, or reports ErrNotFound.
func (r *SenderIDRepo) Delete(ctx context.Context, customerID, senderID uuid.UUID) error {
	n, err := r.q.DeleteSenderID(ctx, sqlcgen.DeleteSenderIDParams{CustomerID: customerID, ID: senderID})
	if err != nil {
		return translate("delete sender id", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func senderIDFromRow(row sqlcgen.ControlPlaneSenderID) cp.SenderID {
	return cp.SenderID{
		ID:         row.ID,
		CustomerID: row.CustomerID,
		Address:    row.Address,
		Status:     cp.SenderIDStatus(row.Status),
		CreatedBy:  row.CreatedBy,
		ApprovedAt: tsPtr(row.ApprovedAt),
		CreatedAt:  tsVal(row.CreatedAt),
		UpdatedAt:  tsVal(row.UpdatedAt),
	}
}
