package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
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

// ListActive returns every active sender ID across all customers, for the sender-ID authorization
// snapshot (step-060). A pending or disabled registration is excluded — it must not authorize a
// source address.
func (r *SenderIDRepo) ListActive(ctx context.Context) ([]cp.SenderID, error) {
	rows, err := r.q.ListActiveSenderIDs(ctx)
	if err != nil {
		return nil, translate("list active sender ids", err)
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

// Delete removes a sender ID scoped to its customer. A missing row is ErrNotFound; one that has already
// sent (first_used_at set) is ErrConflict and stays (ADR-0023).
func (r *SenderIDRepo) Delete(ctx context.Context, customerID, senderID uuid.UUID) error {
	n, err := r.q.DeleteSenderID(ctx, sqlcgen.DeleteSenderIDParams{CustomerID: customerID, ID: senderID})
	if err != nil {
		return translate("delete sender id", err)
	}
	if n > 0 {
		return nil
	}
	if _, err := r.q.GetSenderID(ctx, sqlcgen.GetSenderIDParams{CustomerID: customerID, ID: senderID}); err != nil {
		return translate("delete sender id", err)
	}
	return fmt.Errorf("delete sender id: already used: %w", errs.ErrConflict)
}

// MarkFirstUsed sets first_used_at on each matching sender ID that has none yet. An address that is
// not a registered sender ID of its customer matches no row, which is not an error.
func (r *SenderIDRepo) MarkFirstUsed(ctx context.Context, uses []cp.SenderIDUse) error {
	p := sqlcgen.MarkSenderIDsFirstUsedParams{
		CustomerIds: make([]uuid.UUID, len(uses)),
		Addresses:   make([]string, len(uses)),
		UsedAts:     make([]pgtype.Timestamptz, len(uses)),
	}
	for i, u := range uses {
		p.CustomerIds[i], p.Addresses[i], p.UsedAts[i] = u.CustomerID, u.Address, tsFrom(u.UsedAt)
	}
	return translate("mark sender ids first used", r.q.MarkSenderIDsFirstUsed(ctx, p))
}

func senderIDFromRow(row sqlcgen.ControlPlaneSenderID) cp.SenderID {
	return cp.SenderID{
		ID:          row.ID,
		CustomerID:  row.CustomerID,
		Address:     row.Address,
		Status:      cp.SenderIDStatus(row.Status),
		CreatedBy:   row.CreatedBy,
		ApprovedAt:  tsPtr(row.ApprovedAt),
		FirstUsedAt: tsPtr(row.FirstUsedAt),
		CreatedAt:   tsVal(row.CreatedAt),
		UpdatedAt:   tsVal(row.UpdatedAt),
	}
}
