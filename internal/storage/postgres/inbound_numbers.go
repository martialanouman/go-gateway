package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// InboundNumberRepo is the inbound-numbers repository. It satisfies adminapi.InboundNumberStore
// structurally.
type InboundNumberRepo struct {
	q *sqlcgen.Queries
}

// NewInboundNumberRepo returns the inbound-numbers repository backed by pool.
func NewInboundNumberRepo(pool *pgxpool.Pool) *InboundNumberRepo {
	return &InboundNumberRepo{q: sqlcgen.New(pool)}
}

// Create inserts an inbound number. A duplicate (address, country_code) violates inbound_numbers_uq,
// which translate() reports as a conflict (409); an unknown connector_id/account_id is a foreign-key
// violation, reported as validation (422).
func (r *InboundNumberRepo) Create(ctx context.Context, in cp.NewInboundNumber) (cp.InboundNumber, error) {
	row, err := r.q.CreateInboundNumber(ctx, sqlcgen.CreateInboundNumberParams{
		Address:     in.Address,
		NumberType:  string(in.NumberType),
		CountryCode: in.CountryCode,
		Mccmnc:      in.MCCMNC,
		ConnectorID: in.ConnectorID,
		AccountID:   in.AccountID,
	})
	if err != nil {
		return cp.InboundNumber{}, translate("create inbound number", err)
	}
	return inboundNumberFromRow(row), nil
}

// List returns every inbound number, ordered by id. The contract returns a bare array (no pagination).
func (r *InboundNumberRepo) List(ctx context.Context) ([]cp.InboundNumber, error) {
	rows, err := r.q.ListInboundNumbers(ctx)
	if err != nil {
		return nil, translate("list inbound numbers", err)
	}
	out := make([]cp.InboundNumber, 0, len(rows))
	for _, row := range rows {
		out = append(out, inboundNumberFromRow(row))
	}
	return out, nil
}

// Update applies a partial change and returns the inbound number, or ErrNotFound. Address and
// country_code (the unique key) are immutable, and account_id is changed only through Assign.
func (r *InboundNumberRepo) Update(ctx context.Context, id uuid.UUID, p cp.InboundNumberPatch) (cp.InboundNumber, error) {
	row, err := r.q.UpdateInboundNumber(ctx, sqlcgen.UpdateInboundNumberParams{
		ID:          id,
		NumberType:  strPtr(p.NumberType),
		Mccmnc:      p.MCCMNC,
		ConnectorID: p.ConnectorID,
		Status:      strPtr(p.Status),
	})
	if err != nil {
		return cp.InboundNumber{}, translate("update inbound number", err)
	}
	return inboundNumberFromRow(row), nil
}

// Delete removes an inbound number (its keyword mappings cascade), or reports ErrNotFound when
// nothing matched.
func (r *InboundNumberRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteInboundNumber(ctx, id)
	if err != nil {
		return translate("delete inbound number", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

// Assign dedicates the number to accountID, or clears the dedication (shared, keyword-resolved) when
// accountID is nil. It returns the updated number, or ErrNotFound.
func (r *InboundNumberRepo) Assign(ctx context.Context, id uuid.UUID, accountID *uuid.UUID) (cp.InboundNumber, error) {
	row, err := r.q.AssignInboundNumber(ctx, sqlcgen.AssignInboundNumberParams{
		ID:        id,
		AccountID: accountID,
	})
	if err != nil {
		return cp.InboundNumber{}, translate("assign inbound number", err)
	}
	return inboundNumberFromRow(row), nil
}

func inboundNumberFromRow(row sqlcgen.ControlPlaneInboundNumber) cp.InboundNumber {
	return cp.InboundNumber{
		ID:          row.ID,
		Address:     row.Address,
		NumberType:  cp.NumberType(row.NumberType),
		CountryCode: row.CountryCode,
		MCCMNC:      row.Mccmnc,
		ConnectorID: row.ConnectorID,
		AccountID:   row.AccountID,
		Status:      cp.InboundNumberStatus(row.Status),
		CreatedAt:   tsVal(row.CreatedAt),
		UpdatedAt:   tsVal(row.UpdatedAt),
	}
}
