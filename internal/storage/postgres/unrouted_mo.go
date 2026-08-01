package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// UnroutedMORepo is the unrouted-MO repository (the operator queue for MO that resolved to no
// account). It satisfies the return-path router's writer and the Admin list-unrouted-mo reader
// structurally.
type UnroutedMORepo struct {
	q *sqlcgen.Queries
}

// NewUnroutedMORepo returns the unrouted-MO repository backed by pool.
func NewUnroutedMORepo(pool *pgxpool.Pool) *UnroutedMORepo {
	return &UnroutedMORepo{q: sqlcgen.New(pool)}
}

// Create records an unrouted MO. It never stores the body (the input carries none).
func (r *UnroutedMORepo) Create(ctx context.Context, in cp.NewUnroutedMO) (cp.UnroutedMO, error) {
	row, err := r.q.CreateUnroutedMO(ctx, sqlcgen.CreateUnroutedMOParams{
		ReceivedAt:      tsFrom(in.ReceivedAt),
		ConnectorID:     in.ConnectorID,
		InboundNumberID: in.InboundNumberID,
		SourceAddr:      in.SourceAddr,
		DestAddr:        in.DestAddr,
		SegmentCount:    int32(in.SegmentCount), //nolint:gosec // segment count is a small positive integer
		Encoding:        in.Encoding,
		Reason:          string(in.Reason),
	})
	if err != nil {
		return cp.UnroutedMO{}, translate("create unrouted mo", err)
	}
	return unroutedMOFromRow(row), nil
}

// List returns up to limit unrouted MO newest-first, starting strictly after the keyset position
// (nil = the first page). The caller (Admin) requests limit+1 to detect a further page and builds the
// next cursor from the last item's (ReceivedAt, ID).
func (r *UnroutedMORepo) List(ctx context.Context, limit int, after *cp.UnroutedMOKey) ([]cp.UnroutedMO, error) {
	var (
		rows []sqlcgen.ControlPlaneUnroutedMo
		err  error
	)
	if after == nil {
		rows, err = r.q.ListUnroutedMOFirst(ctx, int32(limit)) //nolint:gosec // limit is a small bounded page size
	} else {
		rows, err = r.q.ListUnroutedMOAfter(ctx, sqlcgen.ListUnroutedMOAfterParams{
			AfterReceivedAt: tsFrom(after.ReceivedAt),
			AfterID:         after.ID,
			Lim:             int32(limit), //nolint:gosec // limit is a small bounded page size
		})
	}
	if err != nil {
		return nil, translate("list unrouted mo", err)
	}
	out := make([]cp.UnroutedMO, 0, len(rows))
	for _, row := range rows {
		out = append(out, unroutedMOFromRow(row))
	}
	return out, nil
}

func unroutedMOFromRow(row sqlcgen.ControlPlaneUnroutedMo) cp.UnroutedMO {
	return cp.UnroutedMO{
		ID:              row.ID,
		ReceivedAt:      tsVal(row.ReceivedAt),
		ConnectorID:     row.ConnectorID,
		InboundNumberID: row.InboundNumberID,
		SourceAddr:      row.SourceAddr,
		DestAddr:        row.DestAddr,
		SegmentCount:    int(row.SegmentCount),
		Encoding:        row.Encoding,
		Reason:          cp.UnroutedReason(row.Reason),
	}
}

// DeleteByMSISDN removes the unrouted-MO records carrying a phone number, for an RGPD erasure (step-166).
// These rows hold the sender's number with no retention of their own, so an erasure that skipped them would
// leave the subject's personal data behind. It returns how many rows were removed.
func (r *UnroutedMORepo) DeleteByMSISDN(ctx context.Context, msisdn string) (int, error) {
	n, err := r.q.DeleteUnroutedMOByMSISDN(ctx, msisdn)
	if err != nil {
		return 0, translate("delete unrouted mo by msisdn", err)
	}
	return int(n), nil
}
