package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// CDRArchiveRepo is the catalogue of archived CDR partitions (control_plane.cdr_archives, step-407).
type CDRArchiveRepo struct{ q *sqlcgen.Queries }

// NewCDRArchiveRepo returns the catalogue backed by pool.
func NewCDRArchiveRepo(pool *pgxpool.Pool) *CDRArchiveRepo {
	return &CDRArchiveRepo{q: sqlcgen.New(pool)}
}

// Record catalogues object as the archive of day unless the day already has one, and returns the object of
// the line that now holds the day.
func (r *CDRArchiveRepo) Record(ctx context.Context, day time.Time, object string, rows uint64) (string, error) {
	params := sqlcgen.RecordCDRArchiveParams{
		Day:      pgtype.Date{Time: day, Valid: true},
		Object:   object,
		RowCount: int64(rows), //nolint:gosec // a day of CDRs is far below 2^63 rows
	}
	catalogued, err := r.q.RecordCDRArchive(ctx, params)
	if errors.Is(err, pgx.ErrNoRows) {
		// A concurrent insert of the same day committed while ours waited on it: our statement's snapshot
		// predates that line, so a second statement is what can read it.
		catalogued, err = r.q.RecordCDRArchive(ctx, params)
	}
	if err != nil {
		return "", fmt.Errorf("postgres: record cdr archive of %s: %w", day.Format(time.DateOnly), err)
	}
	return catalogued, nil
}
