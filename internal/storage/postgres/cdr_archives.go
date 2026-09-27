package postgres

import (
	"context"
	"fmt"
	"math"
	"time"

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

// Record catalogues object as the archive of day unless the day already has one, and returns the row count
// of the line that now holds the day.
func (r *CDRArchiveRepo) Record(ctx context.Context, day time.Time, object string, rows uint64) (uint64, error) {
	if rows > math.MaxInt64 {
		return 0, fmt.Errorf("postgres: cdr archive of %s: %d rows overflow bigint", day.Format(time.DateOnly), rows)
	}
	catalogued, err := r.q.RecordCDRArchive(ctx, sqlcgen.RecordCDRArchiveParams{
		Day:      pgtype.Date{Time: day, Valid: true},
		Object:   object,
		RowCount: int64(rows),
	})
	if err != nil {
		return 0, fmt.Errorf("postgres: record cdr archive of %s: %w", day.Format(time.DateOnly), err)
	}
	return uint64(catalogued), nil //nolint:gosec // CHECK (row_count >= 0)
}
