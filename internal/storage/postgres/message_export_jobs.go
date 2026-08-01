package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// MessageExportJobRepo is the durable record of asynchronous CDR exports
// (control_plane.message_export_jobs, §15). The job row is written BEFORE any row is read, so a
// request survives a worker that dies mid-way: the job is then visibly stuck in running rather than
// silently forgotten.
type MessageExportJobRepo struct {
	q *sqlcgen.Queries
}

// NewMessageExportJobRepo returns the export-job repository backed by pool.
func NewMessageExportJobRepo(pool *pgxpool.Pool) *MessageExportJobRepo {
	return &MessageExportJobRepo{q: sqlcgen.New(pool)}
}

// Create queues an export job.
func (r *MessageExportJobRepo) Create(ctx context.Context, in cp.NewMessageExportJob) (cp.MessageExportJob, error) {
	row, err := r.q.CreateMessageExportJob(ctx, sqlcgen.CreateMessageExportJobParams{
		Format:    string(in.Format),
		Masked:    in.Masked,
		Filters:   in.Filters,
		Operator:  in.Operator,
		ExpiresAt: tsFrom(in.ExpiresAt),
	})
	if err != nil {
		return cp.MessageExportJob{}, translate("create message export job", err)
	}
	return exportJobFromRow(row), nil
}

// Get reads a job by id (ErrNotFound when unknown).
func (r *MessageExportJobRepo) Get(ctx context.Context, id uuid.UUID) (cp.MessageExportJob, error) {
	row, err := r.q.GetMessageExportJob(ctx, id)
	if err != nil {
		return cp.MessageExportJob{}, translate("get message export job", err)
	}
	return exportJobFromRow(row), nil
}

// MarkRunning moves a queued job to running. A job already past queued is left alone.
func (r *MessageExportJobRepo) MarkRunning(ctx context.Context, id uuid.UUID) error {
	if err := r.q.MarkMessageExportJobRunning(ctx, id); err != nil {
		return translate("mark message export job running", err)
	}
	return nil
}

// Complete closes a job that produced an artefact.
func (r *MessageExportJobRepo) Complete(ctx context.Context, id uuid.UUID, artefactURI string, rowCount int) error {
	count := int32(rowCount) //nolint:gosec // bounded by the export row cap, far below int32
	if err := r.q.CompleteMessageExportJob(ctx, sqlcgen.CompleteMessageExportJobParams{
		ID: id, ArtefactUri: &artefactURI, RowCount: &count,
	}); err != nil {
		return translate("complete message export job", err)
	}
	return nil
}

// Fail closes a job that produced nothing, with the reason an operator reads.
func (r *MessageExportJobRepo) Fail(ctx context.Context, id uuid.UUID, reason string) error {
	if err := r.q.FailMessageExportJob(ctx, sqlcgen.FailMessageExportJobParams{ID: id, Error: &reason}); err != nil {
		return translate("fail message export job", err)
	}
	return nil
}

func exportJobFromRow(row sqlcgen.ControlPlaneMessageExportJob) cp.MessageExportJob {
	job := cp.MessageExportJob{
		ID:          row.ID,
		Status:      cp.ExportJobStatus(row.Status),
		Format:      cp.ExportFormat(row.Format),
		Masked:      row.Masked,
		Filters:     row.Filters,
		ArtefactURI: row.ArtefactUri,
		Error:       row.Error,
		Operator:    row.Operator,
		CreatedAt:   tsVal(row.CreatedAt),
		ExpiresAt:   tsVal(row.ExpiresAt),
		FinishedAt:  tsPtr(row.FinishedAt),
	}
	if row.RowCount != nil {
		count := int(*row.RowCount)
		job.RowCount = &count
	}
	return job
}
