package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// GDPREraseJobRepo is the durable record of on-demand RGPD erasures (control_plane.gdpr_erase_jobs, §6.23).
// The job row is written BEFORE any erasure runs, so a request is never lost even if the worker dies mid-way:
// the job is then visibly stuck in running rather than silently forgotten.
type GDPREraseJobRepo struct {
	q *sqlcgen.Queries
}

// NewGDPREraseJobRepo returns the erasure-job repository backed by pool.
func NewGDPREraseJobRepo(pool *pgxpool.Pool) *GDPREraseJobRepo {
	return &GDPREraseJobRepo{q: sqlcgen.New(pool)}
}

// Create queues an erasure job.
func (r *GDPREraseJobRepo) Create(ctx context.Context, in cp.NewGDPREraseJob) (cp.GDPREraseJob, error) {
	row, err := r.q.CreateGDPREraseJob(ctx, sqlcgen.CreateGDPREraseJobParams{
		SubjectType: string(in.SubjectType), SubjectID: in.SubjectID, Operator: in.Operator,
	})
	if err != nil {
		return cp.GDPREraseJob{}, translate("create gdpr erase job", err)
	}
	return gdprJobFromRow(row), nil
}

// Get reads a job by id (ErrNotFound when unknown).
func (r *GDPREraseJobRepo) Get(ctx context.Context, id uuid.UUID) (cp.GDPREraseJob, error) {
	row, err := r.q.GetGDPREraseJob(ctx, id)
	if err != nil {
		return cp.GDPREraseJob{}, translate("get gdpr erase job", err)
	}
	return gdprJobFromRow(row), nil
}

// MarkRunning moves a queued job to running.
func (r *GDPREraseJobRepo) MarkRunning(ctx context.Context, id uuid.UUID) error {
	if err := r.q.MarkGDPREraseJobRunning(ctx, id); err != nil {
		return translate("mark gdpr erase job running", err)
	}
	return nil
}

// Finish closes a job with its final status and attestation.
func (r *GDPREraseJobRepo) Finish(ctx context.Context, id uuid.UUID, status cp.GDPRJobStatus, attestation string) error {
	att := &attestation
	if attestation == "" {
		att = nil
	}
	if err := r.q.FinishGDPREraseJob(ctx, sqlcgen.FinishGDPREraseJobParams{
		ID: id, Status: string(status), Attestation: att,
	}); err != nil {
		return translate("finish gdpr erase job", err)
	}
	return nil
}

func gdprJobFromRow(row sqlcgen.ControlPlaneGdprEraseJob) cp.GDPREraseJob {
	return cp.GDPREraseJob{
		ID:          row.ID,
		SubjectType: cp.GDPRSubjectType(row.SubjectType),
		SubjectID:   row.SubjectID,
		Status:      cp.GDPRJobStatus(row.Status),
		Attestation: row.Attestation,
		Operator:    row.Operator,
		CreatedAt:   tsVal(row.CreatedAt),
		FinishedAt:  tsPtr(row.FinishedAt),
	}
}
