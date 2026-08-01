package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestGDPREraseJobLifecycle: a job is queued durably before any erasure runs, moves to running, and ends
// with its attestation — the proof of execution the operator reads back.
func TestGDPREraseJobLifecycle(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewGDPREraseJobRepo(pool)

	job, err := repo.Create(ctx, cp.NewGDPREraseJob{
		SubjectType: cp.GDPRSubjectMSISDN, SubjectID: "22507000000", Operator: "op-token",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if job.Status != cp.GDPRJobQueued || job.Attestation != nil || job.FinishedAt != nil {
		t.Fatalf("new job = %+v, want queued with no attestation", job)
	}

	if err := repo.MarkRunning(ctx, job.ID); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	running, err := repo.Get(ctx, job.ID)
	if err != nil || running.Status != cp.GDPRJobRunning {
		t.Fatalf("after MarkRunning = (%+v, %v), want running", running, err)
	}

	const attestation = "subject=msisdn:22507000000 cdr_rows_erased=7 opt_out_preserved=true"
	if err := repo.Finish(ctx, job.ID, cp.GDPRJobCompleted, attestation); err != nil {
		t.Fatalf("finish: %v", err)
	}
	done, err := repo.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("get finished: %v", err)
	}
	if done.Status != cp.GDPRJobCompleted || done.Attestation == nil || *done.Attestation != attestation {
		t.Errorf("finished job = %+v, want completed with the attestation", done)
	}
	if done.FinishedAt == nil {
		t.Error("finished_at must be set")
	}
	if done.Operator != "op-token" {
		t.Errorf("operator = %q, want the requester recorded", done.Operator)
	}
}

// TestGDPREraseJobUnknownIsNotFound: an unknown job id is a clean not-found.
func TestGDPREraseJobUnknownIsNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewGDPREraseJobRepo(pool)
	if _, err := repo.Get(context.Background(), uuid.New()); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("Get(unknown) = %v, want not_found", err)
	}
}

// TestGDPREraseJobRejectsBadSubject: the subject type is constrained by the schema.
func TestGDPREraseJobRejectsBadSubject(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewGDPREraseJobRepo(pool)
	_, err := repo.Create(context.Background(), cp.NewGDPREraseJob{
		SubjectType: "everything", SubjectID: "x", Operator: "op",
	})
	if err == nil {
		t.Error("creating a job with an unknown subject type should be rejected by the CHECK")
	}
}
