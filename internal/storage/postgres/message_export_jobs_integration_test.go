package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestMessageExportJobLifecycle: the job is queued durably before a single row is read, moves to
// running, and ends carrying where the artefact landed and how many rows it holds.
func TestMessageExportJobLifecycle(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewMessageExportJobRepo(pool)

	expires := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	job, err := repo.Create(ctx, cp.NewMessageExportJob{
		Format:    cp.ExportFormatCSV,
		Masked:    true,
		Filters:   []byte(`{"from_date":"2026-07-01T00:00:00Z"}`),
		Operator:  "op-token",
		ExpiresAt: expires,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if job.Status != cp.ExportJobQueued || job.ArtefactURI != nil || job.RowCount != nil {
		t.Fatalf("new job = %+v, want queued with no artefact", job)
	}
	if !job.Masked {
		t.Error("masked must persist: it is the durable proof of which artefact exists")
	}
	if !job.ExpiresAt.Equal(expires) {
		t.Errorf("ExpiresAt = %s, want %s", job.ExpiresAt, expires)
	}

	if err := repo.MarkRunning(ctx, job.ID); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	running, err := repo.Get(ctx, job.ID)
	if err != nil || running.Status != cp.ExportJobRunning {
		t.Fatalf("after MarkRunning = (%+v, %v), want running", running, err)
	}

	if err := repo.Complete(ctx, job.ID, "file:///exports/x.csv", 42); err != nil {
		t.Fatalf("complete: %v", err)
	}
	done, err := repo.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if done.Status != cp.ExportJobCompleted {
		t.Errorf("status = %q, want completed", done.Status)
	}
	if done.ArtefactURI == nil || *done.ArtefactURI != "file:///exports/x.csv" {
		t.Errorf("artefact = %v, want the file uri", done.ArtefactURI)
	}
	if done.RowCount == nil || *done.RowCount != 42 {
		t.Errorf("row_count = %v, want 42", done.RowCount)
	}
	if done.FinishedAt == nil {
		t.Error("finished_at must be set when the job closes")
	}
}

// TestMessageExportJobFailureCarriesItsReason: a failed status with no reason is not actionable — the
// operator has to know whether to narrow the window or call an engineer.
func TestMessageExportJobFailureCarriesItsReason(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewMessageExportJobRepo(pool)

	job, err := repo.Create(ctx, cp.NewMessageExportJob{
		Format: cp.ExportFormatJSONL, Masked: false, Filters: []byte(`{}`),
		Operator: "op-token", ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	const reason = "more than 100000 rows match; narrow the window"
	if err := repo.Fail(ctx, job.ID, reason); err != nil {
		t.Fatalf("fail: %v", err)
	}
	failed, err := repo.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if failed.Status != cp.ExportJobFailed {
		t.Errorf("status = %q, want failed", failed.Status)
	}
	if failed.Error == nil || *failed.Error != reason {
		t.Errorf("error = %v, want %q", failed.Error, reason)
	}
	if failed.RowCount != nil {
		t.Errorf("row_count = %v, want none: a failed export produced no file", failed.RowCount)
	}
}

func TestGetMessageExportJobUnknownIsNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewMessageExportJobRepo(pool)

	_, err := repo.Get(context.Background(), uuid.New())
	if !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
