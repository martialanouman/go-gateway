package adminapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// GDPRJobStore is the durable record of erasure jobs. *postgres.GDPREraseJobRepo satisfies it.
type GDPRJobStore interface {
	Create(ctx context.Context, in cp.NewGDPREraseJob) (cp.GDPREraseJob, error)
	Get(ctx context.Context, id uuid.UUID) (cp.GDPREraseJob, error)
	MarkRunning(ctx context.Context, id uuid.UUID) error
	Finish(ctx context.Context, id uuid.UUID, status cp.GDPRJobStatus, attestation string) error
}

// UnroutedMOEraser removes the unrouted-MO records carrying a phone number. *postgres.UnroutedMORepo
// satisfies it; declared consumer-side.
type UnroutedMOEraser interface {
	DeleteByMSISDN(ctx context.Context, msisdn string) (int, error)
}

// CDREraser removes CDR rows for an erasure subject. *clickhouse.CDREraser satisfies it; declared
// consumer-side. It never touches the opt-out list, which must survive an erasure.
type CDREraser interface {
	EraseCustomer(ctx context.Context, customerID uuid.UUID) (uint64, error)
	EraseMSISDN(ctx context.Context, msisdn string) (uint64, error)
}

type gdprHandlers struct {
	jobs     GDPRJobStore
	cdr      CDREraser
	unrouted UnroutedMOEraser
	keys     ContentKeyEraser
	runner   ImportRunner
	logger   *slog.Logger
}

// registerGDPR wires the on-demand RGPD erasure (§6.23, step-166): a request queues a durable job, the
// erasure runs off the request path, and the job carries the attestation once it is done.
func registerGDPR(api huma.API, jobs GDPRJobStore, cdr CDREraser, unrouted UnroutedMOEraser, keys ContentKeyEraser, runner ImportRunner, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	h := &gdprHandlers{jobs: jobs, cdr: cdr, unrouted: unrouted, keys: keys, runner: runner, logger: logger}
	register(api, huma.Operation{
		OperationID: "gdpr-erase", Method: http.MethodPost, Path: "/admin/gdpr/erase",
		DefaultStatus: http.StatusAccepted, Summary: "On-demand RGPD erasure (scope gdpr:erase, async, irreversible)",
		Tags:     []string{"Content & RGPD"},
		Security: scopeSecurity(auth.ScopeGDPRErase),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusServiceUnavailable},
	}, h.erase)
	register(api, huma.Operation{
		OperationID: "get-gdpr-erase-job", Method: http.MethodGet, Path: "/admin/gdpr/erase/{jobId}",
		Summary: "RGPD erase job status + attestation", Tags: []string{"Content & RGPD"},
		Security: scopeSecurity(auth.ScopeGDPRErase),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.getJob)
}

// gdprEraseJobDTO conforms to api/openapi-admin.yaml GdprEraseJob.
type gdprEraseJobDTO struct {
	JobID       string     `json:"job_id" format:"uuid"`
	SubjectType string     `json:"subject_type" enum:"customer,msisdn"`
	SubjectID   string     `json:"subject_id"`
	Status      string     `json:"status" enum:"queued,running,completed,failed"`
	Attestation *string    `json:"attestation,omitempty" nullable:"true"`
	CreatedAt   time.Time  `json:"created_at" format:"date-time"`
	FinishedAt  *time.Time `json:"finished_at,omitempty" nullable:"true" format:"date-time"`
}

type gdprEraseBody struct {
	SubjectType string `json:"subject_type" enum:"customer,msisdn"`
	SubjectID   string `json:"id"`
}

type gdprEraseInput struct{ Body gdprEraseBody }
type gdprEraseOutput struct{ Body gdprEraseJobDTO }

type gdprJobInput struct {
	JobID string `path:"jobId" format:"uuid"`
}

// erase validates and normalises the subject, records the job durably, then runs the erasure off the request
// path. The 202 carries the queued job; the operator polls get-gdpr-erase-job for the attestation.
func (h *gdprHandlers) erase(ctx context.Context, in *gdprEraseInput) (*gdprEraseOutput, error) {
	subject := cp.GDPRSubjectType(in.Body.SubjectType)
	if !subject.Valid() {
		return nil, humaerr.FailValidation("invalid subject_type",
			humaerr.FieldError{Field: "subject_type", Message: "must be customer or msisdn"})
	}
	subjectID, err := normaliseSubject(subject, in.Body.SubjectID)
	if err != nil {
		return nil, err
	}

	job, err := h.jobs.Create(ctx, cp.NewGDPREraseJob{
		SubjectType: subject, SubjectID: subjectID, Operator: operatorSubject(ctx),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	// The job row is already durable, so a runner that refuses the work leaves a visible queued job rather
	// than a silently dropped erasure request.
	if rerr := h.runner.Go("gdpr-erase", func(jctx context.Context) error {
		h.run(jctx, job)
		return nil
	}); rerr != nil {
		// Close the job instead of leaving a queued row nothing will ever pick up: an erasure request stuck
		// "queued" forever reads, to an auditor, as an unhonoured legal obligation.
		if ferr := h.jobs.Finish(ctx, job.ID, cp.GDPRJobFailed, "erasure not started: no capacity, resubmit"); ferr != nil {
			h.logger.ErrorContext(ctx, "gdpr erasure: could not close the unstarted job", "job_id", job.ID, "err", ferr)
		}
		h.logger.ErrorContext(ctx, "could not start gdpr erasure", "job_id", job.ID, "err", rerr)
		return nil, humaerr.FromError(fmt.Errorf("start erasure: %w", errs.ErrServiceUnavailable))
	}
	return &gdprEraseOutput{Body: toGDPRJobDTO(job)}, nil
}

// normaliseSubject turns the request's id into the form the erasure works on: a parsed customer UUID, or an
// MSISDN normalised to the same canonical digits the CDR stores (so the erasure matches the rows).
func normaliseSubject(subject cp.GDPRSubjectType, raw string) (string, error) {
	if subject == cp.GDPRSubjectCustomer {
		id, err := uuid.Parse(raw)
		if err != nil {
			return "", humaerr.FailValidation("invalid id",
				humaerr.FieldError{Field: "id", Message: "must be a customer UUID"})
		}
		return id.String(), nil
	}
	normalised, err := e164.Normalize(raw)
	if err != nil {
		return "", humaerr.FailValidation("invalid id",
			humaerr.FieldError{Field: "id", Message: "must be an E.164 phone number"})
	}
	return normalised, nil
}

// jobBookkeepingTimeout bounds the status writes that must happen even when the job's own context is gone.
const jobBookkeepingTimeout = 10 * time.Second

// run performs the erasure and closes the job with its attestation. It never returns an error to the runner:
// a failure is recorded ON THE JOB, which is what the operator reads, not in a lost background error.
//
// The bookkeeping writes run on a DETACHED context. A ClickHouse erasure can outlive a shutdown deadline,
// and cancelling the client call does not cancel the server-side mutation — so writing the outcome with the
// cancelled context would leave the job stuck in "running", with no attestation, for an irreversible
// operation that did happen. The record of what was done must survive the process that did it.
func (h *gdprHandlers) run(ctx context.Context, job cp.GDPREraseJob) {
	book, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobBookkeepingTimeout)
	defer cancel()

	if err := h.jobs.MarkRunning(book, job.ID); err != nil {
		h.logger.ErrorContext(book, "gdpr erasure: mark running failed", "job_id", job.ID, "err", err)
	}
	attestation, err := h.performErasure(ctx, job)
	status := cp.GDPRJobCompleted
	if err != nil {
		// The error text is a fault description, never erased content.
		status = cp.GDPRJobFailed
		attestation = fmt.Sprintf("erasure failed: %v", err)
		h.logger.ErrorContext(book, "gdpr erasure failed", "job_id", job.ID,
			"subject_type", job.SubjectType, "err", err)
	}
	if ferr := h.jobs.Finish(book, job.ID, status, attestation); ferr != nil {
		// Last resort: the attestation must exist somewhere, so it goes to the log rather than being lost.
		h.logger.ErrorContext(book, "gdpr erasure: recording the outcome failed; attestation follows",
			"job_id", job.ID, "status", status, "attestation", attestation, "err", ferr)
	}
}

// performErasure erases the subject and returns the attestation — the proof of execution: what was erased,
// how much, and when. It states counters only, never any erased content (invariant a).
func (h *gdprHandlers) performErasure(ctx context.Context, job cp.GDPREraseJob) (string, error) {
	done := time.Now().UTC().Format(time.RFC3339)
	if job.SubjectType == cp.GDPRSubjectCustomer {
		customerID, err := uuid.Parse(job.SubjectID)
		if err != nil {
			return "", fmt.Errorf("subject is not a customer id: %w", err)
		}
		// Content first: crypto-shred makes every body already written unreadable even if the metadata
		// erasure that follows is interrupted.
		keys, err := h.keys.Erase(ctx, customerID)
		if err != nil {
			return "", fmt.Errorf("crypto-shred: %w", err)
		}
		rows, err := h.cdr.EraseCustomer(ctx, customerID)
		if err != nil {
			return "", fmt.Errorf("erase cdr rows: %w", err)
		}
		return fmt.Sprintf("subject=customer:%s %s content_keys_destroyed=%d cdr_rows_erased=%d completed_at=%s",
			customerID, attestationScope, keys, rows, done), nil
	}

	rows, err := h.cdr.EraseMSISDN(ctx, job.SubjectID)
	if err != nil {
		return "", fmt.Errorf("erase cdr rows: %w", err)
	}
	// Unrouted MO records carry the sender's number and have no retention of their own, so an erasure that
	// skipped them would leave the subject's personal data behind.
	unrouted, err := h.unrouted.DeleteByMSISDN(ctx, job.SubjectID)
	if err != nil {
		return "", fmt.Errorf("erase unrouted mo: %w", err)
	}
	// The opt-out is deliberately NOT erased: the duty not to contact the person again outlives the erasure
	// of what was sent to them (§14). The attestation says so explicitly, because an auditor reading it must
	// be able to tell that the remaining suppression entry is intentional, not a missed record.
	return fmt.Sprintf("subject=msisdn:%s %s cdr_rows_erased=%d unrouted_mo_rows_erased=%d opt_out_preserved=true completed_at=%s",
		job.SubjectID, attestationScope, rows, unrouted, done), nil
}

// attestationScope states what an attestation covers — and, by omission, what it does not. An attestation is
// a legal document, so it must not read as "everything, everywhere": the cold archives written by tiering and
// the durable Kafka log are outside this erasure and are bounded by their own retention, which is the
// operator's responsibility.
const attestationScope = "scope=cdr+content_keys+unrouted_mo(excludes:cold_archives,kafka_log)"

// getJob returns a job with its attestation.
func (h *gdprHandlers) getJob(ctx context.Context, in *gdprJobInput) (*gdprEraseOutput, error) {
	id, err := uuid.Parse(in.JobID)
	if err != nil {
		return nil, notFound("erase job")
	}
	job, err := h.jobs.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &gdprEraseOutput{Body: toGDPRJobDTO(job)}, nil
}

func toGDPRJobDTO(j cp.GDPREraseJob) gdprEraseJobDTO {
	return gdprEraseJobDTO{
		JobID: j.ID.String(), SubjectType: string(j.SubjectType), SubjectID: j.SubjectID,
		Status: string(j.Status), Attestation: j.Attestation, CreatedAt: j.CreatedAt, FinishedAt: j.FinishedAt,
	}
}
