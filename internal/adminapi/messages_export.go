package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// ExportJobStore is the durable record of export jobs. Declared consumer-side.
type ExportJobStore interface {
	Create(ctx context.Context, in cp.NewMessageExportJob) (cp.MessageExportJob, error)
	Get(ctx context.Context, id uuid.UUID) (cp.MessageExportJob, error)
	MarkRunning(ctx context.Context, id uuid.UUID) error
	Complete(ctx context.Context, id uuid.UUID, artefactURI string, rowCount int) error
	Fail(ctx context.Context, id uuid.UUID, reason string) error
}

// ExportMaxRows caps one export. Beyond it the job FAILS rather than truncating: a truncated export
// is indistinguishable from an exhaustive one once it is on someone's disk, and that is how a wrong
// conclusion gets drawn from a right-looking file.
const ExportMaxRows = 100_000

// exportPageSize is how many rows the worker holds at once. The export streams: a hundred thousand
// CDR rows in memory would be a self-inflicted outage on a control-plane pod.
const exportPageSize = 5_000

// exportTTL is how long an artefact is announced as available. NOTHING deletes it yet — the artefact
// lifecycle belongs to the storage tier, like the cold archives (§6.14) — so this is a promise about
// availability, not a guarantee of destruction.
const exportTTL = 24 * time.Hour

type messageExportFiltersDTO struct {
	TraceID    string    `json:"traceId,omitempty" format:"uuid"`
	AccountID  string    `json:"accountId,omitempty" format:"uuid"`
	CustomerID string    `json:"customerId,omitempty" format:"uuid"`
	GroupID    string    `json:"groupId,omitempty" format:"uuid"`
	Status     string    `json:"status,omitempty" enum:"accepted,enroute,delivered,failed,expired,rejected,rerouted,cancelled"`
	Direction  string    `json:"direction,omitempty" enum:"mt,mo"`
	MSISDN     string    `json:"msisdn,omitempty"`
	FromDate   time.Time `json:"from_date" format:"date-time"`
	ToDate     time.Time `json:"to_date" format:"date-time"`
}

type messageExportRequestDTO struct {
	Filters messageExportFiltersDTO `json:"filters"`
	Format  string                  `json:"format,omitempty" enum:"csv,jsonl" default:"csv"`
	// MaskMSISDN is a POINTER so "absent" and "false" are distinguishable: absent means the default
	// (masked), false is an explicit request for subscriber numbers in clear, which needs a scope.
	MaskMSISDN *bool `json:"mask_msisdn,omitempty"`
}

type exportJobDTO struct {
	JobID       string     `json:"job_id" format:"uuid"`
	Status      string     `json:"status" enum:"queued,running,completed,failed"`
	RowCount    *int       `json:"row_count,omitempty" nullable:"true" required:"false"`
	DownloadURL *string    `json:"download_url,omitempty" format:"uri" nullable:"true" required:"false"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty" format:"date-time" nullable:"true" required:"false"`
	Error       *string    `json:"error,omitempty" nullable:"true" required:"false"`
	CreatedAt   time.Time  `json:"created_at" format:"date-time"`
}

type createExportInput struct{ Body messageExportRequestDTO }
type exportJobOutput struct{ Body exportJobDTO }

type getExportInput struct {
	JobID string `path:"jobId" format:"uuid"`
}

func registerMessageExport(api huma.API, jobs ExportJobStore, search SearchStore, sink ExportSink,
	customers CustomerStore, runner ImportRunner, logger *slog.Logger,
) {
	h := &exportHandlers{jobs: jobs, search: search, sink: sink, customers: customers, runner: runner, logger: logger}
	register(api, huma.Operation{
		OperationID:   "create-message-export",
		Method:        http.MethodPost,
		Path:          "/admin/messages/export",
		DefaultStatus: http.StatusAccepted,
		Summary:       "Create an async CDR export job (row-capped, role-based MSISDN mask)",
		Tags:          []string{"Messages"},
		Security:      scopeSecurity(auth.ScopeCDRExportBulk),
		Errors: []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity,
			http.StatusServiceUnavailable},
	}, h.create)
	register(api, huma.Operation{
		OperationID: "get-message-export",
		Method:      http.MethodGet,
		Path:        "/admin/messages/export/{jobId}",
		Summary:     "Export job status + download URL when ready",
		Tags:        []string{"Messages"},
		Security:    scopeSecurity(auth.ScopeCDRExportBulk),
		Errors:      []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.get)
}

type exportHandlers struct {
	jobs      ExportJobStore
	search    SearchStore
	sink      ExportSink
	customers CustomerStore
	runner    ImportRunner
	logger    *slog.Logger
}

// create queues an export and returns immediately.
//
// Everything that can be judged from the request alone is judged HERE, synchronously: the storage
// tier, the mask permission, the window, the filters. A job only reaches the queue once it is known
// to be runnable, so a 202 means "this will produce a file", not "we will find out later".
func (h *exportHandlers) create(ctx context.Context, in *createExportInput) (*exportJobOutput, error) {
	if h.sink == nil || h.jobs == nil {
		// The capacity is absent from this deployment; that is not the caller's mistake.
		return nil, humaerr.FromError(fmt.Errorf("export storage is not configured: %w", errs.ErrServiceUnavailable))
	}

	format := cp.ExportFormat(in.Body.Format)
	if in.Body.Format == "" {
		format = cp.ExportFormatCSV
	}
	if !format.Valid() {
		return nil, humaerr.FailValidation("invalid format",
			humaerr.FieldError{Field: "format", Message: "must be csv or jsonl"})
	}

	masked := in.Body.MaskMSISDN == nil || *in.Body.MaskMSISDN
	if !masked && !mayRevealMSISDN(ctx) {
		// Refused, never silently masked: an operator who asked for numbers in clear and received a
		// masked file would read "no match" into every masked row.
		return nil, humaerr.FromError(fmt.Errorf("unmasked export requires msisdn:reveal: %w", errs.ErrForbiddenScope))
	}

	filter, empty, err := buildCDRSearchFilter(ctx, searchPredicates{
		TraceID:    in.Body.Filters.TraceID,
		AccountID:  in.Body.Filters.AccountID,
		CustomerID: in.Body.Filters.CustomerID,
		GroupID:    in.Body.Filters.GroupID,
		Status:     in.Body.Filters.Status,
		Direction:  in.Body.Filters.Direction,
		MSISDN:     in.Body.Filters.MSISDN,
		FromDate:   in.Body.Filters.FromDate,
		ToDate:     in.Body.Filters.ToDate,
	}, h.customers)
	if err != nil {
		return nil, err
	}

	filters, err := json.Marshal(in.Body.Filters)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	job, err := h.jobs.Create(ctx, cp.NewMessageExportJob{
		Format:    format,
		Masked:    masked,
		Filters:   filters,
		Operator:  operatorSubject(ctx),
		ExpiresAt: time.Now().UTC().Add(exportTTL),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	if rerr := h.runner.Go("message-export", func(jctx context.Context) error {
		h.run(jctx, job, filter, empty)
		return nil
	}); rerr != nil {
		// Close the job rather than leave a queued row nothing will pick up: to an operator polling it,
		// "queued" three hours later is indistinguishable from an export still running.
		if ferr := h.jobs.Fail(ctx, job.ID, "export not started: no capacity, resubmit"); ferr != nil {
			h.logger.ErrorContext(ctx, "export: could not close the unstarted job", "job_id", job.ID, "err", ferr)
		}
		h.logger.ErrorContext(ctx, "could not start message export", "job_id", job.ID, "err", rerr)
		return nil, humaerr.FromError(fmt.Errorf("start export: %w", errs.ErrServiceUnavailable))
	}
	return &exportJobOutput{Body: toExportJobDTO(job)}, nil
}

// run produces the artefact and closes the job with its outcome. It never returns an error to the
// runner: a failure is recorded ON THE JOB, which is what the operator reads, not in a lost
// background error.
//
// The bookkeeping writes run on a DETACHED context, like the GDPR erasure's: the artefact may well be
// written when the shutdown deadline fires, and a job stuck in "running" over a file that exists is
// the one outcome an operator cannot act on.
func (h *exportHandlers) run(ctx context.Context, job cp.MessageExportJob, filter clickhouse.CDRSearchFilter, empty bool) {
	book, cancel := context.WithTimeout(context.WithoutCancel(ctx), jobBookkeepingTimeout)
	defer cancel()

	if err := h.jobs.MarkRunning(book, job.ID); err != nil {
		h.logger.ErrorContext(book, "export: mark running failed", "job_id", job.ID, "err", err)
	}

	uri, rows, err := h.produce(ctx, job, filter, empty)
	if err != nil {
		// The message is a fault description or a row-cap refusal — never an exported row.
		if ferr := h.jobs.Fail(book, job.ID, err.Error()); ferr != nil {
			h.logger.ErrorContext(book, "export: recording the failure failed", "job_id", job.ID, "err", ferr)
		}
		h.logger.ErrorContext(book, "message export failed", "job_id", job.ID, "err", err)
		return
	}
	if cerr := h.jobs.Complete(book, job.ID, uri, rows); cerr != nil {
		h.logger.ErrorContext(book, "export: recording the outcome failed; artefact follows",
			"job_id", job.ID, "artefact", uri, "rows", rows, "err", cerr)
	}
}

// produce streams the matching rows into an artefact and returns its URI and row count.
//
// It pages with the same keyset the search uses, so memory stays bounded whatever the window holds,
// and it stops the moment the cap is exceeded — the partial file is discarded, never published.
func (h *exportHandlers) produce(ctx context.Context, job cp.MessageExportJob, filter clickhouse.CDRSearchFilter, empty bool) (string, int, error) {
	artefact, err := h.sink.Create(exportArtefactName(job))
	if err != nil {
		return "", 0, err
	}
	defer artefact.Discard() // no-op once committed

	writer, err := newExportRowWriter(job.Format, artefact)
	if err != nil {
		return "", 0, err
	}

	written := 0
	if !empty {
		for {
			rows, serr := h.search.Search(ctx, filter, exportPageSize)
			if serr != nil {
				return "", 0, serr
			}
			if len(rows) == 0 {
				break
			}
			if written+len(rows) > ExportMaxRows {
				return "", 0, fmt.Errorf("more than %d rows match; narrow the window or add a filter", ExportMaxRows)
			}
			for _, dto := range exportRowsProjection(rows, !job.Masked) {
				if werr := writer.Write(dto); werr != nil {
					return "", 0, werr
				}
			}
			written += len(rows)

			last := rows[len(rows)-1]
			filter.After = &clickhouse.CDRKey{SubmittedAt: last.SubmittedAt, MessageID: last.MessageID}
			if len(rows) < exportPageSize {
				break
			}
		}
	}

	if ferr := writer.Flush(); ferr != nil {
		return "", 0, ferr
	}
	uri, cerr := artefact.Commit()
	if cerr != nil {
		return "", 0, cerr
	}
	return uri, written, nil
}

func (h *exportHandlers) get(ctx context.Context, in *getExportInput) (*exportJobOutput, error) {
	if h.jobs == nil {
		return nil, notFound("export job")
	}
	id, err := uuid.Parse(in.JobID)
	if err != nil {
		return nil, notFound("export job")
	}
	job, err := h.jobs.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &exportJobOutput{Body: toExportJobDTO(job)}, nil
}

func toExportJobDTO(job cp.MessageExportJob) exportJobDTO {
	dto := exportJobDTO{
		JobID:       job.ID.String(),
		Status:      string(job.Status),
		RowCount:    job.RowCount,
		DownloadURL: exportDownloadURL(job.ArtefactURI),
		Error:       job.Error,
		CreatedAt:   job.CreatedAt,
	}
	if !job.ExpiresAt.IsZero() {
		expires := job.ExpiresAt
		dto.ExpiresAt = &expires
	}
	return dto
}
