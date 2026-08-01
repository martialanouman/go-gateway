package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// ExportFormat is the file format an export produces.
//
// Parquet is deliberately absent: the tiering archive (§6.14) lets ClickHouse write its own Parquet
// server-side, but an export is masked per role — a Go rule — so the file is written by the gateway,
// and a columnar encoder is a dependency nothing asks for yet.
type ExportFormat string

const (
	ExportFormatCSV   ExportFormat = "csv"
	ExportFormatJSONL ExportFormat = "jsonl"
)

// Valid reports whether f is a format the exporter can write.
func (f ExportFormat) Valid() bool {
	return f == ExportFormatCSV || f == ExportFormatJSONL
}

// ExportJobStatus is the lifecycle of an export job.
type ExportJobStatus string

const (
	ExportJobQueued    ExportJobStatus = "queued"
	ExportJobRunning   ExportJobStatus = "running"
	ExportJobCompleted ExportJobStatus = "completed"
	ExportJobFailed    ExportJobStatus = "failed"
)

// MessageExportJob is one asynchronous CDR export: the request, and — once finished — where the
// artefact landed and how many rows it holds.
//
// It records the request and its outcome, never a message: no body (invariant a). Masked states what
// the ARTEFACT contains rather than what was asked, since producing an unmasked one requires
// msisdn:reveal; it is therefore the durable proof of which file exists.
type MessageExportJob struct {
	ID          uuid.UUID
	Status      ExportJobStatus
	Format      ExportFormat
	Masked      bool
	Filters     []byte // the submitted predicates, verbatim, so an export is reproducible
	RowCount    *int
	ArtefactURI *string
	Error       *string
	Operator    string
	CreatedAt   time.Time
	ExpiresAt   time.Time
	FinishedAt  *time.Time
}

// NewMessageExportJob is the input to queue an export.
type NewMessageExportJob struct {
	Format    ExportFormat
	Masked    bool
	Filters   []byte
	Operator  string
	ExpiresAt time.Time
}
