package adminapi

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// ExportSink opens the artefact an export writes into. Declared consumer-side: the file tier below is
// what a deployment without object storage gets, and swapping in an object tier — the way the CDR
// tiering swaps its Destination (§6.14) — changes nothing in the worker.
type ExportSink interface {
	// Create opens an artefact under name. The caller must Commit or Discard it.
	Create(name string) (ExportArtefact, error)
}

// ExportArtefact is a half-written export. It is published only by Commit, so a reader never sees a
// partial file and a failed job leaves nothing behind.
type ExportArtefact interface {
	io.Writer
	// Commit publishes the artefact and returns its URI.
	Commit() (string, error)
	// Discard removes a partial artefact. It is a no-op after Commit.
	Discard()
}

// FileExportSink writes artefacts to a directory on this pod's filesystem — the local tier, like the
// tiering archive's FileDestination. It proves the path end to end; the real object destination is an
// infrastructure decision, and this seam is where it plugs in.
type FileExportSink struct{ dir string }

// NewFileExportSink returns a sink writing under dir.
func NewFileExportSink(dir string) *FileExportSink { return &FileExportSink{dir: dir} }

// Create opens "<name>.part" under the sink's directory. The final name appears only at Commit, so a
// concurrent reader of the directory cannot mistake a running export for a finished one.
func (s *FileExportSink) Create(name string) (ExportArtefact, error) {
	// The name is derived from a job id today, but a separator sneaking in later would let an artefact
	// escape the sink's directory — so the constraint is enforced here rather than assumed of callers.
	if name == "" || name != filepath.Base(name) {
		return nil, fmt.Errorf("export sink: %q is not a plain file name", name)
	}
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return nil, fmt.Errorf("export sink: prepare %s: %w", s.dir, err)
	}
	final := filepath.Join(s.dir, name)
	// 0600: an unmasked artefact carries subscriber numbers in clear, so it is readable by the
	// process that wrote it and nothing else.
	//nolint:gosec // G304: the path is this sink's own directory joined with a plain name checked above.
	f, err := os.OpenFile(final+".part", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("export sink: create %s: %w", name, err)
	}
	return &fileArtefact{f: f, final: final}, nil
}

type fileArtefact struct {
	f     *os.File
	final string
	done  bool
}

func (a *fileArtefact) Write(p []byte) (int, error) { return a.f.Write(p) }

func (a *fileArtefact) Commit() (string, error) {
	if err := a.f.Close(); err != nil {
		return "", fmt.Errorf("export sink: close: %w", err)
	}
	if err := os.Rename(a.f.Name(), a.final); err != nil {
		return "", fmt.Errorf("export sink: publish: %w", err)
	}
	a.done = true
	return "file://" + a.final, nil
}

func (a *fileArtefact) Discard() {
	if a.done {
		return
	}
	_ = a.f.Close()
	_ = os.Remove(a.f.Name())
}

// exportRowWriter serialises one masked CDR row at a time, so an export streams instead of building
// the whole file in memory.
type exportRowWriter interface {
	Write(row messageSummaryDTO) error
	// Flush pushes anything buffered to the artefact.
	Flush() error
}

// newExportRowWriter picks the writer for a format. Both formats project the SAME DTO the search
// returns, so an export and a search cannot disagree on a field — and neither can carry a body, since
// the DTO has no content member (invariant a).
func newExportRowWriter(format cp.ExportFormat, w io.Writer) (exportRowWriter, error) {
	switch format {
	case cp.ExportFormatCSV:
		return newCSVExportWriter(w)
	case cp.ExportFormatJSONL:
		return &jsonlExportWriter{enc: json.NewEncoder(w)}, nil
	default:
		return nil, fmt.Errorf("unsupported export format %q", format)
	}
}

// csvExportColumns is the header, and the column order every row follows. It is written out rather
// than derived by reflection so a field added to the DTO cannot silently shift an operator's columns.
var csvExportColumns = []string{
	"message_id", "trace_id", "account_id", "customer_id", "direction",
	"source_addr", "dest_addr", "original_source_addr", "connector_id", "route_id",
	"status", "error_code", "segment_count", "encoding",
	"submitted_at", "delivered_at", "latency_ms", "billed", "credits_charged",
}

type csvExportWriter struct{ w *csv.Writer }

func newCSVExportWriter(w io.Writer) (*csvExportWriter, error) {
	cw := csv.NewWriter(w)
	if err := cw.Write(csvExportColumns); err != nil {
		return nil, fmt.Errorf("export: write csv header: %w", err)
	}
	return &csvExportWriter{w: cw}, nil
}

func (c *csvExportWriter) Write(row messageSummaryDTO) error {
	return c.w.Write([]string{
		row.MessageID, row.TraceID, row.AccountID, row.CustomerID, row.Direction,
		row.SourceAddr, row.DestAddr, derefString(row.OriginalSourceAddr), derefString(row.ConnectorID), derefString(row.RouteID),
		row.Status, derefString(row.ErrorCode), strconv.Itoa(row.SegmentCount), derefString(row.Encoding),
		row.SubmittedAt.Format(time.RFC3339), formatTimePtr(row.DeliveredAt),
		formatIntPtr(row.LatencyMs), strconv.FormatBool(row.Billed), formatIntPtr(row.CreditsCharged),
	})
}

func (c *csvExportWriter) Flush() error {
	c.w.Flush()
	return c.w.Error()
}

type jsonlExportWriter struct{ enc *json.Encoder }

// Write emits one JSON object per line — json.Encoder already terminates each value with a newline.
func (j *jsonlExportWriter) Write(row messageSummaryDTO) error { return j.enc.Encode(row) }

func (j *jsonlExportWriter) Flush() error { return nil }

// exportArtefactName names the file after its job, so an artefact is always traceable to the request
// that produced it (and two jobs never collide).
func exportArtefactName(job cp.MessageExportJob) string {
	return "cdr-export-" + job.ID.String() + "." + string(job.Format)
}

// exportDownloadURL renders the stored artefact URI for the contract's download_url. A URI that does
// not parse is dropped rather than served as a broken link.
func exportDownloadURL(uri *string) *string {
	if uri == nil {
		return nil
	}
	if _, err := url.Parse(*uri); err != nil {
		return nil
	}
	return uri
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func formatTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

func formatIntPtr(v *int) string {
	if v == nil {
		return ""
	}
	return strconv.Itoa(*v)
}

// exportRowsProjection is the shared projection: one CDR row to the DTO both the search and the
// export serve, masked on the same rule.
func exportRowsProjection(rows []clickhouse.CDRRow, reveal bool) []messageSummaryDTO {
	out := make([]messageSummaryDTO, 0, len(rows))
	for _, row := range rows {
		out = append(out, toMessageSummaryDTO(row, reveal))
	}
	return out
}
