package adminapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

var (
	errNotFoundForTest   = errs.ErrNotFound
	errNoCapacityForTest = errors.New("runner: no capacity")
)

// --- fakes ---

// fakeExportJobs is the durable job record. It is deliberately synchronous and in-memory: the tests
// assert on the job's FINAL state, which is what an operator polls.
type fakeExportJobs struct {
	mu        sync.Mutex
	jobs      map[uuid.UUID]cp.MessageExportJob
	createErr error
}

func newFakeExportJobs() *fakeExportJobs {
	return &fakeExportJobs{jobs: map[uuid.UUID]cp.MessageExportJob{}}
}

func (f *fakeExportJobs) Create(_ context.Context, in cp.NewMessageExportJob) (cp.MessageExportJob, error) {
	if f.createErr != nil {
		return cp.MessageExportJob{}, f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	job := cp.MessageExportJob{
		ID: uuid.New(), Status: cp.ExportJobQueued, Format: in.Format, Masked: in.Masked,
		Filters: in.Filters, Operator: in.Operator, CreatedAt: time.Now().UTC(), ExpiresAt: in.ExpiresAt,
	}
	f.jobs[job.ID] = job
	return job, nil
}

func (f *fakeExportJobs) Get(_ context.Context, id uuid.UUID) (cp.MessageExportJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[id]
	if !ok {
		return cp.MessageExportJob{}, errNotFoundForTest
	}
	return job, nil
}

func (f *fakeExportJobs) MarkRunning(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job := f.jobs[id]
	job.Status = cp.ExportJobRunning
	f.jobs[id] = job
	return nil
}

func (f *fakeExportJobs) Complete(_ context.Context, id uuid.UUID, uri string, rows int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job := f.jobs[id]
	job.Status, job.ArtefactURI, job.RowCount = cp.ExportJobCompleted, &uri, &rows
	f.jobs[id] = job
	return nil
}

func (f *fakeExportJobs) Fail(_ context.Context, id uuid.UUID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job := f.jobs[id]
	job.Status, job.Error = cp.ExportJobFailed, &reason
	f.jobs[id] = job
	return nil
}

// refusingRunner has no capacity left.
type refusingRunner struct{}

func (refusingRunner) Go(string, func(ctx context.Context) error) error { return errNoCapacityForTest }

// pagingSearchStore answers keyset pages from a fixed set of rows, like the real store.
type pagingSearchStore struct {
	rows  []clickhouse.CDRRow
	calls int
}

func (s *pagingSearchStore) Search(_ context.Context, f clickhouse.CDRSearchFilter, limit int) ([]clickhouse.CDRRow, error) {
	s.calls++
	start := 0
	if f.After != nil {
		for i, r := range s.rows {
			if r.MessageID == f.After.MessageID {
				start = i + 1
				break
			}
		}
	}
	end := min(start+limit, len(s.rows))
	return s.rows[start:end], nil
}

// --- harness ---

type exportJobBody struct {
	JobID       string  `json:"job_id"`
	Status      string  `json:"status"`
	RowCount    *int    `json:"row_count"`
	DownloadURL *string `json:"download_url"`
	Error       *string `json:"error"`
}

func exportRequest(t *testing.T, extra map[string]any) string {
	t.Helper()
	now := time.Now().UTC()
	body := map[string]any{
		"filters": map[string]any{
			"from_date": now.Add(-24 * time.Hour).Format(time.RFC3339),
			"to_date":   now.Format(time.RFC3339),
		},
	}
	for k, v := range extra {
		body[k] = v
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

func postExport(t *testing.T, deps adminapi.Deps, scopes, body string) (int, exportJobBody, string) {
	t.Helper()

	handler := newTestAPIWithScopes(t, deps, scopes)
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/messages/export", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var out exportJobBody
	if rec.Code < 300 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v (%s)", err, rec.Body)
		}
	}
	return rec.Code, out, rec.Body.String()
}

// exportDeps wires a working export over a temp directory.
func exportDeps(t *testing.T, rows []clickhouse.CDRRow) (adminapi.Deps, *fakeExportJobs, string) {
	t.Helper()

	dir := t.TempDir()
	jobs := newFakeExportJobs()
	return adminapi.Deps{
		MessageSearch: &pagingSearchStore{rows: rows},
		ExportJobs:    jobs,
		ExportSink:    adminapi.NewFileExportSink(dir),
		GDPRRunner:    syncRunner{},
	}, jobs, dir
}

func onlyArtefact(t *testing.T, dir string) string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 1 {
		t.Fatalf("directory holds %v, want exactly one artefact", names)
	}
	content, err := os.ReadFile(filepath.Join(dir, names[0]))
	if err != nil {
		t.Fatalf("read artefact: %v", err)
	}
	return string(content)
}

// --- the export itself ---

// TestExportProducesAMaskedArtefact is the step's headline criterion: a job is queued, the worker
// writes the file, and the numbers in it are masked for a caller without msisdn:reveal.
func TestExportProducesAMaskedArtefact(t *testing.T) {
	deps, jobs, dir := exportDeps(t, []clickhouse.CDRRow{searchRowFixture("33612345678")})

	code, body, raw := postExport(t, deps, "cdr:export_bulk", exportRequest(t, nil))
	if code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", code, raw)
	}

	job, err := jobs.Get(context.Background(), uuid.MustParse(body.JobID))
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != cp.ExportJobCompleted {
		t.Fatalf("job status = %q (error=%v), want completed", job.Status, job.Error)
	}
	if job.RowCount == nil || *job.RowCount != 1 {
		t.Errorf("row_count = %v, want 1", job.RowCount)
	}
	if job.ArtefactURI == nil || !strings.HasPrefix(*job.ArtefactURI, "file://") {
		t.Errorf("artefact uri = %v, want a file:// uri", job.ArtefactURI)
	}

	artefact := onlyArtefact(t, dir)
	if strings.Contains(artefact, "33612345678") {
		t.Errorf("the artefact carries an unmasked number:\n%s", artefact)
	}
	if !strings.Contains(artefact, "3361*****78") {
		t.Errorf("the artefact does not carry the masked number:\n%s", artefact)
	}
}

// TestExportNeverCarriesABody is invariant (a) on the artefact: the rows handed to the writer have
// their content columns populated, and none of it may reach the file.
func TestExportNeverCarriesABody(t *testing.T) {
	deps, _, dir := exportDeps(t, []clickhouse.CDRRow{searchRowFixture("33612345678")})

	if code, _, raw := postExport(t, deps, "cdr:export_bulk", exportRequest(t, nil)); code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", code, raw)
	}
	if artefact := onlyArtefact(t, dir); strings.Contains(artefact, secretBody) {
		t.Errorf("the artefact carries the message body:\n%s", artefact)
	}
}

// TestExportUnmaskedRequiresTheRevealScope is the security point of the step: without it the
// mask_msisdn flag would be a way around every role-based mask.
func TestExportUnmaskedRequiresTheRevealScope(t *testing.T) {
	deps, _, dir := exportDeps(t, []clickhouse.CDRRow{searchRowFixture("33612345678")})

	code, _, raw := postExport(t, deps, "cdr:export_bulk", exportRequest(t, map[string]any{"mask_msisdn": false}))
	if code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (%s)", code, raw)
	}
	// Refused, not silently masked: no artefact at all.
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a refused export still produced %d file(s)", len(entries))
	}
}

func TestExportUnmaskedIsAllowedWithTheRevealScope(t *testing.T) {
	deps, _, dir := exportDeps(t, []clickhouse.CDRRow{searchRowFixture("33612345678")})

	code, _, raw := postExport(t, deps, "cdr:export_bulk|msisdn:reveal",
		exportRequest(t, map[string]any{"mask_msisdn": false}))
	if code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", code, raw)
	}
	if artefact := onlyArtefact(t, dir); !strings.Contains(artefact, "33612345678") {
		t.Errorf("the unmasked artefact does not carry the number:\n%s", artefact)
	}
}

// TestExportRefusesBeyondTheRowCap: the job FAILS rather than truncating. A truncated export reads as
// an exhaustive one, which is how an operator draws a wrong conclusion from a right-looking file.
func TestExportRefusesBeyondTheRowCap(t *testing.T) {
	rows := make([]clickhouse.CDRRow, adminapi.ExportMaxRows+1)
	for i := range rows {
		rows[i] = searchRowFixture("3361234" + strings.Repeat("0", 4))
		rows[i].SubmittedAt = time.Now().UTC().Add(-time.Duration(i) * time.Second)
	}
	deps, jobs, dir := exportDeps(t, rows)

	code, body, raw := postExport(t, deps, "cdr:export_bulk", exportRequest(t, nil))
	if code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", code, raw)
	}
	job, err := jobs.Get(context.Background(), uuid.MustParse(body.JobID))
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != cp.ExportJobFailed {
		t.Fatalf("status = %q, want failed beyond the row cap", job.Status)
	}
	if job.Error == nil || !strings.Contains(*job.Error, "narrow") {
		t.Errorf("error = %v, want a message telling the operator to narrow the window", job.Error)
	}
	// The partial file must not linger: a half-written export is worse than none.
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a failed export left %d file(s) behind", len(entries))
	}
}

func TestExportWritesJSONLWhenAsked(t *testing.T) {
	deps, _, dir := exportDeps(t, []clickhouse.CDRRow{searchRowFixture("33612345678")})

	code, _, raw := postExport(t, deps, "cdr:export_bulk", exportRequest(t, map[string]any{"format": "jsonl"}))
	if code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", code, raw)
	}
	artefact := onlyArtefact(t, dir)
	var row map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(artefact)), &row); err != nil {
		t.Fatalf("the jsonl artefact is not one JSON object per line: %v (%s)", err, artefact)
	}
	if row["message_id"] == nil {
		t.Errorf("the jsonl row has no message_id: %s", artefact)
	}
}

// --- refusals ---

func TestExportRefusedWhenNoStorageIsConfigured(t *testing.T) {
	deps := adminapi.Deps{MessageSearch: &pagingSearchStore{}, ExportJobs: newFakeExportJobs(), GDPRRunner: syncRunner{}}

	code, _, raw := postExport(t, deps, "cdr:export_bulk", exportRequest(t, nil))
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (%s)", code, raw)
	}
}

func TestExportRequiresItsOwnScope(t *testing.T) {
	deps, _, _ := exportDeps(t, nil)

	// admin:read is enough to SEARCH; it is not enough to export a hundred thousand rows.
	if code, _, _ := postExport(t, deps, "admin:read", exportRequest(t, nil)); code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 without cdr:export_bulk", code)
	}
}

func TestExportRefusesAnUnboundedWindow(t *testing.T) {
	deps, _, _ := exportDeps(t, nil)
	now := time.Now().UTC()

	body := exportRequest(t, nil)
	body = strings.Replace(body,
		now.Add(-24*time.Hour).Format(time.RFC3339),
		now.Add(-40*24*time.Hour).Format(time.RFC3339), 1)

	code, _, raw := postExport(t, deps, "cdr:export_bulk", body)
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (%s)", code, raw)
	}
	if !strings.Contains(raw, "from_date") {
		t.Errorf("the error does not name from_date: %s", raw)
	}
}

// TestExportRefusesAnUnknownFilter: an ignored filter would WIDEN an export. The contract says
// additionalProperties: false, and the API must enforce it rather than quietly dropping the member.
func TestExportRefusesAnUnknownFilter(t *testing.T) {
	deps, _, _ := exportDeps(t, nil)
	now := time.Now().UTC()
	body := `{"filters":{"from_date":"` + now.Add(-time.Hour).Format(time.RFC3339) +
		`","to_date":"` + now.Format(time.RFC3339) + `","connectorId":"` + uuid.NewString() + `"}}`

	if code, _, raw := postExport(t, deps, "cdr:export_bulk", body); code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 for an unknown filter (%s)", code, raw)
	}
}

// TestExportClosesAJobItCouldNotStart: a queued row nothing will ever pick up reads, to an operator
// polling it, as an export still running hours later.
func TestExportClosesAJobItCouldNotStart(t *testing.T) {
	deps, jobs, _ := exportDeps(t, nil)
	deps.GDPRRunner = refusingRunner{}

	code, _, raw := postExport(t, deps, "cdr:export_bulk", exportRequest(t, nil))
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (%s)", code, raw)
	}
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	for _, job := range jobs.jobs {
		if job.Status != cp.ExportJobFailed {
			t.Errorf("unstarted job left in %q, want failed", job.Status)
		}
	}
}

// --- reading a job back ---

func TestGetExportJobReturnsItsArtefact(t *testing.T) {
	deps, jobs, _ := exportDeps(t, []clickhouse.CDRRow{searchRowFixture("33612345678")})
	_, created, _ := postExport(t, deps, "cdr:export_bulk", exportRequest(t, nil))

	handler := newTestAPIWithScopes(t, deps, "cdr:export_bulk")
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/messages/export/"+created.JobID, http.NoBody)
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}
	var body exportJobBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != string(cp.ExportJobCompleted) {
		t.Errorf("status = %q, want completed", body.Status)
	}
	if body.DownloadURL == nil {
		t.Error("download_url is null on a completed job")
	}
	_ = jobs
}

func TestGetExportJobUnknownIs404(t *testing.T) {
	deps, _, _ := exportDeps(t, nil)

	handler := newTestAPIWithScopes(t, deps, "cdr:export_bulk")
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/messages/export/"+uuid.NewString(), http.NoBody)
	req.Header.Set("Authorization", "Bearer "+operatorToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}
