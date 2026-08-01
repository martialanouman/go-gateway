package adminapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// --- fakes ---

// fakeGDPRJobs is an in-memory erasure-job store.
type fakeGDPRJobs struct {
	mu        sync.Mutex
	jobs      map[uuid.UUID]cp.GDPREraseJob
	createErr error
}

func newFakeGDPRJobs() *fakeGDPRJobs {
	return &fakeGDPRJobs{jobs: map[uuid.UUID]cp.GDPREraseJob{}}
}

func (f *fakeGDPRJobs) Create(_ context.Context, in cp.NewGDPREraseJob) (cp.GDPREraseJob, error) {
	if f.createErr != nil {
		return cp.GDPREraseJob{}, f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	job := cp.GDPREraseJob{
		ID: uuid.New(), SubjectType: in.SubjectType, SubjectID: in.SubjectID,
		Status: cp.GDPRJobQueued, Operator: in.Operator,
	}
	f.jobs[job.ID] = job
	return job, nil
}

func (f *fakeGDPRJobs) Get(_ context.Context, id uuid.UUID) (cp.GDPREraseJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobs[id]
	if !ok {
		return cp.GDPREraseJob{}, errors.New("not found")
	}
	return job, nil
}

func (f *fakeGDPRJobs) MarkRunning(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job := f.jobs[id]
	job.Status = cp.GDPRJobRunning
	f.jobs[id] = job
	return nil
}

func (f *fakeGDPRJobs) Finish(_ context.Context, id uuid.UUID, status cp.GDPRJobStatus, attestation string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	job := f.jobs[id]
	job.Status = status
	job.Attestation = &attestation
	f.jobs[id] = job
	return nil
}

func (f *fakeGDPRJobs) only(t *testing.T) cp.GDPREraseJob {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.jobs) != 1 {
		t.Fatalf("expected exactly one job, got %d", len(f.jobs))
	}
	for _, j := range f.jobs {
		return j
	}
	return cp.GDPREraseJob{}
}

// fakeUnroutedEraser records the unrouted-MO purge of an MSISDN erasure.
type fakeUnroutedEraser struct {
	rows      int
	gotMSISDN string
}

func (f *fakeUnroutedEraser) DeleteByMSISDN(_ context.Context, msisdn string) (int, error) {
	f.gotMSISDN = msisdn
	return f.rows, nil
}

func (f *fakeUnroutedEraser) List(context.Context, int, *cp.UnroutedMOKey) ([]cp.UnroutedMO, error) {
	return nil, nil
}

type fakeCDREraser struct {
	customerRows uint64
	msisdnRows   uint64
	gotCustomer  uuid.UUID
	gotMSISDN    string
	err          error
}

func (f *fakeCDREraser) EraseCustomer(_ context.Context, id uuid.UUID) (uint64, error) {
	f.gotCustomer = id
	return f.customerRows, f.err
}

func (f *fakeCDREraser) EraseMSISDN(_ context.Context, msisdn string) (uint64, error) {
	f.gotMSISDN = msisdn
	return f.msisdnRows, f.err
}

// newGDPRAPI builds the Admin API with a verifier granting the operator token the gdpr:erase scope.
func newGDPRAPI(t *testing.T, deps adminapi.Deps) http.Handler {
	t.Helper()
	v, err := auth.NewStaticVerifier([]string{operatorToken + ":gdpr:erase"})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	deps.Verifier = v
	deps.GDPRRunner = syncRunner{}
	if deps.UnroutedMO == nil {
		deps.UnroutedMO = &fakeUnroutedEraser{}
	}
	mux, _ := adminapi.New(deps)
	return mux
}

func postErase(t *testing.T, api http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/gdpr/erase", body))
	return w
}

// TestGDPREraseMSISDNAcrossCustomersKeepsOptOut is the §14 requirement: erasing a phone number removes its
// messages across every customer, but the opt-out is NEVER erased — the duty not to contact the person again
// outlives the erasure. The eraser exposes no suppression operation at all, and the attestation says so.
func TestGDPREraseMSISDNAcrossCustomersKeepsOptOut(t *testing.T) {
	jobs := newFakeGDPRJobs()
	eraser := &fakeCDREraser{msisdnRows: 42}
	unrouted := &fakeUnroutedEraser{rows: 3}
	keys := &fakeContentKeyEraser{}
	api := newGDPRAPI(t, adminapi.Deps{GDPRJobs: jobs, CDREraser: eraser, UnroutedMO: unrouted, ContentKeyEraser: keys})

	w := postErase(t, api, `{"subject_type":"msisdn","id":"+2250700000000"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}

	// The number is normalised to the canonical form the CDR stores, so the erasure actually matches rows.
	if eraser.gotMSISDN != "2250700000000" {
		t.Errorf("erased msisdn = %q, want the normalised 2250700000000", eraser.gotMSISDN)
	}
	// A phone-number erasure must not crypto-shred anyone's keys.
	if keys.called {
		t.Error("erasing an MSISDN must not destroy a customer's content keys")
	}

	job := jobs.only(t)
	if job.Status != cp.GDPRJobCompleted || job.Attestation == nil {
		t.Fatalf("job = %+v, want completed with an attestation", job)
	}
	att := *job.Attestation
	if !strings.Contains(att, "cdr_rows_erased=42") {
		t.Errorf("attestation %q must state how much was erased", att)
	}
	if !strings.Contains(att, "opt_out_preserved=true") {
		t.Errorf("attestation %q must record that the opt-out was preserved", att)
	}
	// Unrouted-MO records carry the subject's number with no retention of their own, so they are erased too.
	if unrouted.gotMSISDN != "2250700000000" || !strings.Contains(att, "unrouted_mo_rows_erased=3") {
		t.Errorf("unrouted-MO purge missing: got=%q attestation=%q", unrouted.gotMSISDN, att)
	}
	// The attestation is a legal document: it must state what it covers, and by omission what it does not.
	if !strings.Contains(att, "scope=") || !strings.Contains(att, "excludes:") {
		t.Errorf("attestation %q must qualify its scope", att)
	}
}

// TestGDPREraseCustomerShredsThenErases: a customer erasure crypto-shreds the content keys AND removes the
// CDR rows, and attests both counters.
func TestGDPREraseCustomerShredsThenErases(t *testing.T) {
	cust := uuid.New()
	jobs := newFakeGDPRJobs()
	eraser := &fakeCDREraser{customerRows: 7}
	keys := &fakeContentKeyEraser{count: 3}
	api := newGDPRAPI(t, adminapi.Deps{GDPRJobs: jobs, CDREraser: eraser, ContentKeyEraser: keys})

	w := postErase(t, api, `{"subject_type":"customer","id":"`+cust.String()+`"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	if !keys.called || keys.gotID != cust {
		t.Errorf("crypto-shred not performed for %s (called=%v, got=%s)", cust, keys.called, keys.gotID)
	}
	if eraser.gotCustomer != cust {
		t.Errorf("cdr erasure targeted %s, want %s", eraser.gotCustomer, cust)
	}
	job := jobs.only(t)
	att := ""
	if job.Attestation != nil {
		att = *job.Attestation
	}
	if job.Status != cp.GDPRJobCompleted ||
		!strings.Contains(att, "content_keys_destroyed=3") || !strings.Contains(att, "cdr_rows_erased=7") {
		t.Errorf("job = %+v attestation=%q, want completed with both counters", job, att)
	}
}

// TestGDPREraseRecordsFailureOnTheJob: an erasure fault is recorded on the job (which the operator reads),
// not lost in a background error.
func TestGDPREraseRecordsFailureOnTheJob(t *testing.T) {
	jobs := newFakeGDPRJobs()
	eraser := &fakeCDREraser{err: errors.New("clickhouse unavailable")}
	api := newGDPRAPI(t, adminapi.Deps{GDPRJobs: jobs, CDREraser: eraser, ContentKeyEraser: &fakeContentKeyEraser{}})

	if w := postErase(t, api, `{"subject_type":"msisdn","id":"+2250700000000"}`); w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	job := jobs.only(t)
	if job.Status != cp.GDPRJobFailed {
		t.Errorf("job status = %q, want failed", job.Status)
	}
}

// TestGDPREraseValidatesSubject: an unknown subject type or a malformed id is refused before anything is
// erased — an irreversible operation never runs on a guess.
func TestGDPREraseValidatesSubject(t *testing.T) {
	for _, body := range []string{
		`{"subject_type":"everything","id":"x"}`,
		`{"subject_type":"customer","id":"not-a-uuid"}`,
		`{"subject_type":"msisdn","id":"not-a-number"}`,
	} {
		jobs := newFakeGDPRJobs()
		eraser := &fakeCDREraser{}
		api := newGDPRAPI(t, adminapi.Deps{GDPRJobs: jobs, CDREraser: eraser, ContentKeyEraser: &fakeContentKeyEraser{}})
		w := postErase(t, api, body)
		if w.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s: status = %d, want 422; body=%s", body, w.Code, w.Body.String())
		}
		if len(jobs.jobs) != 0 || eraser.gotMSISDN != "" || eraser.gotCustomer != uuid.Nil {
			t.Errorf("%s: nothing may be queued or erased on an invalid request", body)
		}
	}
}

// TestGDPREraseRequiresScope: without gdpr:erase the request is refused (403).
func TestGDPREraseRequiresScope(t *testing.T) {
	jobs := newFakeGDPRJobs()
	// newTestAPIWith grants admin:read|admin:write, NOT gdpr:erase.
	api := newTestAPIWith(t, adminapi.Deps{GDPRJobs: jobs, CDREraser: &fakeCDREraser{}, ContentKeyEraser: &fakeContentKeyEraser{}, UnroutedMO: &fakeUnroutedEraser{}, GDPRRunner: syncRunner{}})
	if w := postErase(t, api, `{"subject_type":"msisdn","id":"+2250700000000"}`); w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (missing gdpr:erase); body=%s", w.Code, w.Body.String())
	}
	if len(jobs.jobs) != 0 {
		t.Error("nothing may be queued without the scope")
	}
}

// TestGetGDPREraseJobReturnsAttestation: the operator reads the proof of execution back.
func TestGetGDPREraseJobReturnsAttestation(t *testing.T) {
	jobs := newFakeGDPRJobs()
	api := newGDPRAPI(t, adminapi.Deps{GDPRJobs: jobs, CDREraser: &fakeCDREraser{msisdnRows: 5}, ContentKeyEraser: &fakeContentKeyEraser{}})
	if w := postErase(t, api, `{"subject_type":"msisdn","id":"+2250700000000"}`); w.Code != http.StatusAccepted {
		t.Fatalf("queue failed: %d", w.Code)
	}
	job := jobs.only(t)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/gdpr/erase/"+job.ID.String(), ""))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "completed" || got["subject_type"] != "msisdn" {
		t.Errorf("job = %v, want a completed msisdn job", got)
	}
	att, _ := got["attestation"].(string)
	if !strings.Contains(att, "cdr_rows_erased=5") {
		t.Errorf("attestation = %q, want the erasure counters", att)
	}
}
