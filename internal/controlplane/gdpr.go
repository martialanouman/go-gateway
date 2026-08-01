package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// GDPRSubjectType is what an erasure targets (control_plane.gdpr_erase_jobs.subject_type, §6.23).
type GDPRSubjectType string

// The erasure subjects.
const (
	// GDPRSubjectCustomer erases one customer: its content keys are crypto-shredded and its CDR rows removed.
	GDPRSubjectCustomer GDPRSubjectType = "customer"
	// GDPRSubjectMSISDN erases one phone number ACROSS every customer — but never its opt-out: the duty not
	// to contact the person again outlives the erasure of what was sent to them (§14).
	GDPRSubjectMSISDN GDPRSubjectType = "msisdn"
)

// Valid reports whether s is a published erasure subject.
func (s GDPRSubjectType) Valid() bool {
	return s == GDPRSubjectCustomer || s == GDPRSubjectMSISDN
}

// GDPRJobStatus is an erasure job's lifecycle state.
type GDPRJobStatus string

// The erasure job states.
const (
	GDPRJobQueued    GDPRJobStatus = "queued"
	GDPRJobRunning   GDPRJobStatus = "running"
	GDPRJobCompleted GDPRJobStatus = "completed"
	GDPRJobFailed    GDPRJobStatus = "failed"
)

// GDPREraseJob is one on-demand RGPD erasure and, once finished, its attestation — the proof of execution
// (scope, counters, completion time). The attestation never contains erased content.
type GDPREraseJob struct {
	ID          uuid.UUID
	SubjectType GDPRSubjectType
	SubjectID   string
	Status      GDPRJobStatus
	Attestation *string
	Operator    string
	CreatedAt   time.Time
	FinishedAt  *time.Time
}

// NewGDPREraseJob is the input to queue an erasure.
type NewGDPREraseJob struct {
	SubjectType GDPRSubjectType
	SubjectID   string
	Operator    string
}
