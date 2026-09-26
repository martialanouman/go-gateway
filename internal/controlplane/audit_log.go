package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// AuditIntent is one operator request about to run, as the consolidated audit trail records it before the
// handler (control_plane.audit_log, step-290c). It names who, which operation and which resource — never a
// request body, a query string or a token: Operator is the principal's subject, a fingerprint.
type AuditIntent struct {
	Operator    string
	OperationID string
	Method      string
	// Target is the resolved request path, without its query string.
	Target string
	// RequestID correlates the row with the request's logs; empty when the request carried none.
	RequestID string
}

// AuditEntry is one row of the audit trail as it is read back. Status nil means the outcome was not recorded.
type AuditEntry struct {
	ID          uuid.UUID
	Operator    string
	OperationID string
	Method      string
	Target      string
	RequestID   *string
	Status      *int
	At          time.Time
	FinishedAt  *time.Time
}

// AuditLogFilter narrows a read of the trail. A zero field does not filter; To is exclusive.
type AuditLogFilter struct {
	Operator string
	From, To *time.Time
}

// AuditLogKey is a keyset position in the trail: the (at, id) of a page's last row.
type AuditLogKey struct {
	At time.Time
	ID uuid.UUID
}
