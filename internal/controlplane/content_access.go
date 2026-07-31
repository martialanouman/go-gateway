package controlplane

import "github.com/google/uuid"

// ContentAccessOutcome is the result recorded for one content:read access (control_plane.content_access_audit.
// outcome, §14). It captures WHY a read did or did not return the plaintext, for the audit trail — never the
// plaintext itself.
type ContentAccessOutcome string

// The content-access outcomes.
const (
	ContentAccessGranted    ContentAccessOutcome = "granted"    // decrypted and returned
	ContentAccessUnreadable ContentAccessOutcome = "unreadable" // key destroyed (crypto-shred) or decrypt failed
	ContentAccessNotFound   ContentAccessOutcome = "not_found"  // no message / no stored content
)

// ContentAccess is one audited content:read access.
type ContentAccess struct {
	Operator   string
	MessageID  uuid.UUID
	CustomerID *uuid.UUID
	Outcome    ContentAccessOutcome
}
