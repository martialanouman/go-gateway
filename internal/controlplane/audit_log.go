package controlplane

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
