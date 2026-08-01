package clickhouse

import (
	"context"
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// msisdnPattern is the shape a number must have before it may be interpolated into the erasure mutation
// (which, being DDL-ish, cannot take a bound parameter). The caller normalises to E.164 digits first, so
// anything else is a bug or an injection attempt — either way it must not reach the statement.
var msisdnPattern = regexp.MustCompile(`^[0-9]{6,15}$`)

// DefaultEraseRowCap bounds how many CDR rows one erasure may remove in a single mutation. Beyond it the
// job fails with a clear message instead of launching an unbounded mutation over a table taking 8000 msg/s:
// an erasure that large is a maintenance-window operation, run deliberately, not a background surprise.
const DefaultEraseRowCap = 5_000_000

// CDREraser removes CDR rows for an RGPD erasure (§6.23, step-166).
//
// It uses a DELETE mutation, which retention deliberately does NOT (§14): retention drops whole partitions
// because it runs against the full 8000 msg/s stream, while an erasure targets one subject scattered across
// every partition — a partition drop cannot express it. Erasures are rare and legally mandated, so paying a
// mutation is the right trade here. The mutation is awaited (mutations_sync) so the attestation states what
// actually happened rather than what was merely scheduled.
//
// Cluster caveat: ALTER ... DELETE applies where it is issued. The CDR is a single-node ReplacingMergeTree
// today, so this is complete; a replicated/sharded deployment MUST switch these statements to ON CLUSTER
// before relying on the attestation, which is a legal document.
type CDREraser struct {
	conn   *Conn
	rowCap uint64
}

// NewCDREraser returns an eraser over conn, capped at DefaultEraseRowCap rows per erasure.
func NewCDREraser(c *Conn) *CDREraser { return &CDREraser{conn: c, rowCap: DefaultEraseRowCap} }

// WithRowCap overrides the per-erasure row cap (0 disables the cap).
func (e *CDREraser) WithRowCap(cap uint64) *CDREraser {
	e.rowCap = cap
	return e
}

// EraseCustomer removes every CDR row of a customer and reports how many rows were removed. It is the
// metadata half of a customer erasure: the content half is the crypto-shred of the customer's keys, which
// makes any body already written unreadable.
func (e *CDREraser) EraseCustomer(ctx context.Context, customerID uuid.UUID) (uint64, error) {
	const where = "customer_id = ?"
	return e.erase(ctx, where, fmt.Sprintf("customer_id = toUUID('%s')", customerID), customerID)
}

// EraseMSISDN removes every CDR row where the number appears as source or destination, ACROSS ALL CUSTOMERS
// (§14): the person's messages are erased wherever they are, not customer by customer. The caller passes the
// number already normalised to the form the CDR stores.
//
// It deliberately does NOT touch the opt-out: the suppression list lives in the control plane and must
// survive the erasure, because the duty not to contact the person again outlives the erasure of what was
// sent to them.
func (e *CDREraser) EraseMSISDN(ctx context.Context, msisdn string) (uint64, error) {
	if !msisdnPattern.MatchString(msisdn) {
		return 0, fmt.Errorf("clickhouse: refusing to erase malformed msisdn %q", msisdn)
	}
	// original_source_addr is included too: sender-ID rewriting fills it with the address as submitted, so a
	// predicate that ignored it would leave the subject's number behind once rewriting is in use.
	const where = "dest_addr = ? OR source_addr = ? OR original_source_addr = ?"
	lit := fmt.Sprintf("dest_addr = '%s' OR source_addr = '%s' OR original_source_addr = '%s'", msisdn, msisdn, msisdn)
	return e.erase(ctx, where, lit, msisdn, msisdn, msisdn)
}

// erase counts the matching rows, deletes them with an awaited mutation, and returns the count. The count is
// taken BEFORE the delete because that is what the attestation must state; the predicate is passed twice —
// bound for the count, and as a literal for the mutation, which cannot take bound parameters.
func (e *CDREraser) erase(ctx context.Context, boundWhere, literalWhere string, args ...any) (uint64, error) {
	count := func() (uint64, error) {
		var n uint64
		err := e.conn.QueryRow(ctx, "SELECT count() FROM "+cdrTable+" WHERE "+boundWhere, args...).Scan(&n)
		return n, err
	}

	matched, err := count()
	if err != nil {
		return 0, fmt.Errorf("clickhouse: count rows to erase: %w", err)
	}
	if matched == 0 {
		return 0, nil
	}
	if e.rowCap > 0 && matched > e.rowCap {
		return 0, fmt.Errorf("clickhouse: erasure matches %d rows, above the %d cap: run it in a maintenance window",
			matched, e.rowCap)
	}

	stmt := fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s SETTINGS mutations_sync = 2", cdrTable, literalWhere)
	if err := e.conn.Exec(ctx, stmt); err != nil {
		return 0, fmt.Errorf("clickhouse: erase cdr rows: %w", err)
	}

	// Verify rather than assume: a ClickHouse mutation only rewrites the parts that existed when it was
	// created, so rows written while it ran survive it. The attestation is a legal document — it must state
	// what is actually gone, so a residue fails the job instead of being attested as erased.
	residual, err := count()
	if err != nil {
		return 0, fmt.Errorf("clickhouse: verify erasure: %w", err)
	}
	if residual > 0 {
		return 0, fmt.Errorf("clickhouse: %d rows still match after the erasure (written while it ran): re-run it",
			residual)
	}
	return matched, nil
}
