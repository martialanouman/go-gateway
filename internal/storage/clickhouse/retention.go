package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

// cdrTable is the single table this package owns. Retention addresses it by name in DDL (a partition
// expression cannot be bound as a query parameter), so the name is a constant, never caller-supplied.
const cdrTable = "cdr"

// partitionDayLayout is how ClickHouse names a daily partition of PARTITION BY toDate(submitted_at).
const partitionDayLayout = "2006-01-02"

// partitionNamePattern is the shape a daily partition name must have before it may be interpolated into
// DDL. system.parts is a trusted source, but a partition name reaches the SQL text unquoted-by-driver, so
// it is validated (and then re-rendered from the parsed date) rather than trusted verbatim.
var partitionNamePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// Partition is one daily CDR partition as ClickHouse reports it in system.parts.
type Partition struct {
	// Day is the partition's date — the value of toDate(submitted_at) for every row it holds.
	Day time.Time
	// Rows and Bytes are the partition's active size, for the purge log and for capacity dashboards.
	Rows  uint64
	Bytes uint64
}

// Name renders the partition's ClickHouse name from its parsed date, so nothing read from the server is
// interpolated into DDL verbatim.
func (p Partition) Name() string { return p.Day.Format(partitionDayLayout) }

// Archiver ships a partition to cold storage before it is dropped (tiering, §6.14). Implementations differ
// only in DESTINATION: the local one has ClickHouse write the partition as a Parquet file next to itself,
// a production one points at object storage. The real cold bucket is infrastructure and out of scope here —
// what this package guarantees is that a partition is archived BEFORE it is dropped, and never dropped when
// archiving failed. Declared consumer-side.
type Archiver interface {
	Archive(ctx context.Context, p Partition) error
}

// Destination builds the ClickHouse table-function expression a partition is archived into, for the given
// run token. It is operator configuration, not user input: it becomes part of the INSERT statement.
// FileDestination is the local default; a production deploy supplies an s3(...) builder instead — that is
// the pluggable seam. The same expression must be READABLE, because the archive is verified by reading it
// back before the partition is dropped.
//
// The token makes every archiving attempt write its OWN object. That is deliberate: two writers sharing one
// destination (two replicas, or a retry overlapping its predecessor) produce a truncated, unreadable file
// while both report success — and would then both drop the partition. Unique names cost a stale object after
// a failed attempt and buy the guarantee that no archive is ever half-written.
type Destination func(p Partition, token string) string

// archivePrefixPattern constrains the operator-supplied archive prefix. It lands inside a quoted SQL string
// literal, so an apostrophe would break out of it: reject anything but a plain path-ish name, at startup.
var archivePrefixPattern = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,128}$`)

// ValidArchivePrefix reports whether a configured archive prefix is safe to interpolate.
func ValidArchivePrefix(prefix string) bool { return archivePrefixPattern.MatchString(prefix) }

// FileDestination archives a partition as a Parquet file on the ClickHouse server, under its configured
// user_files directory, named "<prefix>-<YYYY-MM-DD>-<token>.parquet".
//
// It writes to the ClickHouse server's OWN disk, so it is the local/dev tier: it proves the path end to end
// but frees no space and dies with the node. Production supplies an s3(...) Destination.
//
// GDPR note for whoever enables tiering: the archive holds source_addr/dest_addr (MSISDNs) outside the
// platform, so it is reached by neither the crypto-shred nor the per-MSISDN erasure. Its own retention and
// erasure are the operator's responsibility.
func FileDestination(prefix string) Destination {
	return func(p Partition, token string) string {
		return fmt.Sprintf("file('%s-%s-%s.parquet', 'Parquet')", prefix, p.Name(), token)
	}
}

// PartitionArchiver archives a CDR partition by having ClickHouse itself write it out through a table
// function — the server streams the rows straight to the destination, so no partition ever transits through
// this process (and no Parquet encoder is needed here). Swapping Destination swaps the storage tier.
type PartitionArchiver struct {
	conn *Conn
	dest Destination
}

// NewPartitionArchiver returns an Archiver writing partitions to dest.
func NewPartitionArchiver(c *Conn, dest Destination) *PartitionArchiver {
	return &PartitionArchiver{conn: c, dest: dest}
}

// columnNamePattern guards a column name read from system.columns before it is interpolated into the
// archive projection.
var columnNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// projection builds the SELECT the archive exports: `*` — so a column added to the CDR is archived with no
// code change — with the columns Parquet cannot represent faithfully rendered as strings via REPLACE.
//
// It is derived from system.columns rather than hardcoded, because a hardcoded list silently rots: adding a
// UUID column to the CDR made ClickHouse refuse the whole export ("Internal type 'UUID' ... is not supported
// for conversion into Parquet"), which fails archiving forever and therefore freezes retention. Enums are
// cast too: Parquet would store their integer values, leaving an archive that cannot be read back without
// the platform's own schema.
func (a *PartitionArchiver) projection(ctx context.Context) (string, error) {
	rows, err := a.conn.Query(ctx,
		"SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ?", cdrTable)
	if err != nil {
		return "", fmt.Errorf("clickhouse: read cdr columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var casts []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return "", fmt.Errorf("clickhouse: scan cdr column: %w", err)
		}
		if !strings.Contains(typ, "UUID") && !strings.Contains(typ, "Enum") {
			continue
		}
		if !columnNamePattern.MatchString(name) {
			return "", fmt.Errorf("clickhouse: refusing to archive column with an unexpected name %q", name)
		}
		casts = append(casts, fmt.Sprintf("toString(%s) AS %s", name, name))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("clickhouse: read cdr columns: %w", err)
	}
	if len(casts) == 0 {
		return "*", nil
	}
	return "* REPLACE (" + strings.Join(casts, ", ") + ")", nil
}

// Archive writes every row of the partition to its own destination object and then READS IT BACK to confirm
// the rows landed. Verification is what makes the caller's "archive before drop" guarantee real: an export
// that silently wrote nothing (an already-emptied partition) or was truncated by a concurrent writer must
// never be mistaken for a successful archive, because the partition is dropped straight after.
func (a *PartitionArchiver) Archive(ctx context.Context, p Partition) error {
	if !partitionNamePattern.MatchString(p.Name()) {
		return fmt.Errorf("clickhouse: refusing to archive malformed partition %q", p.Name())
	}
	if p.Rows == 0 {
		return nil // nothing to archive; the caller may drop the empty partition
	}
	projection, err := a.projection(ctx)
	if err != nil {
		return err
	}
	dest := a.dest(p, uuid.NewString()[:8])

	stmt := fmt.Sprintf("INSERT INTO FUNCTION %s SELECT %s FROM %s WHERE toDate(submitted_at) = '%s'",
		dest, projection, cdrTable, p.Name())
	if err := a.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("clickhouse: archive partition %s: %w", p.Name(), err)
	}

	var archived uint64
	if err := a.conn.QueryRow(ctx, "SELECT count() FROM "+dest).Scan(&archived); err != nil {
		return fmt.Errorf("clickhouse: verify archive of partition %s: %w", p.Name(), err)
	}
	if archived < p.Rows {
		return fmt.Errorf("clickhouse: archive of partition %s holds %d rows, expected at least %d",
			p.Name(), archived, p.Rows)
	}
	return nil
}

// Retainer enforces CDR retention by DROPPING WHOLE PARTITIONS, never by DELETE (§14): at 8000 msg/s a
// delete-by-predicate would rewrite parts continuously, while dropping a daily partition is a metadata
// operation. Each expired partition is archived first (when an Archiver is configured) and dropped only
// once the archive succeeded, so retention never destroys data that failed to reach cold storage.
//
// The message BODY is not this component's job: it expires earlier and on its own, through the per-column
// TTL on content_ciphertext/content_key_id (migration 0003), so a body is already gone from a partition
// this Retainer is still keeping for its metadata.
type Retainer struct {
	conn      *Conn
	retention time.Duration
	archiver  Archiver
	metric    RetentionMetric
	logger    *slog.Logger
	now       func() time.Time
}

// observe records a partition outcome when a metric is wired.
func (r *Retainer) observe(outcome string) {
	if r.metric != nil {
		r.metric.Observe(outcome)
	}
}

// RetentionMetric counts what a retention pass did, so a pass that has been failing for weeks is an alert
// rather than a log line nobody reads — its consequence is a disk filling up. Declared consumer-side; the
// service adapts a Prometheus counter to it.
type RetentionMetric interface {
	// Observe records one partition outcome: "archived", "dropped", "archive_failed" or "drop_failed".
	Observe(outcome string)
}

// RetainerOption configures a Retainer.
type RetainerOption func(*Retainer)

// WithRetentionMetric records per-partition outcomes.
func WithRetentionMetric(m RetentionMetric) RetainerOption {
	return func(r *Retainer) { r.metric = m }
}

// WithArchiver archives each partition before dropping it (tiering). Without one, an expired partition is
// dropped outright.
func WithArchiver(a Archiver) RetainerOption { return func(r *Retainer) { r.archiver = a } }

// WithRetainerLogger sets the logger.
func WithRetainerLogger(l *slog.Logger) RetainerOption {
	return func(r *Retainer) {
		if l != nil {
			r.logger = l
		}
	}
}

// WithRetainerClock overrides the clock (tests).
func WithRetainerClock(now func() time.Time) RetainerOption {
	return func(r *Retainer) {
		if now != nil {
			r.now = now
		}
	}
}

// NewRetainer returns a Retainer keeping CDR partitions for retention.
func NewRetainer(c *Conn, retention time.Duration, opts ...RetainerOption) *Retainer {
	r := &Retainer{conn: c, retention: retention, logger: slog.Default(), now: time.Now}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Partitions lists the CDR table's active daily partitions, oldest first. A partition whose name is not a
// plain date is skipped: it cannot have come from PARTITION BY toDate(submitted_at), and it must never reach
// a DROP statement.
func (r *Retainer) Partitions(ctx context.Context) ([]Partition, error) {
	const q = `SELECT partition, sum(rows), sum(bytes_on_disk) FROM system.parts
		WHERE database = currentDatabase() AND table = ? AND active
		GROUP BY partition ORDER BY partition`
	rows, err := r.conn.Query(ctx, q, cdrTable)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: list cdr partitions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Partition
	for rows.Next() {
		var (
			name        string
			nRows, nBts uint64
		)
		if err := rows.Scan(&name, &nRows, &nBts); err != nil {
			return nil, fmt.Errorf("clickhouse: scan cdr partition: %w", err)
		}
		if !partitionNamePattern.MatchString(name) {
			r.logger.WarnContext(ctx, "skipping cdr partition with an unexpected name", "partition", name)
			continue
		}
		day, perr := time.Parse(partitionDayLayout, name)
		if perr != nil {
			r.logger.WarnContext(ctx, "skipping cdr partition with an unparsable date", "partition", name)
			continue
		}
		out = append(out, Partition{Day: day.UTC(), Rows: nRows, Bytes: nBts})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: list cdr partitions: %w", err)
	}
	return out, nil
}

// Expired returns the partitions whose whole day is older than the retention window. A partition is expired
// only once its LAST second is past retention (day + 1 day + retention <= now), so a partition is never
// dropped while it could still hold rows inside the window.
func (r *Retainer) Expired(ctx context.Context) ([]Partition, error) {
	all, err := r.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	now := r.now().UTC()
	var expired []Partition
	for _, p := range all {
		if partitionExpired(p.Day, r.retention, now) {
			expired = append(expired, p)
		}
	}
	return expired, nil
}

// partitionExpired is the decision to DESTROY a partition, isolated so it can be tested at its boundary.
// A daily partition holds rows from its 00:00 to its 23:59:59, so it is expired only once its LAST instant
// is past the retention window: day + 1 day + retention <= now. Using the day's start instead would drop a
// partition up to a day early, taking rows still inside the window with it.
func partitionExpired(day time.Time, retention time.Duration, now time.Time) bool {
	return !day.AddDate(0, 0, 1).Add(retention).After(now)
}

// PurgeReport is what one purge pass did.
type PurgeReport struct {
	Archived int
	Dropped  int
	Rows     uint64
}

// Purge archives (when configured) and drops every expired partition. A partition whose archive fails is
// LEFT IN PLACE and the pass continues with the next one: retention lagging is recoverable, data dropped
// without an archive is not.
//
// Running it on several instances at once is tolerable but not free: DROP PARTITION is a no-op on an
// already-dropped partition, and each archiving attempt writes its own destination object (so concurrent
// archives cannot truncate each other), but the same partition may then be archived twice. Prefer a single
// scheduled runner.
func (r *Retainer) Purge(ctx context.Context) (PurgeReport, error) {
	expired, err := r.Expired(ctx)
	if err != nil {
		return PurgeReport{}, err
	}
	var report PurgeReport
	for _, p := range expired {
		if ctx.Err() != nil {
			return report, nil // shutting down: the remaining partitions wait for the next pass
		}
		if r.archiver != nil {
			if aerr := r.archiver.Archive(ctx, p); aerr != nil {
				r.logger.ErrorContext(ctx, "archiving cdr partition failed, keeping it", "partition", p.Name(), "err", aerr)
				r.observe("archive_failed")
				continue
			}
			report.Archived++
			r.observe("archived")
		}
		if derr := r.DropPartition(ctx, p); derr != nil {
			r.logger.ErrorContext(ctx, "dropping cdr partition failed", "partition", p.Name(), "err", derr)
			r.observe("drop_failed")
			continue
		}
		report.Dropped++
		r.observe("dropped")
		report.Rows += p.Rows
		r.logger.InfoContext(ctx, "dropped expired cdr partition", "partition", p.Name(), "rows", p.Rows, "bytes", p.Bytes)
	}
	return report, nil
}

// DropPartition drops one daily partition — the whole retention mechanism in one metadata operation. The
// partition name is re-rendered from its parsed date and re-validated before it reaches the statement.
func (r *Retainer) DropPartition(ctx context.Context, p Partition) error {
	name := p.Name()
	if !partitionNamePattern.MatchString(name) {
		return fmt.Errorf("clickhouse: refusing to drop malformed partition %q", name)
	}
	if err := r.conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", cdrTable, name)); err != nil {
		return fmt.Errorf("clickhouse: drop partition %s: %w", name, err)
	}
	return nil
}

// Run purges once shortly after start and then on a ticker, until ctx is cancelled — the supervised
// component form. A failing pass is logged and retried on the next tick: retention is never urgent enough to
// crash a service over.
//
// The startup pass matters: with a daily interval, a service redeployed every day would otherwise never
// reach its first tick and retention would silently never run.
func (r *Retainer) Run(ctx context.Context, every time.Duration) error {
	if every <= 0 {
		<-ctx.Done()
		return nil
	}
	// Let the service finish coming up before the first pass.
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(retentionStartupDelay):
	}
	r.pass(ctx)

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.pass(ctx)
		}
	}
}

// retentionStartupDelay keeps the first retention pass off the service's startup path.
const retentionStartupDelay = time.Minute

// pass runs one purge, logging its outcome. It never returns an error: a retention fault must not take the
// service down.
func (r *Retainer) pass(ctx context.Context) {
	report, err := r.Purge(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		r.logger.ErrorContext(ctx, "cdr retention pass failed", "err", err)
		return
	}
	if report.Dropped > 0 || report.Archived > 0 {
		r.logger.InfoContext(ctx, "cdr retention pass",
			"archived", report.Archived, "dropped", report.Dropped, "rows", report.Rows)
	}
}
