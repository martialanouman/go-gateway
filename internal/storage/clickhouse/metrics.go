package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// metricsMaxExecutionSeconds bounds one metrics read on the server: an operator's dashboard must not be
// able to hold the cluster's CPU for as long as it keeps a tab open.
const metricsMaxExecutionSeconds = 10

// clickhouseTimeoutExceeded is TIMEOUT_EXCEEDED, raised when max_execution_time trips.
const clickhouseTimeoutExceeded = 159

// metricsMessages collapses each message of a submitted_at window to its final status, like cdrAggregate
// but over the few columns a count needs: the explorer's inner level takes argMax of every column,
// ciphertext included, which a window of millions of messages cannot afford.
const metricsMessages = `SELECT submitted_at, customer_id, direction, connector_id, latency_ms,
	` + cdrStatusPrecedence + ` AS status
FROM (
	SELECT submitted_at, message_id, any(customer_id) AS customer_id, any(direction) AS direction,
		anyIf(connector_id, ` + cdrDispatched + `) AS connector_id,
		maxIf(latency_ms, ` + cdrDispatched + `) AS latency_ms,
		` + cdrStatusCounts + `
	FROM (
		SELECT customer_id, direction, submitted_at, message_id, segment_seq,
			argMax(status, version) AS status, argMax(segment_count, version) AS segment_count,
			argMax(connector_id, version) AS connector_id, argMax(latency_ms, version) AS latency_ms
		FROM cdr
		WHERE submitted_at >= ? AND submitted_at < ?
		GROUP BY customer_id, direction, submitted_at, message_id, segment_seq
	) GROUP BY submitted_at, message_id
)`

// metricsFailed counts an expiry as a failure: both are a message that will never be delivered, and the
// contract has no separate expired counter.
const metricsFailed = `status IN ('failed', 'expired')`

// MetricsCounts is the summary of the messages submitted within a window, each in its current status.
type MetricsCounts struct {
	Submitted  uint64
	Delivered  uint64
	Failed     uint64
	Rejected   uint64
	MOReceived uint64
	// E2EP50 and E2EP99 are nil when no message of the window was delivered with a measured latency.
	E2EP50 *float64
	E2EP99 *float64
}

// MetricsSummary counts the MT and MO messages submitted in [from, to).
func (r *CDRReader) MetricsSummary(ctx context.Context, from, to time.Time) (MetricsCounts, error) {
	const query = `SELECT
		countIf(direction = 'mt'),
		countIf(direction = 'mt' AND status = 'delivered'),
		countIf(direction = 'mt' AND ` + metricsFailed + `),
		countIf(direction = 'mt' AND status = 'rejected'),
		countIf(direction = 'mo'),
		quantilesTDigestIf(0.5, 0.99)(latency_ms, direction = 'mt' AND status = 'delivered')
	FROM (` + metricsMessages + `)`

	var out MetricsCounts
	var quantiles []float32
	err := r.conn.QueryRow(metricsContext(ctx), query, from, to).
		Scan(&out.Submitted, &out.Delivered, &out.Failed, &out.Rejected, &out.MOReceived, &quantiles)
	if err != nil {
		return MetricsCounts{}, metricsErr("summary", err)
	}
	if len(quantiles) == 2 && !math.IsNaN(float64(quantiles[0])) {
		p50, p99 := float64(quantiles[0]), float64(quantiles[1])
		out.E2EP50, out.E2EP99 = &p50, &p99
	}
	return out, nil
}

// TrafficDimension is the key a traffic series is broken down by. A group is not one: the CDR carries no
// group, so the caller folds customers into their current group.
type TrafficDimension string

// The dimensions the CDR can break traffic down by.
const (
	TrafficByConnector TrafficDimension = "connector"
	TrafficByCustomer  TrafficDimension = "customer"
)

// TrafficPoint is one bucket of one key's MT traffic.
type TrafficPoint struct {
	Bucket    time.Time
	Key       uuid.UUID
	Submitted uint64
	Delivered uint64
	Failed    uint64
}

// Traffic buckets the MT messages submitted in [from, to) by step and dimension. A message never dispatched
// has no connector and appears in no connector series. More than maxRows points is refused as
// ErrServiceUnavailable rather than returned truncated.
func (r *CDRReader) Traffic(ctx context.Context, from, to time.Time, step time.Duration, dim TrafficDimension, maxRows int) ([]TrafficPoint, error) {
	key, filter := `customer_id`, ``
	if dim == TrafficByConnector {
		key, filter = `assumeNotNull(connector_id)`, ` AND connector_id IS NOT NULL`
	}
	query := `SELECT toStartOfInterval(submitted_at, toIntervalSecond(?)) AS bucket, ` + key + ` AS key,
		count(), countIf(status = 'delivered'), countIf(` + metricsFailed + `)
	FROM (` + metricsMessages + `)
	WHERE direction = 'mt'` + filter + `
	GROUP BY bucket, key
	ORDER BY bucket, key
	LIMIT ?`

	rows, err := r.conn.Query(metricsContext(ctx), query, int64(step/time.Second), from, to, maxRows+1)
	if err != nil {
		return nil, metricsErr("traffic", err)
	}
	defer func() { _ = rows.Close() }()

	var out []TrafficPoint
	for rows.Next() {
		var p TrafficPoint
		if err := rows.Scan(&p.Bucket, &p.Key, &p.Submitted, &p.Delivered, &p.Failed); err != nil {
			return nil, fmt.Errorf("clickhouse: scan traffic: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, metricsErr("traffic", err)
	}
	if len(out) > maxRows {
		return nil, fmt.Errorf("clickhouse: traffic holds more than %d points: %w", maxRows, errs.ErrServiceUnavailable)
	}
	return out, nil
}

func metricsContext(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"max_execution_time": metricsMaxExecutionSeconds}))
}

func metricsErr(op string, err error) error {
	var ex *clickhouse.Exception
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ex) && ex.Code == clickhouseTimeoutExceeded) {
		return fmt.Errorf("clickhouse: metrics %s: %w", op, errs.ErrServiceUnavailable)
	}
	return fmt.Errorf("clickhouse: metrics %s: %w", op, err)
}
