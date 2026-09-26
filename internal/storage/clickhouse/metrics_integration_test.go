package clickhouse_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// metricsWindow is a window no other test writes into. The metrics read every customer of a window and the
// container is shared, so a window near now would count the rows of whichever test ran alongside. The
// future keeps the rows clear of the 90-day TTL.
func metricsWindow() (from, to time.Time) {
	from = time.Now().UTC().Truncate(time.Hour).Add(time.Duration(24+rand.IntN(24*365)) * time.Hour)
	return from, from.Add(time.Hour)
}

type metricsFixture struct {
	writer *clickhouse.CDRWriter
	rows   []clickhouse.CDRRow
}

// message seeds a message's pre-dispatch row, then one segment row per later status, each written with
// its own version: the read must count the message once, in its last state.
func (f *metricsFixture) message(customerID uuid.UUID, dir clickhouse.Direction, at time.Time, connectorID *uuid.UUID, latencyMs uint32, statuses ...clickhouse.Status) uuid.UUID {
	id := uuid.New()
	base := clickhouse.CDRRow{
		MessageID: id, TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: customerID, Direction: dir,
		SourceAddr: "GATEWAY", DestAddr: searchMSISDN(), SubmittedAt: at, Status: clickhouse.StatusAccepted,
		SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
	}
	if dir == clickhouse.DirectionMT {
		f.rows = append(f.rows, base)
	}
	for _, st := range statuses {
		row := base
		row.Status = st
		if st != clickhouse.StatusRejected {
			row.SegmentSeq = 1
			row.ConnectorID = connectorID
		}
		if st == clickhouse.StatusDelivered {
			delivered := at.Add(time.Duration(latencyMs) * time.Millisecond)
			row.DeliveredAt = &delivered
			row.LatencyMs = &latencyMs
		}
		f.rows = append(f.rows, row)
	}
	return id
}

func (f *metricsFixture) flush(t *testing.T) {
	t.Helper()
	if err := f.writer.InsertBatch(context.Background(), f.rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	f.rows = nil
}

func TestMetricsSummaryCountsEachMessageOnceInItsFinalState(t *testing.T) {
	writer, reader := searchReader(t)
	ctx := context.Background()
	from, to := metricsWindow()
	customer, connector := uuid.New(), uuid.New()
	at := from.Add(time.Minute)
	f := &metricsFixture{writer: writer}

	f.message(customer, clickhouse.DirectionMT, at, &connector, 100, clickhouse.StatusEnroute, clickhouse.StatusDelivered)
	f.message(customer, clickhouse.DirectionMT, at, &connector, 300, clickhouse.StatusEnroute, clickhouse.StatusDelivered)
	f.message(customer, clickhouse.DirectionMT, at, &connector, 0, clickhouse.StatusEnroute, clickhouse.StatusFailed)
	f.message(customer, clickhouse.DirectionMT, at, &connector, 0, clickhouse.StatusEnroute, clickhouse.StatusExpired)
	f.message(customer, clickhouse.DirectionMT, at, nil, 0, clickhouse.StatusRejected)
	inFlight := f.message(customer, clickhouse.DirectionMT, at, &connector, 0, clickhouse.StatusEnroute)
	f.message(customer, clickhouse.DirectionMO, at, nil, 0, clickhouse.StatusDelivered)
	f.message(customer, clickhouse.DirectionMT, to, &connector, 0, clickhouse.StatusEnroute, clickhouse.StatusDelivered)
	f.message(customer, clickhouse.DirectionMT, from.Add(-time.Millisecond), &connector, 0, clickhouse.StatusDelivered)
	f.flush(t)

	got, err := reader.MetricsSummary(ctx, from, to)
	if err != nil {
		t.Fatalf("MetricsSummary: %v", err)
	}
	want := clickhouse.MetricsCounts{Submitted: 6, Delivered: 2, Failed: 2, Rejected: 1, MOReceived: 1}
	if got.Submitted != want.Submitted || got.Delivered != want.Delivered || got.Failed != want.Failed ||
		got.Rejected != want.Rejected || got.MOReceived != want.MOReceived {
		t.Fatalf("counts = %+v, want %+v", got, want)
	}
	if got.E2EP50 == nil || got.E2EP99 == nil || *got.E2EP50 < 100 || *got.E2EP50 > 300 || *got.E2EP99 < *got.E2EP50 {
		t.Fatalf("e2e p50=%v p99=%v, want within the two delivered latencies (100, 300)", got.E2EP50, got.E2EP99)
	}

	// A DLR landing later moves the message, without rewriting its earlier rows.
	f.rows = append(f.rows, clickhouse.CDRRow{
		MessageID: inFlight, TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: customer,
		Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: searchMSISDN(), SubmittedAt: at,
		Status: clickhouse.StatusDelivered, SegmentSeq: 1, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
	})
	f.flush(t)
	if got, err = reader.MetricsSummary(ctx, from, to); err != nil || got.Delivered != 3 || got.Submitted != 6 {
		t.Fatalf("after the DLR: delivered=%d submitted=%d err=%v, want 3 and 6", got.Delivered, got.Submitted, err)
	}
}

func TestMetricsSummaryOfAnEmptyWindowHasNoLatency(t *testing.T) {
	_, reader := searchReader(t)
	from, to := metricsWindow()
	got, err := reader.MetricsSummary(context.Background(), from, to)
	if err != nil {
		t.Fatalf("MetricsSummary: %v", err)
	}
	if got.Submitted != 0 || got.E2EP50 != nil || got.E2EP99 != nil {
		t.Fatalf("empty window = %+v, want zero counts and null latencies", got)
	}
}

func TestTrafficBucketsDispatchedMessagesByConnector(t *testing.T) {
	writer, reader := searchReader(t)
	ctx := context.Background()
	from, to := metricsWindow()
	customer, a, b := uuid.New(), uuid.New(), uuid.New()
	f := &metricsFixture{writer: writer}

	f.message(customer, clickhouse.DirectionMT, from.Add(5*time.Second), &a, 10, clickhouse.StatusEnroute, clickhouse.StatusDelivered)
	f.message(customer, clickhouse.DirectionMT, from.Add(50*time.Second), &a, 0, clickhouse.StatusEnroute, clickhouse.StatusFailed)
	f.message(customer, clickhouse.DirectionMT, from.Add(70*time.Second), &a, 0, clickhouse.StatusEnroute)
	f.message(customer, clickhouse.DirectionMT, from.Add(70*time.Second), &b, 0, clickhouse.StatusEnroute, clickhouse.StatusExpired)
	f.message(customer, clickhouse.DirectionMT, from.Add(70*time.Second), nil, 0, clickhouse.StatusRejected)
	f.message(customer, clickhouse.DirectionMO, from.Add(70*time.Second), &a, 0, clickhouse.StatusDelivered)
	f.flush(t)

	points, err := reader.Traffic(ctx, from, to, time.Minute, clickhouse.TrafficByConnector, 100)
	if err != nil {
		t.Fatalf("Traffic: %v", err)
	}
	want := map[[2]string]clickhouse.TrafficPoint{
		{from.String(), a.String()}:                  {Submitted: 2, Delivered: 1, Failed: 1},
		{from.Add(time.Minute).String(), a.String()}: {Submitted: 1},
		{from.Add(time.Minute).String(), b.String()}: {Submitted: 1, Failed: 1},
	}
	if len(points) != len(want) {
		t.Fatalf("points = %+v, want %d (the rejected MT and the MO in none)", points, len(want))
	}
	for _, p := range points {
		w, ok := want[[2]string{p.Bucket.UTC().String(), p.Key.String()}]
		if !ok || p.Submitted != w.Submitted || p.Delivered != w.Delivered || p.Failed != w.Failed {
			t.Errorf("point %s %s = %d/%d/%d, want %+v (present=%v)", p.Bucket.UTC(), p.Key, p.Submitted, p.Delivered, p.Failed, w, ok)
		}
	}
}

func TestTrafficByCustomerKeepsTheRejected(t *testing.T) {
	writer, reader := searchReader(t)
	from, to := metricsWindow()
	customer := uuid.New()
	f := &metricsFixture{writer: writer}
	f.message(customer, clickhouse.DirectionMT, from, nil, 0, clickhouse.StatusRejected)
	f.flush(t)

	points, err := reader.Traffic(context.Background(), from, to, time.Minute, clickhouse.TrafficByCustomer, 100)
	if err != nil {
		t.Fatalf("Traffic: %v", err)
	}
	if len(points) != 1 || points[0].Key != customer || points[0].Submitted != 1 {
		t.Fatalf("points = %+v, want one submission for %s", points, customer)
	}
}

func TestTrafficRefusesMoreRowsThanItMayReturn(t *testing.T) {
	writer, reader := searchReader(t)
	from, to := metricsWindow()
	f := &metricsFixture{writer: writer}
	f.message(uuid.New(), clickhouse.DirectionMT, from, nil, 0)
	f.message(uuid.New(), clickhouse.DirectionMT, from, nil, 0)
	f.flush(t)

	if _, err := reader.Traffic(context.Background(), from, to, time.Minute, clickhouse.TrafficByCustomer, 1); !errors.Is(err, errs.ErrServiceUnavailable) {
		t.Fatalf("err = %v, want ErrServiceUnavailable rather than a silently truncated series", err)
	}
}
