package adminapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

type fakeMetrics struct {
	counts    clickhouse.MetricsCounts
	points    []clickhouse.TrafficPoint
	err       error
	from, to  time.Time
	step      time.Duration
	dimension clickhouse.TrafficDimension
}

func (f *fakeMetrics) MetricsSummary(_ context.Context, from, to time.Time) (clickhouse.MetricsCounts, error) {
	f.from, f.to = from, to
	return f.counts, f.err
}

func (f *fakeMetrics) Traffic(_ context.Context, from, to time.Time, step time.Duration, dim clickhouse.TrafficDimension, _ int) ([]clickhouse.TrafficPoint, error) {
	f.from, f.to, f.step, f.dimension = from, to, step, dim
	return f.points, f.err
}

func getMetrics(t *testing.T, api http.Handler, path string, into any) int {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/metrics/"+path, ""))
	if into != nil {
		_ = json.Unmarshal(w.Body.Bytes(), into)
	}
	return w.Code
}

func TestMetricsSummaryReadsTheLastFiveMinutesByDefault(t *testing.T) {
	p50, p99 := 120.0, 900.0
	fake := &fakeMetrics{counts: clickhouse.MetricsCounts{Submitted: 9, Delivered: 5, Failed: 2, Rejected: 1, MOReceived: 4, E2EP50: &p50, E2EP99: &p99}}
	api := newTestAPIWithScopes(t, adminapi.Deps{Metrics: fake}, "admin:read")

	var body map[string]any
	if code := getMetrics(t, api, "summary", &body); code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a read-only operator", code)
	}
	if got := fake.to.Sub(fake.from); got != 5*time.Minute || !fake.to.Equal(fake.to.Truncate(10*time.Second)) {
		t.Fatalf("read [%v, %v), want 5m ending on a 10s bucket boundary", fake.from, fake.to)
	}
	want := map[string]any{"window": "5m", "submitted": 9.0, "delivered": 5.0, "failed": 2.0, "rejected": 1.0,
		"mo_received": 4.0, "e2e_latency_ms_p50": 120.0, "e2e_latency_ms_p99": 900.0}
	for k, v := range want {
		if body[k] != v {
			t.Errorf("%s = %v, want %v", k, body[k], v)
		}
	}
	for _, unmeasured := range []string{"ingest_latency_ms_p50", "ingest_latency_ms_p99"} {
		if v, ok := body[unmeasured]; !ok || v != nil {
			t.Errorf("%s = %v (present=%v), want null as the contract says", unmeasured, v, ok)
		}
	}
	if v, ok := body["active_sessions"]; ok {
		t.Errorf("active_sessions = %v, want absent: nothing counts it", v)
	}
}

func TestMetricsRefuseAWindowTheyCannotAfford(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Metrics: &fakeMetrics{}})
	for _, path := range []string{"summary?window=24h", "traffic?window=24h"} {
		var body struct {
			Errors []struct {
				Field string `json:"field"`
			} `json:"errors"`
		}
		code := getMetrics(t, api, path, &body)
		if code != http.StatusUnprocessableEntity || len(body.Errors) != 1 || body.Errors[0].Field != "window" {
			t.Errorf("%s: status = %d errors = %+v, want 422 naming window", path, code, body.Errors)
		}
	}
}

func TestMetricsDuringAnOverloadedReadAre503(t *testing.T) {
	api := newTestAPIWith(t, adminapi.Deps{Metrics: &fakeMetrics{err: fmt.Errorf("metrics: %w", errs.ErrServiceUnavailable)}})
	for _, path := range []string{"summary", "traffic"} {
		if code := getMetrics(t, api, path, nil); code != http.StatusServiceUnavailable {
			t.Errorf("%s: status = %d, want 503", path, code)
		}
	}
}

type trafficBody struct {
	GroupBy string `json:"group_by"`
	Window  string `json:"window"`
	Series  []struct {
		Key    string `json:"key"`
		Points []struct {
			T         time.Time `json:"t"`
			Submitted int       `json:"submitted"`
			Delivered int       `json:"delivered"`
			Failed    int       `json:"failed"`
		} `json:"points"`
	} `json:"series"`
}

func (b trafficBody) submittedBy() map[string]int {
	out := map[string]int{}
	for _, s := range b.Series {
		for _, p := range s.Points {
			out[s.Key] += p.Submitted
		}
	}
	return out
}

func TestTrafficBucketsAnHourByTheMinute(t *testing.T) {
	connector := uuid.New()
	bucket := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	fake := &fakeMetrics{}
	for _, m := range []int{4, 1, 3, 0, 2} {
		fake.points = append(fake.points, clickhouse.TrafficPoint{Bucket: bucket.Add(time.Duration(m) * time.Minute), Key: connector, Submitted: int64(m + 1)})
	}
	fake.points[3].Submitted, fake.points[3].Delivered, fake.points[3].Failed = 3, 2, 1
	api := newTestAPIWithScopes(t, adminapi.Deps{Metrics: fake}, "admin:read")

	var body trafficBody
	if code := getMetrics(t, api, "traffic", &body); code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if fake.to.Sub(fake.from) != time.Hour || fake.step != time.Minute || fake.dimension != clickhouse.TrafficByConnector {
		t.Fatalf("read = %v by %v per %s, want 1h by 1m per connector", fake.to.Sub(fake.from), fake.step, fake.dimension)
	}
	if body.GroupBy != "connector" || body.Window != "1h" || len(body.Series) != 1 || body.Series[0].Key != connector.String() {
		t.Fatalf("body = %+v, want one connector series over 1h", body)
	}
	p := body.Series[0].Points
	if len(p) != 5 || p[0].Submitted != 3 || p[0].Delivered != 2 || p[0].Failed != 1 {
		t.Fatalf("points = %+v, want five buckets, the first carrying its outcomes", p)
	}
	for i, pt := range p[1:] {
		if !pt.T.Equal(bucket.Add(time.Duration(i+1)*time.Minute)) || pt.Submitted != i+2 {
			t.Fatalf("points = %+v, want the buckets in time order", p)
		}
	}
}

func TestTrafficKeepsTheTopTwentyAndSumsTheRestUnderOther(t *testing.T) {
	bucket := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	fake := &fakeMetrics{}
	want := map[string]int{"other": 0}
	for i := range 23 {
		key := uuid.New()
		fake.points = append(fake.points, clickhouse.TrafficPoint{Bucket: bucket, Key: key, Submitted: int64(100 - i)})
		if i < 20 {
			want[key.String()] = 100 - i
		} else {
			want["other"] += 100 - i
		}
	}
	api := newTestAPIWith(t, adminapi.Deps{Metrics: fake})

	var body trafficBody
	getMetrics(t, api, "traffic?groupBy=customer&window=5m", &body)
	got := body.submittedBy()
	if len(body.Series) != 21 || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("series = %d %v, want the 20 largest and other=%d", len(body.Series), got, want["other"])
	}
	if fake.step != 10*time.Second || fake.dimension != clickhouse.TrafficByCustomer {
		t.Fatalf("read by %v per %s, want 10s per customer", fake.step, fake.dimension)
	}
}

func TestTrafficByGroupFollowsTheCurrentMembership(t *testing.T) {
	ctx := context.Background()
	customers := newFakeCustomerStore()
	groups := []uuid.UUID{uuid.New(), uuid.New()}
	var inFirst, loner cp.Customer
	// More customers than one page, so a fold that read only the first page would miss the last one.
	for i := range 501 {
		c, err := customers.Create(ctx, cp.NewCustomer{Name: fmt.Sprintf("c%d", i)})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if i == 0 {
			loner = c
			continue
		}
		if c, err = customers.SetGroup(ctx, c.ID, &groups[0]); err != nil {
			t.Fatalf("set group: %v", err)
		}
		inFirst = c
	}
	bucket := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	fake := &fakeMetrics{points: []clickhouse.TrafficPoint{
		{Bucket: bucket, Key: inFirst.ID, Submitted: 3},
		{Bucket: bucket, Key: loner.ID, Submitted: 2},
	}}
	api := newTestAPIWith(t, adminapi.Deps{Metrics: fake, Customers: customers})

	var body trafficBody
	getMetrics(t, api, "traffic?groupBy=group", &body)
	if got := body.submittedBy(); got[groups[0].String()] != 3 || got["ungrouped"] != 2 || len(got) != 2 {
		t.Fatalf("by group = %v, want %s=3 and ungrouped=2", got, groups[0])
	}
	if fake.dimension != clickhouse.TrafficByCustomer {
		t.Fatalf("read per %s, want per customer: the CDR carries no group", fake.dimension)
	}

	if _, err := customers.SetGroup(ctx, inFirst.ID, &groups[1]); err != nil {
		t.Fatalf("move: %v", err)
	}
	getMetrics(t, api, "traffic?groupBy=group", &body)
	if got := body.submittedBy(); got[groups[1].String()] != 3 || got[groups[0].String()] != 0 {
		t.Fatalf("after the move = %v, want the traffic under %s", got, groups[1])
	}
}
