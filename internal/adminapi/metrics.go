package adminapi

import (
	"cmp"
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// MetricsReader aggregates the CDR for the metrics pull. Declared consumer-side.
type MetricsReader interface {
	MetricsSummary(ctx context.Context, from, to time.Time) (clickhouse.MetricsCounts, error)
	Traffic(ctx context.Context, from, to time.Time, step time.Duration, dim clickhouse.TrafficDimension, maxRows int) ([]clickhouse.TrafficPoint, error)
}

// metricsWindows are the only windows a read may cover. 24h is not one: at the target throughput it is
// ~690 M messages to scan per request, and the spec (§6.3) wants it served from a pre-aggregate that does
// not exist yet.
var metricsWindows = map[string]struct{ span, step time.Duration }{
	"5m": {5 * time.Minute, 10 * time.Second},
	"1h": {time.Hour, time.Minute},
}

const (
	trafficTopSeries = 20
	trafficMaxRows   = 100_000
	trafficOtherKey  = "other"
	trafficNoGroup   = "ungrouped"
	membershipPage   = 500
)

type metricsSummaryInput struct {
	Window string `query:"window" default:"5m" doc:"One of 5m, 1h."`
}

type metricsSummaryDTO struct {
	Window         string   `json:"window"`
	Submitted      int64    `json:"submitted" required:"false"`
	Delivered      int64    `json:"delivered" required:"false"`
	Failed         int64    `json:"failed" required:"false"`
	Rejected       int64    `json:"rejected" required:"false"`
	MOReceived     int64    `json:"mo_received" required:"false"`
	IngestP50      *float64 `json:"ingest_latency_ms_p50,omitempty" nullable:"true"`
	IngestP99      *float64 `json:"ingest_latency_ms_p99,omitempty" nullable:"true"`
	E2EP50         *float64 `json:"e2e_latency_ms_p50,omitempty" nullable:"true"`
	E2EP99         *float64 `json:"e2e_latency_ms_p99,omitempty" nullable:"true"`
	ActiveSessions *int     `json:"active_sessions,omitempty"`
}

type metricsSummaryOutput struct{ Body metricsSummaryDTO }

type trafficInput struct {
	GroupBy string `query:"groupBy" enum:"connector,customer,group" default:"connector"`
	Window  string `query:"window" default:"1h" doc:"One of 5m, 1h."`
}

type trafficPointDTO struct {
	T         time.Time `json:"t"`
	Submitted int64     `json:"submitted" required:"false"`
	Delivered int64     `json:"delivered" required:"false"`
	Failed    int64     `json:"failed" required:"false"`
}

type trafficSeriesDTO struct {
	Key    string            `json:"key"`
	Points []trafficPointDTO `json:"points" nullable:"false"`
}

type trafficDTO struct {
	GroupBy string             `json:"group_by" enum:"connector,customer,group"`
	Window  string             `json:"window"`
	Series  []trafficSeriesDTO `json:"series" nullable:"false"`
}

type trafficOutput struct{ Body trafficDTO }

func registerMetrics(api huma.API, reader MetricsReader, customers CustomerStore) {
	h := &metricsHandlers{reader: reader, customers: customers}
	errors := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusServiceUnavailable}

	register(api, huma.Operation{
		OperationID: "get-metrics-summary", Method: http.MethodGet, Path: "/admin/metrics/summary",
		Summary: "Rolling summary counters", Tags: []string{"Metrics"},
		Security: scopeSecurity(auth.ScopeAdminRead), Errors: errors,
	}, h.summary)

	register(api, huma.Operation{
		OperationID: "get-traffic-metrics", Method: http.MethodGet, Path: "/admin/metrics/traffic",
		Summary: "Traffic time series grouped by dimension", Tags: []string{"Metrics"},
		Security: scopeSecurity(auth.ScopeAdminRead), Errors: errors,
	}, h.traffic)
}

type metricsHandlers struct {
	reader    MetricsReader
	customers CustomerStore
}

func windowOf(name string) (from, to time.Time, step time.Duration, err error) {
	w, ok := metricsWindows[name]
	if !ok {
		return from, to, 0, humaerr.FailValidation("unsupported window",
			humaerr.FieldError{Field: "window", Message: "must be one of 5m, 1h"})
	}
	to = time.Now().UTC()
	return to.Add(-w.span), to, w.step, nil
}

func (h *metricsHandlers) summary(ctx context.Context, in *metricsSummaryInput) (*metricsSummaryOutput, error) {
	from, to, _, err := windowOf(in.Window)
	if err != nil {
		return nil, err
	}
	c, err := h.reader.MetricsSummary(ctx, from, to)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &metricsSummaryOutput{Body: metricsSummaryDTO{
		Window: in.Window, Submitted: c.Submitted, Delivered: c.Delivered, Failed: c.Failed,
		Rejected: c.Rejected, MOReceived: c.MOReceived, E2EP50: c.E2EP50, E2EP99: c.E2EP99,
	}}, nil
}

func (h *metricsHandlers) traffic(ctx context.Context, in *trafficInput) (*trafficOutput, error) {
	from, to, step, err := windowOf(in.Window)
	if err != nil {
		return nil, err
	}
	dim := clickhouse.TrafficByConnector
	if in.GroupBy != string(clickhouse.TrafficByConnector) {
		dim = clickhouse.TrafficByCustomer
	}
	points, err := h.reader.Traffic(ctx, from, to, step, dim, trafficMaxRows)
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	keyOf := func(id uuid.UUID) string { return id.String() }
	if in.GroupBy == "group" {
		groups, err := h.groupOfEachCustomer(ctx)
		if err != nil {
			return nil, humaerr.FromError(err)
		}
		keyOf = func(id uuid.UUID) string {
			if g, ok := groups[id]; ok {
				return g.String()
			}
			return trafficNoGroup
		}
	}
	return &trafficOutput{Body: trafficDTO{GroupBy: in.GroupBy, Window: in.Window, Series: foldSeries(points, keyOf)}}, nil
}

// groupOfEachCustomer reads the membership as it is NOW: the CDR carries no group, so a customer moved to
// another group takes its past traffic with it.
func (h *metricsHandlers) groupOfEachCustomer(ctx context.Context) (map[uuid.UUID]uuid.UUID, error) {
	out := map[uuid.UUID]uuid.UUID{}
	f := cp.CustomerFilter{Limit: membershipPage}
	for {
		page, err := h.customers.List(ctx, f)
		if err != nil {
			return nil, err
		}
		for _, c := range page.Items {
			if c.GroupID != nil {
				out[c.ID] = *c.GroupID
			}
		}
		if !page.HasMore {
			return out, nil
		}
		f.After = page.Items[len(page.Items)-1].ID
	}
}

// foldSeries groups the points by key, keeps the trafficTopSeries keys with the most submissions and sums
// the rest under trafficOtherKey, so the series still add up to the whole traffic.
func foldSeries(points []clickhouse.TrafficPoint, keyOf func(uuid.UUID) string) []trafficSeriesDTO {
	byKey := map[string]map[time.Time]*trafficPointDTO{}
	totals := map[string]int64{}
	add := func(key string, p clickhouse.TrafficPoint) {
		buckets := byKey[key]
		if buckets == nil {
			buckets = map[time.Time]*trafficPointDTO{}
			byKey[key] = buckets
		}
		b := buckets[p.Bucket]
		if b == nil {
			b = &trafficPointDTO{T: p.Bucket}
			buckets[p.Bucket] = b
		}
		b.Submitted += p.Submitted
		b.Delivered += p.Delivered
		b.Failed += p.Failed
		totals[key] += p.Submitted
	}
	for _, p := range points {
		add(keyOf(p.Key), p)
	}

	keys := make([]string, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int { return cmp.Or(cmp.Compare(totals[b], totals[a]), cmp.Compare(a, b)) })
	if len(keys) > trafficTopSeries {
		for _, k := range keys[trafficTopSeries:] {
			for _, b := range byKey[k] {
				add(trafficOtherKey, clickhouse.TrafficPoint{Bucket: b.T, Submitted: b.Submitted, Delivered: b.Delivered, Failed: b.Failed})
			}
		}
		keys = append(keys[:trafficTopSeries], trafficOtherKey)
	}

	series := make([]trafficSeriesDTO, 0, len(keys))
	for _, k := range keys {
		pts := make([]trafficPointDTO, 0, len(byKey[k]))
		for _, b := range byKey[k] {
			pts = append(pts, *b)
		}
		slices.SortFunc(pts, func(a, b trafficPointDTO) int { return a.T.Compare(b.T) })
		series = append(series, trafficSeriesDTO{Key: k, Points: pts})
	}
	return series
}
