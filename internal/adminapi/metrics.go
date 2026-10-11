package adminapi

import (
	"cmp"
	"context"
	"log/slog"
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

// metricsWindows are the only windows a read may cover; 24h awaits a pre-aggregate
// (debts/metriques-24h-sans-pre-agregat.md).
var metricsWindows = map[string]struct{ span, step time.Duration }{
	"5m":  {5 * time.Minute, 10 * time.Second},
	"15m": {15 * time.Minute, 30 * time.Second},
	"30m": {30 * time.Minute, time.Minute},
	"1h":  {time.Hour, time.Minute},
}

const (
	trafficTopSeries = 20
	trafficMaxRows   = 100_000
	trafficOtherKey  = "other"
	// trafficUnknownChannel names the MT no client submitted, or written before the CDR recorded the channel.
	trafficUnknownChannel = "unknown"
)

type metricsSummaryInput struct {
	Window string `query:"window" default:"5m" doc:"One of 5m, 15m, 30m, 1h."`
}

type metricsSummaryDTO struct {
	Window         string   `json:"window"`
	Submitted      int64    `json:"submitted" required:"false"`
	Delivered      int64    `json:"delivered" required:"false"`
	Failed         int64    `json:"failed" required:"false"`
	Rejected       int64    `json:"rejected" required:"false"`
	MOReceived     int64    `json:"mo_received" required:"false"`
	IngestP50      *float64 `json:"ingest_latency_ms_p50" nullable:"true" required:"false"`
	IngestP99      *float64 `json:"ingest_latency_ms_p99" nullable:"true" required:"false"`
	E2EP50         *float64 `json:"e2e_latency_ms_p50" nullable:"true" required:"false"`
	E2EP99         *float64 `json:"e2e_latency_ms_p99" nullable:"true" required:"false"`
	ActiveSessions *int     `json:"active_sessions,omitempty"`
}

type metricsSummaryOutput struct{ Body metricsSummaryDTO }

type trafficInput struct {
	GroupBy string `query:"groupBy" enum:"connector,customer,group,channel" default:"connector"`
	Window  string `query:"window" default:"1h" doc:"One of 5m, 15m, 30m, 1h."`
}

type trafficPointDTO struct {
	T          time.Time `json:"t"`
	Submitted  int64     `json:"submitted" required:"false"`
	Delivered  int64     `json:"delivered" required:"false"`
	Failed     int64     `json:"failed" required:"false"`
	MOReceived int64     `json:"mo_received" required:"false"`
}

type trafficSeriesDTO struct {
	Key    string            `json:"key"`
	Points []trafficPointDTO `json:"points" nullable:"false"`
}

type trafficDTO struct {
	GroupBy string             `json:"group_by" enum:"connector,customer,group,channel"`
	Window  string             `json:"window"`
	Series  []trafficSeriesDTO `json:"series" nullable:"false"`
}

type trafficOutput struct{ Body trafficDTO }

func registerMetrics(api huma.API, reader MetricsReader, customers CustomerStore, sessions SessionDirectory, logger *slog.Logger) {
	h := &metricsHandlers{reader: reader, customers: customers, sessions: sessions, logger: logger}
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
	sessions  SessionDirectory
	logger    *slog.Logger
}

func windowOf(name string) (from, to time.Time, step time.Duration, err error) {
	w, ok := metricsWindows[name]
	if !ok {
		return from, to, 0, humaerr.FailValidation("unsupported window",
			humaerr.FieldError{Field: "window", Message: "must be one of 5m, 15m, 30m, 1h"})
	}
	to = time.Now().UTC().Truncate(w.step)
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
		ActiveSessions: h.activeSessions(ctx),
	}}, nil
}

// activeSessions is nil when the registry cannot be read: a 0 would read "no client connected".
func (h *metricsHandlers) activeSessions(ctx context.Context) *int {
	if h.sessions == nil {
		return nil
	}
	n, err := h.sessions.CountSessions(ctx)
	if err != nil {
		h.logger.WarnContext(ctx, "metrics summary: session registry unreadable, active_sessions omitted", "err", err)
		return nil
	}
	return &n
}

func (h *metricsHandlers) traffic(ctx context.Context, in *trafficInput) (*trafficOutput, error) {
	from, to, step, err := windowOf(in.Window)
	if err != nil {
		return nil, err
	}
	dim := clickhouse.TrafficDimension(in.GroupBy)
	if in.GroupBy == "group" {
		dim = clickhouse.TrafficByCustomer
	}
	points, err := h.reader.Traffic(ctx, from, to, step, dim, trafficMaxRows)
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	keyOf := func(key string) string { return key }
	switch in.GroupBy {
	case string(clickhouse.TrafficByChannel):
		keyOf = func(channel string) string { return cmp.Or(channel, trafficUnknownChannel) }
	case "group":
		groups, err := h.groupOfEachCustomer(ctx)
		if err != nil {
			return nil, humaerr.FromError(err)
		}
		keyOf = func(customer string) string {
			if g, ok := groups[customer]; ok {
				return g.String()
			}
			return "ungrouped"
		}
	}
	return &trafficOutput{Body: trafficDTO{GroupBy: in.GroupBy, Window: in.Window, Series: foldSeries(points, keyOf)}}, nil
}

// groupOfEachCustomer reads the membership as it is NOW: the CDR carries no group, so a customer moved to
// another group takes its past traffic with it.
func (h *metricsHandlers) groupOfEachCustomer(ctx context.Context) (map[string]uuid.UUID, error) {
	out := map[string]uuid.UUID{}
	f := cp.CustomerFilter{Limit: 500}
	for {
		page, err := h.customers.List(ctx, f)
		if err != nil {
			return nil, err
		}
		for _, c := range page.Items {
			if c.GroupID != nil {
				out[c.ID.String()] = *c.GroupID
			}
		}
		if !page.HasMore {
			return out, nil
		}
		f.After = page.Items[len(page.Items)-1].ID
	}
}

// foldSeries groups the points by key, keeps the trafficTopSeries keys with the most messages, MT and MO,
// and sums the rest under trafficOtherKey, so the series still add up to the whole traffic.
func foldSeries(points []clickhouse.TrafficPoint, keyOf func(string) string) []trafficSeriesDTO {
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
		b.MOReceived += p.MOReceived
		totals[key] += p.Submitted + p.MOReceived
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
				add(trafficOtherKey, clickhouse.TrafficPoint{Bucket: b.T, Submitted: b.Submitted, Delivered: b.Delivered, Failed: b.Failed, MOReceived: b.MOReceived})
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
