package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// TraceStore reads a message's lifecycle. Declared consumer-side.
type TraceStore interface {
	ByMessageID(ctx context.Context, messageID uuid.UUID) (clickhouse.CDRRow, bool, error)
	Timeline(ctx context.Context, messageID uuid.UUID) ([]clickhouse.CDRMilestone, error)
}

type traceSpanDTO struct {
	Name       string         `json:"name"`
	Start      time.Time      `json:"start"`
	End        *time.Time     `json:"end" nullable:"true" required:"false"`
	DurationMS *float64       `json:"duration_ms" nullable:"true" required:"false"`
	Status     string         `json:"status" enum:"ok,error"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type messageTraceDTO struct {
	MessageID string         `json:"message_id" format:"uuid"`
	TraceID   string         `json:"trace_id" format:"uuid"`
	Spans     []traceSpanDTO `json:"spans"`
}

type getMessageTraceInput struct {
	ID string `path:"id" format:"uuid"`
}

type getMessageTraceOutput struct {
	Body messageTraceDTO
}

func registerMessageTrace(api huma.API, store TraceStore) {
	h := &traceHandler{store: store}
	register(api, huma.Operation{
		OperationID: "get-message-trace",
		Method:      http.MethodGet,
		Path:        "/admin/messages/{id}/trace",
		Summary:     "Span timeline for a message (never carries the body)",
		Tags:        []string{"Messages"},
		Security:    scopeSecurity(auth.ScopeAdminRead),
		Errors:      []int{http.StatusNotFound},
	}, h.get)
}

type traceHandler struct{ store TraceStore }

// get assembles the trace from the CDR's versioned rows.
//
// There is no span store to query: spans leave the process for a collector. The CDR keeps one row per
// lifecycle stage (§1.10), which is the durable timeline; trace_id is returned so an operator can pivot to
// their tracing backend for span-level detail.
func (h *traceHandler) get(ctx context.Context, in *getMessageTraceInput) (*getMessageTraceOutput, error) {
	if h.store == nil {
		return nil, humaerr.FromError(errs.ErrNotFound)
	}
	messageID := uuid.MustParse(in.ID) // huma rejects a malformed uuid before the handler runs

	row, found, err := h.store.ByMessageID(ctx, messageID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, humaerr.FromError(errs.ErrNotFound)
	}
	milestones, err := h.store.Timeline(ctx, messageID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	reveal := mayRevealMSISDN(ctx)
	spans := make([]traceSpanDTO, 0, len(milestones))
	for i, m := range milestones {
		span := traceSpanDTO{
			Name:   "cdr." + string(m.Status),
			Start:  m.At,
			Status: spanStatus(m),
		}
		// latency_ms is END-TO-END (submit → outcome), so it belongs to a span that starts at submission.
		// Hanging it off the outcome's own timestamp would end the span in the future.
		if m.LatencyMS != nil {
			ms := float64(*m.LatencyMS)
			end := m.At
			span.Start = row.SubmittedAt
			span.End = &end
			span.DurationMS = &ms
		}
		// Message-level attributes once, on the first span: repeating them on every stage is most of the
		// payload on a multipart message.
		if i == 0 {
			span.Attributes = messageAttributes(row, reveal)
		}
		if stage := stageAttributes(m); len(stage) > 0 {
			if span.Attributes == nil {
				span.Attributes = stage
			} else {
				for k, v := range stage {
					span.Attributes[k] = v
				}
			}
		}
		spans = append(spans, span)
	}
	return &getMessageTraceOutput{Body: messageTraceDTO{
		MessageID: row.MessageID.String(),
		TraceID:   row.TraceID.String(),
		Spans:     spans,
	}}, nil
}

func spanStatus(m clickhouse.CDRMilestone) string {
	switch m.Status {
	case clickhouse.StatusFailed, clickhouse.StatusRejected, clickhouse.StatusExpired:
		return "error"
	default:
		return "ok"
	}
}

// messageAttributes carries identifiers only. No content column is ever selected, so no body can reach here;
// the subscriber address is masked unless the caller holds the reveal scope.
func messageAttributes(row clickhouse.CDRRow, reveal bool) map[string]any {
	source, dest := maskAddresses(string(row.Direction), row.SourceAddr, row.DestAddr, reveal)
	return map[string]any{
		"account_id":  row.AccountID.String(),
		"customer_id": row.CustomerID.String(),
		"direction":   string(row.Direction),
		"source_addr": source,
		"dest_addr":   dest,
	}
}

// stageAttributes carries what is specific to one lifecycle stage.
func stageAttributes(m clickhouse.CDRMilestone) map[string]any {
	attrs := map[string]any{}
	if m.SegmentSeq > 0 {
		attrs["segment_seq"] = m.SegmentSeq
	}
	if m.ErrorCode != nil {
		attrs["code"] = *m.ErrorCode
	}
	if m.ConnectorID != nil {
		attrs["connector_id"] = m.ConnectorID.String()
	}
	return attrs
}
