// Package connectorpool is the outbound SMSC leg (M2): a single SMPP bind to one connector. It
// consumes mt.routed, submits each message with submit_sm, and records the outcome in the CDR
// (enroute on ESME_ROK, failed otherwise). M2 has one bind, no circuit breaker, no fallback and no
// reroute — those are later milestones.
package connectorpool

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Consumer reads mt.routed. *kafka.Consumer satisfies it.
type Consumer interface {
	Run(ctx context.Context, handle kafka.Handler) error
}

// CDRWriter records the send outcome. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// Deps are the connector pool's collaborators.
type Deps struct {
	Consumer Consumer
	CDR      CDRWriter
	Bind     BindConfig
	Tracer   trace.Tracer
	Logger   *slog.Logger
}

// Service is the connector pool.
type Service struct {
	deps Deps
}

// New builds a Service. A nil logger defaults to slog.Default.
func New(deps Deps) *Service {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &Service{deps: deps}
}

// Run binds to the SMSC, then consumes mt.routed until ctx is cancelled, unbinding cleanly on exit.
// A failure to bind returns an error (the service restarts); a per-message infrastructure failure
// leaves the offset uncommitted for reprocessing.
func (s *Service) Run(ctx context.Context) error {
	b, err := dialAndBind(ctx, s.deps.Bind, s.deps.Logger)
	if err != nil {
		return err
	}
	// Close detaches from ctx on purpose: the unbind must be sent AFTER ctx is cancelled (that is
	// what triggers the drain), on its own bounded context, exactly like observability's tracing
	// drain.
	//nolint:contextcheck // deliberate detach for the shutdown unbind
	defer b.Close()

	return s.deps.Consumer.Run(ctx, s.handler(b))
}

func (s *Service) handler(b *bind) kafka.Handler {
	return func(ctx context.Context, rec kafka.Record) error {
		ctx, span := s.deps.Tracer.Start(ctx, "connector.submit")
		defer span.End()

		routed, err := pipeline.DecodeRouted(rec)
		if err != nil {
			return fmt.Errorf("connectorpool: decode mt.routed: %w", err)
		}

		resp, err := b.Submit(ctx, buildSubmit(routed))
		if err != nil {
			// A dead bind, a write failure or a timeout is transient: do not commit, so the message is
			// reprocessed after a restart. At-least-once means the SMSC may see a duplicate submit; the
			// versioned CDR collapses the duplicate enroute rows (no dedup until M3).
			return fmt.Errorf("connectorpool: submit_sm: %w", err)
		}

		if err := s.deps.CDR.Insert(ctx, cdrRow(routed, resp)); err != nil {
			return fmt.Errorf("connectorpool: write cdr: %w", err)
		}
		return nil
	}
}

// buildSubmit maps a routed message onto a submit_sm. Revealing the body here is an audited egress
// (like the Kafka payload): the plaintext goes onto the SMSC wire, never into a log or span. A body
// larger than a single short_message travels in the message_payload TLV — M2 does not segment.
func buildSubmit(r pipeline.RoutedMT) *smpp.SubmitSM {
	sm := &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddr:      r.From,
		DestinationAddr: r.To,
		DataCoding:      dataCoding(r.Encoding),
	}}
	sourceTON, sourceNPI := addrTypeOf(r.From)
	sm.SourceAddrTON, sm.SourceAddrNPI = sourceTON, sourceNPI
	sm.DestAddrTON, sm.DestAddrNPI = smpp.TONInternational, smpp.NPIISDN
	if r.RegisteredDelivery {
		sm.RegisteredDelivery = smpp.RegisteredDeliveryReceipt
	}

	body := r.Body.Reveal() // audited: body -> SMSC wire, never logged
	if len(body) > 254 {
		sm.TLVs.Set(smpp.TagMessagePayload, body)
	} else {
		sm.ShortMessage = body
	}
	return sm
}

// cdrRow builds the enroute (or failed) CDR row from the submit_sm_resp.
func cdrRow(r pipeline.RoutedMT, resp smpp.PDU) clickhouse.CDRRow {
	connectorID := r.ConnectorID
	status, errorCode := outcome(resp.Status)
	return clickhouse.CDRRow{
		MessageID:    r.MessageID,
		TraceID:      r.TraceID,
		AccountID:    r.AccountID,
		CustomerID:   r.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   r.From,
		DestAddr:     r.To,
		ConnectorID:  &connectorID,
		RouteID:      r.RouteID,
		SubmittedAt:  r.SubmittedAt,
		Status:       status,
		ErrorCode:    errorCode,
		SegmentCount: segmentCount(r.SegmentCount),
		Encoding:     mapEncoding(r.Encoding),
		Billed:       false,
	}
}

// outcome maps a submit_sm_resp command_status to a CDR status. ESME_ROK is enroute; anything else
// is a failed send, with an error_code derived from the SMPP status.
func outcome(cmdStatus uint32) (clickhouse.Status, *string) {
	if cmdStatus == smpp.StatusOK {
		return clickhouse.StatusEnroute, nil
	}
	code := smppErrorCode(cmdStatus)
	return clickhouse.StatusFailed, &code
}

func smppErrorCode(status uint32) string {
	switch status {
	case errs.StatusThrottled:
		return string(errs.ErrRateLimited)
	case errs.StatusSysErr:
		return string(errs.ErrInternal)
	case errs.StatusSubmitFail:
		return "submit_failed"
	default:
		return fmt.Sprintf("smpp_status_0x%08x", status)
	}
}

func dataCoding(encoding string) uint8 {
	switch encoding {
	case "ucs2":
		return smpp.DataCodingUCS2
	case "binary":
		return smpp.DataCodingBinary
	default:
		return smpp.DataCodingGSM7
	}
}

func mapEncoding(encoding string) clickhouse.Encoding {
	switch encoding {
	case "ucs2":
		return clickhouse.EncodingUCS2
	case "binary":
		return clickhouse.EncodingBinary
	default:
		return clickhouse.EncodingGSM7
	}
}

// addrTypeOf picks the TON/NPI for a source address: an all-digit sender is treated as an
// international MSISDN, anything else as an alphanumeric sender id.
func addrTypeOf(addr string) (ton, npi uint8) {
	for _, r := range addr {
		if r < '0' || r > '9' {
			return smpp.TONAlphanumeric, smpp.NPIUnknown
		}
	}
	return smpp.TONInternational, smpp.NPIISDN
}

func segmentCount(n int) uint16 {
	if n < 1 {
		return 1
	}
	return uint16(n) //nolint:gosec // segment count is a small positive integer
}
