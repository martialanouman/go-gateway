// Package connectorpool is the outbound SMSC leg (M2): a single SMPP bind to one connector. It
// consumes mt.routed, submits each message with submit_sm, and records the outcome in the CDR
// (enroute on ESME_ROK, failed otherwise). M2 has one bind, no circuit breaker, no fallback and no
// reroute — those are later milestones.
package connectorpool

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"unicode/utf16"

	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/encoding"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// errBindNotReady is reported by the readiness probe while the SMSC bind is not established.
var errBindNotReady = errors.New("connectorpool: smsc bind not established")

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

	// bound reports whether the SMSC bind is currently established. It gates the readiness probe, so
	// a bind that drops — including an idle-time drop no in-flight Submit would notice — takes the
	// pod out of rotation until Run re-dials.
	bound atomic.Bool
}

// New builds a Service. A nil logger defaults to slog.Default.
func New(deps Deps) *Service {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &Service{deps: deps}
}

// BindReady is the readiness probe for the SMSC bind: nil while the bind is established, an error
// once it is down. The connector pool cannot deliver a single message without a live bind, so this
// is a vital readiness dependency (plan §1.5) — register it alongside Kafka and ClickHouse.
func (s *Service) BindReady(context.Context) error {
	if s.bound.Load() {
		return nil
	}
	return errBindNotReady
}

// Run binds to the SMSC, then consumes mt.routed until ctx is cancelled, unbinding cleanly on exit.
// A failure to bind returns an error (the service restarts); a per-message infrastructure failure
// leaves the offset uncommitted for reprocessing.
//
// The bind is also watched independently of the consumer: if it drops while idle — no mt.routed
// flowing, so no Submit is in flight to surface the failure — the consumer would otherwise block on
// Kafka forever with a dead bind while the pod stayed Ready. When that happens Run flips readiness,
// tears the consumer down and returns an error so the supervisor re-dials.
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

	s.bound.Store(true)
	defer s.bound.Store(false)

	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	consumerErr := make(chan error, 1)
	go func() { consumerErr <- s.deps.Consumer.Run(consumerCtx, s.handler(b)) }()

	select {
	case err := <-consumerErr:
		return err
	case <-b.done:
		// The bind died on its own (idle drop, enquire_link timeout, peer close). Take the pod out of
		// rotation immediately, unwind the consumer, and surface the failure so the service restarts.
		s.bound.Store(false)
		cancel()
		<-consumerErr
		return fmt.Errorf("connectorpool: smsc bind dropped: %w", errBindClosed)
	}
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
	source, sourceTON, sourceNPI := sourceAddr(r.From)
	dcs := submitDataCoding(r)
	sm := &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddr:      source,
		DestinationAddr: r.To,
		DataCoding:      dcs,
	}}
	sm.SourceAddrTON, sm.SourceAddrNPI = sourceTON, sourceNPI
	sm.DestAddrTON, sm.DestAddrNPI = smpp.TONInternational, smpp.NPIISDN
	if r.RegisteredDelivery {
		sm.RegisteredDelivery = smpp.RegisteredDeliveryReceipt
	}
	if r.ValidityPeriod != nil {
		sm.ValidityPeriod = *r.ValidityPeriod // already an SMPP validity (relative/absolute) per the contract
	}

	body := encodeBody(r.Body.Reveal(), dcs) // audited: body -> SMSC wire, never logged
	if len(body) > 254 {
		sm.TLVs.Set(smpp.TagMessagePayload, body)
	} else {
		sm.ShortMessage = body
	}
	return sm
}

// submitDataCoding is the wire data_coding byte. An explicit, in-range client override wins (the
// client is driving the DCS directly); otherwise it is derived from the resolved encoding.
func submitDataCoding(r pipeline.RoutedMT) uint8 {
	if dc := r.DataCoding; dc != nil && *dc >= 0 && *dc <= 255 {
		return uint8(*dc) //nolint:gosec // bounded to 0..255 on the line above
	}
	return dataCoding(r.Encoding)
}

// encodeBody renders the revealed body for the wire per the EFFECTIVE data_coding — the same byte
// buildSubmit writes to the submit_sm — so the DCS label and the bytes always agree. A UCS-2 DCS
// means UTF-16BE on the wire, so the UTF-8 bytes msg.Body carries are transcoded; every other DCS
// (GSM-7, binary, or a raw client override) goes out as-is (GSM-7 packing and segmentation are the
// encoding milestone, not M2). The caller reveals the plaintext — an audited egress that reaches the
// SMSC wire, never a log or span.
func encodeBody(body []byte, dcs uint8) []byte {
	if dcs == smpp.DataCodingUCS2 {
		return utf16BE(body)
	}
	return body
}

// utf16BE transcodes UTF-8 bytes to big-endian UTF-16 (UCS-2 on the SMPP wire).
func utf16BE(utf8 []byte) []byte {
	units := utf16.Encode([]rune(string(utf8)))
	out := make([]byte, 2*len(units))
	for i, u := range units {
		binary.BigEndian.PutUint16(out[2*i:], u)
	}
	return out
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
		Encoding:     clickhouse.EncodingOf(r.Encoding),
		Billed:       false,
	}
}

// outcome maps a submit_sm_resp command_status to a CDR status. ESME_ROK is enroute; anything else
// is a failed send, with an error_code drawn from the shared platform/errors contract (never an
// ad-hoc string), so a client reading GET /messages sees a documented code.
func outcome(cmdStatus uint32) (clickhouse.Status, *string) {
	if cmdStatus == smpp.StatusOK {
		return clickhouse.StatusEnroute, nil
	}
	code := string(errs.CodeFromSMPPStatus(cmdStatus))
	return clickhouse.StatusFailed, &code
}

// dataCoding derives the SMPP data_coding byte from the resolved encoding, using the shared encoding
// vocabulary (internal/platform/encoding) so the value set does not drift across the pipeline.
func dataCoding(enc string) uint8 {
	switch enc {
	case encoding.UCS2:
		return smpp.DataCodingUCS2
	case encoding.Binary:
		return smpp.DataCodingBinary
	default:
		return smpp.DataCodingGSM7
	}
}

// sourceAddr maps a submitted source address to its wire form and TON/NPI. A numeric MSISDN
// (optionally "+"-prefixed) becomes a plus-stripped international/ISDN address; anything else passes
// through as an alphanumeric sender id. Source normalization proper is an M5 concern — this is only
// the wire typing the SMSC requires, so a "+1206…" MSISDN is not mistyped as an alphanumeric sender.
func sourceAddr(addr string) (wire string, ton, npi uint8) {
	digits := strings.TrimPrefix(addr, "+")
	if digits != "" && isAllDigits(digits) {
		return digits, smpp.TONInternational, smpp.NPIISDN
	}
	return addr, smpp.TONAlphanumeric, smpp.NPIUnknown
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func segmentCount(n int) uint16 {
	if n < 1 {
		return 1
	}
	return uint16(n) //nolint:gosec // segment count is a small positive integer
}
