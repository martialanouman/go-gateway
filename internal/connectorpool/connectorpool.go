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

	"github.com/google/uuid"
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

// errTransientReject marks an SMSC submit_sm_resp whose command_status is retryable (throttled,
// system error, queue full). The handler returns it so the record is not committed and is
// redelivered, rather than recording the message as a terminal failure and losing it.
var errTransientReject = errors.New("connectorpool: transient smsc rejection")

// Consumer reads mt.routed. *kafka.Consumer satisfies it.
type Consumer interface {
	Run(ctx context.Context, handle kafka.Handler) error
}

// CDRWriter records the send outcome. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// CancelFlags reports whether a message was cancelled (via cancel_sm) before it reached the SMSC.
// *cancel.RedisFlags satisfies it. New defaults a nil CancelFlags to a no-op that reports nothing
// cancelled, so the hot path never branches on nil and a missing wiring cannot silently disable the
// check without the no-op being explicit.
type CancelFlags interface {
	Exists(ctx context.Context, messageID uuid.UUID) (bool, error)
}

// noopCancelFlags is the New default when no flag store is wired: it reports nothing cancelled, so the
// connector dispatches normally. Tests that do not exercise cancellation rely on it.
type noopCancelFlags struct{}

func (noopCancelFlags) Exists(context.Context, uuid.UUID) (bool, error) { return false, nil }

// DLRMap remembers smsc_msg_id -> message_id after a successful submit, so a later deliver_sm
// (delivery receipt) can be correlated back to the message (step-044 reads it). *dlrmap.RedisMap
// satisfies it. New defaults a nil DLRMap to a no-op, so the hot path never branches on nil and a
// missing wiring is explicit rather than a silent panic.
type DLRMap interface {
	Put(ctx context.Context, smscMsgID string, r pipeline.RoutedMT) error
}

// noopDLRMap is the New default when no DLR map is wired: it records nothing. Tests that do not
// exercise DLR correlation rely on it.
type noopDLRMap struct{}

func (noopDLRMap) Put(context.Context, string, pipeline.RoutedMT) error { return nil }

// Producer publishes the return path (mo.inbound, dlr.events) durably. *kafka.Producer satisfies it.
// New defaults a nil Producer to a no-op, so a bind with no producer wired acknowledges deliver_sm as
// before (the M2 behaviour) rather than panicking.
type Producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// noopProducer is the New default when no producer is wired: it drops the record. With it, a
// deliver_sm is acknowledged without publishing (the pre-M4 behaviour), which the MT-only tests rely
// on.
type noopProducer struct{}

func (noopProducer) Produce(context.Context, kafka.Record) error { return nil }

// Deps are the connector pool's collaborators.
type Deps struct {
	Consumer    Consumer
	CDR         CDRWriter
	CancelFlags CancelFlags
	DLRMap      DLRMap
	Producer    Producer
	// ConnectorID identifies the SMSC link this pool binds, stamped onto every mo.inbound / dlr.events
	// record so the return-path router can correlate a receipt (step-044). At M2 it is injected from
	// env; M3+ sources it from the connectors control plane.
	ConnectorID uuid.UUID
	Bind        BindConfig
	Tracer      trace.Tracer
	Logger      *slog.Logger
}

// Service is the connector pool.
type Service struct {
	deps Deps

	// bound reports whether the SMSC bind is currently established. It gates the readiness probe, so
	// a bind that drops — including an idle-time drop no in-flight Submit would notice — takes the
	// pod out of rotation until Run re-dials.
	bound atomic.Bool
}

// New builds a Service. A nil logger defaults to slog.Default; a nil CancelFlags defaults to a no-op
// that reports nothing cancelled.
func New(deps Deps) *Service {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.CancelFlags == nil {
		deps.CancelFlags = noopCancelFlags{}
	}
	if deps.DLRMap == nil {
		deps.DLRMap = noopDLRMap{}
	}
	if deps.Producer == nil {
		deps.Producer = noopProducer{}
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
	b, err := dialAndBind(ctx, s.deps.Bind, s.deps.Logger, s.handleDeliver)
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

		// A cancel_sm may have flagged this message before it reached the SMSC. Redis is best-effort
		// here: cancellation is itself best-effort (an already-dispatched message cannot be recalled), so
		// a flag-read failure fails OPEN — we log and dispatch rather than halt all outbound delivery on a
		// Redis outage.
		cancelled, err := s.deps.CancelFlags.Exists(ctx, routed.MessageID)
		if err != nil {
			s.deps.Logger.WarnContext(ctx, "connector: cancel-flag check failed, dispatching anyway",
				"message_id", routed.MessageID, "err", err)
		} else if cancelled {
			// Honour the cancel: record the cancelled outcome and commit without submitting. Writing the
			// row here (not only in the Canceller) is what makes the skip safe: it is idempotent under
			// ReplacingMergeTree (rank 60, collapsing with the Canceller's row) and closes the window where
			// the Canceller crashed after flagging but before writing the row — otherwise the message would
			// be neither sent nor recorded, leaving the CDR stuck on accepted.
			if err := s.deps.CDR.Insert(ctx, cancelledRow(routed)); err != nil {
				return fmt.Errorf("connectorpool: write cancelled cdr: %w", err)
			}
			s.deps.Logger.InfoContext(ctx, "connector: message cancelled before dispatch", "message_id", routed.MessageID)
			return nil
		}

		resp, err := b.Submit(ctx, buildSubmit(routed))
		if err != nil {
			// A dead bind, a write failure or a timeout is transient: do not commit, so the message is
			// reprocessed after a restart. At-least-once means the SMSC may see a duplicate submit; the
			// versioned CDR collapses the duplicate enroute rows (no dedup until M3).
			return fmt.Errorf("connectorpool: submit_sm: %w", err)
		}

		// A transient SMSC rejection (throttled, system error, queue full) is backpressure, not a
		// terminal outcome: do not write a failed CDR and do not commit, so the message is redelivered
		// rather than lost. Permanent rejections (invalid address, submit_fail) fall through to the CDR
		// write below. Proper rate-limited backoff is M7; this reuses the same "return error → no commit
		// → reprocess" path the submit errors above use.
		if resp.Status != smpp.StatusOK && errs.Retryable(errs.CodeFromSMPPStatus(resp.Status)) {
			return fmt.Errorf("connectorpool: submit_sm rejected transiently (status 0x%08x): %w", resp.Status, errTransientReject)
		}

		if err := s.deps.CDR.Insert(ctx, cdrRow(routed, resp)); err != nil {
			return fmt.Errorf("connectorpool: write cdr: %w", err)
		}
		s.recordDLRMapping(ctx, routed, resp)
		return nil
	}
}

// recordDLRMapping remembers smsc_msg_id -> message_id after a successful submit, so a later
// deliver_sm (delivery receipt) can be correlated back to this message (step-044). It is best-effort:
// the message is already enroute, so a mapping-write failure — or a non-ROK response, or a response
// carrying no smsc_msg_id — must never fail the record. A write error is logged and counted only by
// the log; the consequence (a later receipt arriving uncorrelated) is handled in step-044. The log
// carries the ids, never the body (invariant a).
func (s *Service) recordDLRMapping(ctx context.Context, r pipeline.RoutedMT, resp smpp.PDU) {
	if resp.Status != smpp.StatusOK {
		return
	}
	body, ok := resp.Body.(*smpp.SubmitSMResp)
	if !ok || body.MessageID == "" {
		return
	}
	if err := s.deps.DLRMap.Put(ctx, body.MessageID, r); err != nil {
		s.deps.Logger.WarnContext(ctx, "connector: dlr mapping write failed, a later receipt will be uncorrelated",
			"message_id", r.MessageID, "connector_id", r.ConnectorID, "err", err)
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
	// The SMPP validity_period is a 16-char C-Octet String; a longer value would marshal a PDU with no
	// NUL terminator, which the SMSC rejects by dropping the connection — poisoning the partition on
	// redelivery. REST bounds it (maxLength 16), but guard here too so a malformed mt.routed record can
	// never crash the bind: an over-length value is dropped rather than sent.
	if r.ValidityPeriod != nil && len(*r.ValidityPeriod) <= 16 {
		sm.ValidityPeriod = *r.ValidityPeriod
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

// cancelledRow builds the cancelled CDR row (rank 60) for a message a cancel_sm flagged before
// dispatch. It mirrors cdrRow's identifier projection but records no connector outcome (the message
// was never submitted). It is written in addition to the Canceller's own row: idempotent under
// ReplacingMergeTree (same ORDER BY key and rank), it closes the crash window between flag and row.
func cancelledRow(r pipeline.RoutedMT) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID:    r.MessageID,
		TraceID:      r.TraceID,
		AccountID:    r.AccountID,
		CustomerID:   r.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   r.From,
		DestAddr:     r.To,
		SubmittedAt:  r.SubmittedAt,
		Status:       clickhouse.StatusCancelled,
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
