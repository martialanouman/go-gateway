package router_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

type fakeConsumer struct{ records []kafka.Record }

func (f *fakeConsumer) Run(ctx context.Context, handle kafka.Handler) error {
	for _, r := range f.records {
		if err := handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

type fakeProducer struct{ produced []kafka.Record }

func (f *fakeProducer) Produce(_ context.Context, rec kafka.Record) error {
	f.produced = append(f.produced, rec)
	return nil
}

type fakeCDR struct{ rows []clickhouse.CDRRow }

func (f *fakeCDR) Insert(_ context.Context, row clickhouse.CDRRow) error {
	f.rows = append(f.rows, row)
	return nil
}

type stubResolver struct {
	conn uuid.UUID
	err  error
}

func (s stubResolver) Resolve(context.Context, pipeline.RouteRequest) (pipeline.Route, error) {
	if s.err != nil {
		return pipeline.Route{}, s.err
	}
	return pipeline.Route{ConnectorID: s.conn}, nil
}

func inbound(to string) pipeline.InboundMT {
	return pipeline.InboundMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		From: "GATEWAY", To: to, Body: msg.NewBodyString("hello"), Encoding: "auto",
		SubmittedAt: time.Now().UTC(),
	}
}

// allowAllSenderIDs authorizes every source address, so the router tests exercise routing and CDR
// outcomes without a sender-ID policy in the way (that stage is covered in the pipeline tests).
type allowAllSenderIDs struct{}

func (allowAllSenderIDs) Authorize(context.Context, uuid.UUID, uuid.UUID, string) error { return nil }

// allowAllOptOut passes every message; opt-out enforcement is covered in the pipeline/optout tests.
type allowAllOptOut struct{}

func (allowAllOptOut) IsOptedOut(context.Context, uuid.UUID, uuid.UUID, string, string) (bool, error) {
	return false, nil
}

// allowAllAntispam passes every message; anti-spam is covered in the pipeline/antispam tests.
type allowAllAntispam struct{}

func (allowAllAntispam) Evaluate(context.Context, uuid.UUID, uuid.UUID, string, string, []byte) (cp.AntispamAction, error) {
	return "", nil
}

// stubReserver returns a fixed credit-stage verdict, so a router test can drive the reserve outcome without
// a billing client: a coded error rejects, a raw error is transient, the zero value reserves nothing.
type stubReserver struct {
	reserved  bool
	ownerType string
	err       error
}

func (s stubReserver) Reserve(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int) (bool, string, error) {
	return s.reserved, s.ownerType, s.err
}

func newRouter(t *testing.T, resolver pipeline.Resolver, prod router.Producer, cdr router.CDRWriter, cons router.Consumer) *router.Router {
	t.Helper()
	return newRouterWithReserver(t, resolver, nil, prod, cdr, cons)
}

func newRouterWithReserver(t *testing.T, resolver pipeline.Resolver, reserver pipeline.CreditReserver, prod router.Producer, cdr router.CDRWriter, cons router.Consumer) *router.Router {
	t.Helper()
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	return router.New(router.Deps{
		Consumer: cons,
		Producer: prod,
		Pipeline: pipeline.New(tracer, resolver, allowAllSenderIDs{}, allowAllOptOut{}, allowAllAntispam{}, nil, reserver),
		CDR:      cdr,
		Tracer:   tracer,
	})
}

func TestRouterPublishesRoutedOnSuccess(t *testing.T) {
	connector := uuid.New()
	in := inbound("+2250700000000")
	inRec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode inbound: %v", err)
	}

	prod := &fakeProducer{}
	cdr := &fakeCDR{}
	r := newRouter(t, stubResolver{conn: connector}, prod, cdr, &fakeConsumer{records: []kafka.Record{inRec}})

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(prod.produced) != 1 {
		t.Fatalf("expected 1 mt.routed record, got %d", len(prod.produced))
	}
	if prod.produced[0].Topic != kafka.TopicMTRouted {
		t.Errorf("topic: got %q", prod.produced[0].Topic)
	}
	routed, err := pipeline.DecodeRouted(prod.produced[0])
	if err != nil {
		t.Fatalf("decode routed: %v", err)
	}
	if routed.ConnectorID != connector {
		t.Errorf("connector: got %s want %s", routed.ConnectorID, connector)
	}
	if routed.MessageID != in.MessageID {
		t.Errorf("message id not preserved")
	}
	if len(cdr.rows) != 0 {
		t.Errorf("no CDR row expected on the happy path, got %d", len(cdr.rows))
	}
}

// TestRouterFansOutOneRecordPerSegment: a long message is published as one mt.routed record per
// segment, every record keyed by the logical message id (so the segments share a partition and stay
// ordered on one bind) and numbered 1..N with a UDH.
func TestRouterFansOutOneRecordPerSegment(t *testing.T) {
	connector := uuid.New()
	in := inbound("+2250700000000")
	in.Body = msg.NewBodyString(strings.Repeat("a", 161)) // 161 GSM-7 chars -> 2 segments
	inRec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode inbound: %v", err)
	}

	prod := &fakeProducer{}
	r := newRouter(t, stubResolver{conn: connector}, prod, &fakeCDR{}, &fakeConsumer{records: []kafka.Record{inRec}})

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(prod.produced) != 2 {
		t.Fatalf("expected 2 mt.routed records (one per segment), got %d", len(prod.produced))
	}
	key := in.MessageID
	for i, rec := range prod.produced {
		if !bytes.Equal(rec.Key, key[:]) {
			t.Errorf("record %d key = %x, want the message id %x (segments must share a partition)", i, rec.Key, key[:])
		}
		routed, derr := pipeline.DecodeRouted(rec)
		if derr != nil {
			t.Fatalf("decode routed %d: %v", i, derr)
		}
		if routed.SegmentSeq != i+1 || routed.SegmentCount != 2 || !routed.HasUDH {
			t.Errorf("record %d = {seq:%d count:%d udh:%v}, want seq %d / count 2 / udh", i, routed.SegmentSeq, routed.SegmentCount, routed.HasUDH, i+1)
		}
		if routed.ConnectorID != connector {
			t.Errorf("record %d connector = %s, want %s", i, routed.ConnectorID, connector)
		}
	}
}

// fakeSealer records that the router asked it to seal a rejected row, and stamps the body into the content
// column the way a real ContentSealer would for a stored_plaintext customer.
type fakeSealer struct {
	called  bool
	gotCust uuid.UUID
}

func (f *fakeSealer) Seal(_ context.Context, row *clickhouse.CDRRow, body msg.Body, customerID uuid.UUID) {
	f.called = true
	f.gotCust = customerID
	plaintext := string(body.Reveal())
	row.ContentCiphertext = &plaintext
}

// TestRouterSealsContentOnRejectedRow: a rejected message's body is sealed into the CDR row per policy (step
// follow-up), keyed by the customer id — rejects store content the same way accepted rows do.
func TestRouterSealsContentOnRejectedRow(t *testing.T) {
	in := inbound("not-a-number") // fails E.164 → rejected
	inRec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode inbound: %v", err)
	}
	cdr := &fakeCDR{}
	sealer := &fakeSealer{}
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	r := router.New(router.Deps{
		Consumer: &fakeConsumer{records: []kafka.Record{inRec}},
		Producer: &fakeProducer{},
		Pipeline: pipeline.New(tracer, stubResolver{conn: uuid.New()}, allowAllSenderIDs{}, allowAllOptOut{}, allowAllAntispam{}, nil, nil),
		CDR:      cdr,
		Sealer:   sealer,
		Tracer:   tracer,
	})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !sealer.called || sealer.gotCust != in.CustomerID {
		t.Fatalf("sealer called=%v cust=%s, want called for %s", sealer.called, sealer.gotCust, in.CustomerID)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].ContentCiphertext == nil || *cdr.rows[0].ContentCiphertext != "hello" {
		t.Errorf("rejected row content not sealed: %+v", cdr.rows)
	}
}

func TestRouterWritesRejectedCDROnPipelineRejection(t *testing.T) {
	in := inbound("not-a-number") // fails E.164 normalization
	inRec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode inbound: %v", err)
	}

	prod := &fakeProducer{}
	cdr := &fakeCDR{}
	r := newRouter(t, stubResolver{conn: uuid.New()}, prod, cdr, &fakeConsumer{records: []kafka.Record{inRec}})

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(prod.produced) != 0 {
		t.Errorf("a rejected message must not be published to mt.routed, got %d", len(prod.produced))
	}
	if len(cdr.rows) != 1 {
		t.Fatalf("expected 1 rejected CDR row, got %d", len(cdr.rows))
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusRejected {
		t.Errorf("status: got %q want rejected", row.Status)
	}
	if row.MessageID != in.MessageID || !row.SubmittedAt.Equal(in.SubmittedAt) {
		t.Error("rejected row must carry the message id and immutable submitted_at")
	}
	if row.ErrorCode == nil || *row.ErrorCode != "invalid_destination" {
		t.Errorf("error_code: got %v want invalid_destination", row.ErrorCode)
	}
}

// TestRouterPublishesBilledRoutedOnReserve: a billed customer whose reserve succeeds is published to
// mt.routed carrying Billable=true and the resolved OwnerType, the settlement contract connector-pool reads
// to capture the identical balance key (step-146). Closes the reserve→publish loop end-to-end at the router.
func TestRouterPublishesBilledRoutedOnReserve(t *testing.T) {
	connector := uuid.New()
	in := inbound("+2250700000000")
	inRec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode inbound: %v", err)
	}

	prod := &fakeProducer{}
	r := newRouterWithReserver(t, stubResolver{conn: connector},
		stubReserver{reserved: true, ownerType: cp.OwnerTypeCustomer},
		prod, &fakeCDR{}, &fakeConsumer{records: []kafka.Record{inRec}})

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.produced) != 1 {
		t.Fatalf("expected 1 mt.routed record, got %d", len(prod.produced))
	}
	routed, err := pipeline.DecodeRouted(prod.produced[0])
	if err != nil {
		t.Fatalf("decode routed: %v", err)
	}
	if !routed.Billable || routed.OwnerType != cp.OwnerTypeCustomer {
		t.Errorf("(Billable, OwnerType) = (%v, %q), want (true, customer)", routed.Billable, routed.OwnerType)
	}
}

// TestRouterRejectsInsufficientCredit: a billed message the customer cannot afford is rejected with
// insufficient_credit — NO mt.routed for any segment, one rejected CDR (not billed), no ledger (the reserve
// never committed in billing-svc). A multi-segment body proves the WHOLE message is rejected, not one segment.
func TestRouterRejectsInsufficientCredit(t *testing.T) {
	in := inbound("+2250700000000")
	in.Body = msg.NewBodyString(strings.Repeat("a", 161)) // 2 segments
	inRec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode inbound: %v", err)
	}

	prod := &fakeProducer{}
	cdr := &fakeCDR{}
	r := newRouterWithReserver(t, stubResolver{conn: uuid.New()}, stubReserver{err: errs.ErrInsufficientCredit},
		prod, cdr, &fakeConsumer{records: []kafka.Record{inRec}})

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.produced) != 0 {
		t.Errorf("an unaffordable message must publish no segment to mt.routed, got %d", len(prod.produced))
	}
	if len(cdr.rows) != 1 {
		t.Fatalf("expected 1 rejected CDR row, got %d", len(cdr.rows))
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusRejected || row.ErrorCode == nil || *row.ErrorCode != "insufficient_credit" {
		t.Errorf("row = {status:%q code:%v}, want rejected/insufficient_credit", row.Status, row.ErrorCode)
	}
	if row.Billed {
		t.Error("a rejected message must not be marked billed")
	}
}

// TestRouterRetriesOnBillingTransportError: a billing-svc transport fault is transient — the router returns
// the error (leaving the offset uncommitted for redelivery) and writes NEITHER a CDR NOR mt.routed, so a
// billing outage never becomes a permanent rejected row or an unbilled send (fail-closed).
func TestRouterRetriesOnBillingTransportError(t *testing.T) {
	in := inbound("+2250700000000")
	inRec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode inbound: %v", err)
	}

	prod := &fakeProducer{}
	cdr := &fakeCDR{}
	r := newRouterWithReserver(t, stubResolver{conn: uuid.New()}, stubReserver{err: errors.New("billing-svc unavailable")},
		prod, cdr, &fakeConsumer{records: []kafka.Record{inRec}})

	if err := r.Run(context.Background()); err == nil {
		t.Fatal("a billing transport fault must surface as an error so the offset is not committed")
	}
	if len(prod.produced) != 0 || len(cdr.rows) != 0 {
		t.Errorf("a transient billing fault must write neither mt.routed (%d) nor a CDR (%d)", len(prod.produced), len(cdr.rows))
	}
}
