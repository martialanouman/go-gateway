package router_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
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

func newRouter(t *testing.T, resolver pipeline.Resolver, prod router.Producer, cdr router.CDRWriter, cons router.Consumer) *router.Router {
	t.Helper()
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	return router.New(router.Deps{
		Consumer: cons,
		Producer: prod,
		Pipeline: pipeline.New(tracer, resolver, allowAllSenderIDs{}, allowAllOptOut{}, allowAllAntispam{}, nil),
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
