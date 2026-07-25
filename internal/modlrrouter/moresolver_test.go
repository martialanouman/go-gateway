package modlrrouter_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

type fakeNumberLister struct{ nums []cp.InboundNumber }

func (f fakeNumberLister) List(context.Context) ([]cp.InboundNumber, error) { return f.nums, nil }

type fakeKeywordLister struct{ kws []cp.InboundKeyword }

func (f fakeKeywordLister) ListAll(context.Context) ([]cp.InboundKeyword, error) { return f.kws, nil }

type fakeCustomerLister struct{ m map[uuid.UUID]uuid.UUID }

func (f fakeCustomerLister) ListAccountCustomers(context.Context) (map[uuid.UUID]uuid.UUID, error) {
	return f.m, nil
}

type fakeProducer struct{ recs []kafka.Record }

func (f *fakeProducer) Produce(_ context.Context, rec kafka.Record) error {
	f.recs = append(f.recs, rec)
	return nil
}

type fakeUnrouted struct{ rows []cp.NewUnroutedMO }

func (f *fakeUnrouted) Create(_ context.Context, in cp.NewUnroutedMO) (cp.UnroutedMO, error) {
	f.rows = append(f.rows, in)
	return cp.UnroutedMO{}, nil
}

type fakeMetric struct{ calls []string }

func (f *fakeMetric) Inc(connectorID, reason string) {
	f.calls = append(f.calls, connectorID+"/"+reason)
}

// moSnapshot builds a snapshot from fixed fixtures.
func moSnapshot(t *testing.T, nums []cp.InboundNumber, kws []cp.InboundKeyword, cust map[uuid.UUID]uuid.UUID) *modlrrouter.Snapshot {
	t.Helper()
	snap, err := modlrrouter.LoadSnapshot(context.Background(), slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)),
		fakeNumberLister{nums}, fakeKeywordLister{kws}, fakeCustomerLister{cust})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return snap
}

// moRecord encodes an mo.inbound record.
func moRecord(t *testing.T, connectorID uuid.UUID, from, to, body string) kafka.Record {
	t.Helper()
	rec, err := pipeline.EncodeMO(pipeline.MOInbound{
		ConnectorID: connectorID, From: from, To: to, Body: msg.NewBodyString(body),
		DataCoding: 0, ReceivedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("EncodeMO: %v", err)
	}
	return rec
}

// runMO drives one mo.inbound record through the MO router and returns its log output and Run's error.
func runMO(t *testing.T, deps modlrrouter.MODeps, rec kafka.Record) (string, error) {
	t.Helper()
	logBuf := &bytes.Buffer{}
	deps.Consumer = &fakeConsumer{records: []kafka.Record{rec}}
	deps.Logger = slog.New(slog.NewTextHandler(logBuf, nil))
	if deps.Tracer == nil {
		deps.Tracer = observability.Tracer(nil, "test")
	}
	err := modlrrouter.NewMORouter(deps).Run(context.Background())
	return logBuf.String(), err
}

// TestMORouterPublishesRoutedMO: an MO to a dedicated number is published on mo.routed with the
// resolved account and a minted message id; nothing is filed as unrouted.
func TestMORouterPublishesRoutedMO(t *testing.T) {
	account, customer := uuid.New(), uuid.New()
	numID, connector := uuid.New(), uuid.New()
	snap := moSnapshot(t,
		[]cp.InboundNumber{{ID: numID, Address: "36000", Status: cp.InboundNumberActive, AccountID: &account}},
		nil, map[uuid.UUID]uuid.UUID{account: customer})
	prod := &fakeProducer{}
	unrouted := &fakeUnrouted{}

	_, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: unrouted},
		moRecord(t, connector, "22507000001", "36000", "hello"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.recs) != 1 || len(unrouted.rows) != 0 {
		t.Fatalf("want 1 routed / 0 unrouted, got %d / %d", len(prod.recs), len(unrouted.rows))
	}
	routed, err := pipeline.DecodeMORouted(prod.recs[0])
	if err != nil {
		t.Fatalf("DecodeMORouted: %v", err)
	}
	if routed.AccountID != account || routed.CustomerID != customer || routed.InboundNumberID != numID ||
		routed.ConnectorID != connector || routed.To != "36000" {
		t.Errorf("routed = %+v, want account %s / customer %s / number %s", routed, account, customer, numID)
	}
	if routed.MessageID == uuid.Nil || routed.TraceID == uuid.Nil {
		t.Error("routed MO must have minted message_id and trace_id")
	}
	if string(routed.Body.Reveal()) != "hello" {
		t.Errorf("body = %q, want hello", routed.Body.Reveal())
	}
}

// TestMORouterMintsDeterministicID: the SAME mo.inbound record routed twice yields the SAME message
// id, so an at-least-once redelivery collapses downstream instead of duplicating.
func TestMORouterMintsDeterministicID(t *testing.T) {
	account := uuid.New()
	snap := moSnapshot(t,
		[]cp.InboundNumber{{ID: uuid.New(), Address: "36000", Status: cp.InboundNumberActive, AccountID: &account}},
		nil, map[uuid.UUID]uuid.UUID{account: uuid.New()})
	rec := moRecord(t, uuid.New(), "22507000001", "36000", "hello")

	route := func() uuid.UUID {
		prod := &fakeProducer{}
		if _, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: &fakeUnrouted{}}, rec); err != nil {
			t.Fatalf("Run: %v", err)
		}
		routed, _ := pipeline.DecodeMORouted(prod.recs[0])
		return routed.MessageID
	}
	if a, b := route(), route(); a != b {
		t.Errorf("message id not deterministic: %s vs %s (redelivery would duplicate)", a, b)
	}
}

// TestMORouterRecordsUnrouted: an MO to an unknown number is filed as unrouted and counted, and NOT
// published.
func TestMORouterRecordsUnrouted(t *testing.T) {
	snap := moSnapshot(t, nil, nil, map[uuid.UUID]uuid.UUID{})
	prod := &fakeProducer{}
	unrouted := &fakeUnrouted{}
	metric := &fakeMetric{}
	connector := uuid.New()

	_, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: prod, Unrouted: unrouted, Metric: metric},
		moRecord(t, connector, "22507000001", "99999", "hello"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.recs) != 0 {
		t.Errorf("an unrouted MO must not be published, got %d", len(prod.recs))
	}
	if len(unrouted.rows) != 1 || unrouted.rows[0].Reason != cp.UnroutedUnknownNumber {
		t.Fatalf("want 1 unrouted with unknown_number, got %+v", unrouted.rows)
	}
	if unrouted.rows[0].ConnectorID == nil || *unrouted.rows[0].ConnectorID != connector {
		t.Errorf("unrouted connector = %v, want %s", unrouted.rows[0].ConnectorID, connector)
	}
	if len(metric.calls) != 1 || metric.calls[0] != connector.String()+"/unknown_number" {
		t.Errorf("metric calls = %v, want one unknown_number", metric.calls)
	}
}

// TestMORouterNeverLogsBody: the MO body never appears in the log, on either path (invariant a).
func TestMORouterNeverLogsBody(t *testing.T) {
	const body = "SECRET_MO_CONTENT"
	// Routed path.
	account := uuid.New()
	snap := moSnapshot(t,
		[]cp.InboundNumber{{ID: uuid.New(), Address: "36000", Status: cp.InboundNumberActive, AccountID: &account}},
		nil, map[uuid.UUID]uuid.UUID{account: uuid.New()})
	logs, err := runMO(t, modlrrouter.MODeps{Snapshot: snap, Producer: &fakeProducer{}, Unrouted: &fakeUnrouted{}},
		moRecord(t, uuid.New(), "22507000001", "36000", body))
	if err != nil {
		t.Fatalf("Run routed: %v", err)
	}
	if strings.Contains(logs, body) {
		t.Errorf("body leaked into the log on the routed path:\n%s", logs)
	}
	// Unrouted path.
	logs, err = runMO(t, modlrrouter.MODeps{Snapshot: moSnapshot(t, nil, nil, nil), Producer: &fakeProducer{}, Unrouted: &fakeUnrouted{}, Metric: &fakeMetric{}},
		moRecord(t, uuid.New(), "22507000001", "99999", body))
	if err != nil {
		t.Fatalf("Run unrouted: %v", err)
	}
	if strings.Contains(logs, body) {
		t.Errorf("body leaked into the log on the unrouted path:\n%s", logs)
	}
}

// TestMORouterSkipsUndecodable: a garbage record is skipped (committed), never wedging the partition.
func TestMORouterSkipsUndecodable(t *testing.T) {
	prod := &fakeProducer{}
	unrouted := &fakeUnrouted{}
	_, err := runMO(t, modlrrouter.MODeps{Snapshot: moSnapshot(t, nil, nil, nil), Producer: prod, Unrouted: unrouted},
		kafka.Record{Topic: kafka.TopicMOInbound, Value: []byte("not json")})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(prod.recs) != 0 || len(unrouted.rows) != 0 {
		t.Errorf("undecodable record should do nothing: routed=%d unrouted=%d", len(prod.recs), len(unrouted.rows))
	}
}
