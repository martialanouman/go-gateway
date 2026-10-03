package outcome_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/outcome"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

type fakeMarker struct {
	calls [][]cp.SenderIDUse
	err   error
}

func (f *fakeMarker) MarkFirstUsed(_ context.Context, uses []cp.SenderIDUse) error {
	f.calls = append(f.calls, uses)
	return f.err
}

// TestFirstUseMarksTheSubmittedAddressOncePerBatch: the address marked is the one the client submitted
// (the rewrite target is never one it registered), failed counts like enroute — both reached the SMSC
// wire — and a sender ID seen twice in a poll batch carries its earliest submission.
func TestFirstUseMarksTheSubmittedAddressOncePerBatch(t *testing.T) {
	customer := uuid.New()
	early, late := submittedAt, submittedAt.Add(time.Minute)
	event := func(from, original, status string, at time.Time) pipeline.OutcomeMT {
		e := enrouteEvent()
		e.CustomerID, e.From, e.OriginalFrom, e.Status, e.SubmittedAt = customer, from, original, status, at
		return e
	}
	consumer := &capturingConsumer{recs: []kafka.Record{
		outcomeRec(t, event("ACME", "", "enroute", late)),
		{Value: []byte("not json")},
		outcomeRec(t, event("ACME", "", "failed", early)),
		outcomeRec(t, event("PLATFORM", "BANK", "enroute", late)),
	}}
	marker := &fakeMarker{}

	_ = outcome.NewFirstUse(consumer, marker, nil).Run(t.Context())

	want := []cp.SenderIDUse{
		{CustomerID: customer, Address: "ACME", UsedAt: early},
		{CustomerID: customer, Address: "BANK", UsedAt: late},
	}
	if len(marker.calls) != 1 || !reflect.DeepEqual(marker.calls[0], want) {
		t.Fatalf("marks = %+v, want one call with %+v", marker.calls, want)
	}
	for i, err := range consumer.results {
		if err != nil {
			t.Fatalf("record %d not committable: %v", i, err)
		}
	}
}

// TestFirstUseFailsTheWholeBatchWhenTheMarkFails: nothing commits, so the batch is replayed rather than
// a first use lost.
func TestFirstUseFailsTheWholeBatchWhenTheMarkFails(t *testing.T) {
	consumer := &capturingConsumer{recs: []kafka.Record{outcomeRec(t, enrouteEvent()), {Value: []byte("not json")}}}

	_ = outcome.NewFirstUse(consumer, &fakeMarker{err: errors.New("postgres down")}, nil).Run(t.Context())

	for i, err := range consumer.results {
		if err == nil {
			t.Fatalf("record %d committable despite the failed mark", i)
		}
	}
}
