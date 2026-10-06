package ingest_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// TestAcceptedRowEncoding: the accepted row keys on the envelope's resolved Encoding string — the same
// value the connector's enroute row uses — so the two lifecycle rows agree. data_coding is the wire
// override the connector honours, not the CDR encoding label.
func TestAcceptedRowEncoding(t *testing.T) {
	dc := 0 // present but not consulted for the CDR encoding label
	tests := []struct {
		name     string
		encoding string
		want     clickhouse.Encoding
	}{
		{"ucs2", "ucs2", clickhouse.EncodingUCS2},
		{"binary", "binary", clickhouse.EncodingBinary},
		{"gsm7", "gsm7", clickhouse.EncodingGSM7},
		{"auto resolves to gsm7", "auto", clickhouse.EncodingGSM7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := ingest.AcceptedRow(pipeline.InboundMT{Encoding: tc.encoding, DataCoding: &dc})
			if row.Encoding != tc.want {
				t.Errorf("encoding = %q, want %q", row.Encoding, tc.want)
			}
		})
	}
}

type countingProducer struct{ produced []kafka.Record }

func (p *countingProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.produced = append(p.produced, rec)
	return nil
}

type stubAdmission struct {
	err      error
	segments []int
	senders  []string
}

func (a *stubAdmission) Admit(_ context.Context, _, customerID uuid.UUID, from string, segments int) error {
	a.segments = append(a.segments, segments)
	a.senders = append(a.senders, customerID.String()+"/"+from)
	return a.err
}

func envelope(text string) pipeline.InboundMT {
	return pipeline.InboundMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		From: "INFO", To: "+2250700000001", Body: msg.NewBodyString(text), Encoding: "auto", SubmittedAt: time.Now(),
	}
}

// TestASubmissionBeyondItsAccountRateIsNeverWritten: the refusal comes before the durable write, so a
// submission the client was refused never reaches mt.inbound (step-283).
func TestASubmissionBeyondItsAccountRateIsNeverWritten(t *testing.T) {
	producer := &countingProducer{}
	i := ingest.NewIngestor(producer, &stubAdmission{err: errs.ErrRateLimited}, nil)

	err := i.Accept(context.Background(), envelope("hello"))
	if code, _ := errs.CodeOf(err); code != errs.ErrRateLimited {
		t.Fatalf("Accept = %v, want rate_limited", err)
	}
	if len(producer.produced) != 0 {
		t.Errorf("a refused submission was written to mt.inbound (%d records)", len(producer.produced))
	}
}

// TestAdmissionPaysTheSegmentsTheMessageWillSend: 160 characters outside GSM-7 go out as three UCS-2
// segments; one token per message would let a client send three times its contract.
func TestAdmissionPaysTheSegmentsTheMessageWillSend(t *testing.T) {
	admission := &stubAdmission{}
	producer := &countingProducer{}
	i := ingest.NewIngestor(producer, admission, nil)

	if err := i.Accept(context.Background(), envelope(strings.Repeat("ê", 160))); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if len(admission.segments) != 1 || admission.segments[0] != 3 {
		t.Errorf("admission charged %v, want [3]", admission.segments)
	}
	if len(producer.produced) != 1 {
		t.Errorf("an admitted submission produced %d records, want 1", len(producer.produced))
	}
}

// TestAdmissionIsAskedForTheSubmittedSender: the sender's bucket is keyed by the address the client
// submitted under its customer, before any rewrite (ADR-0020 §1).
func TestAdmissionIsAskedForTheSubmittedSender(t *testing.T) {
	admission := &stubAdmission{}
	i := ingest.NewIngestor(&countingProducer{}, admission, nil)
	env := envelope("hello")

	if err := i.Accept(context.Background(), env); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if want := env.CustomerID.String() + "/INFO"; len(admission.senders) != 1 || admission.senders[0] != want {
		t.Errorf("admission asked for %v, want [%s]", admission.senders, want)
	}
}
