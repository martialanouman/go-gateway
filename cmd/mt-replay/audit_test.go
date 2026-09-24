package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

type fakeTrail struct {
	beginErr    error
	intent      cp.AuditIntent
	id          uuid.UUID
	finishedID  uuid.UUID
	status      int
	finishCtxOK bool
}

func (f *fakeTrail) Begin(_ context.Context, in cp.AuditIntent) (uuid.UUID, error) {
	f.intent = in
	if f.beginErr != nil {
		return uuid.Nil, f.beginErr
	}
	f.id = uuid.New()
	return f.id, nil
}

func (f *fakeTrail) Finish(ctx context.Context, id uuid.UUID, status int) error {
	f.finishedID, f.status, f.finishCtxOK = id, status, ctx.Err() == nil
	return nil
}

func TestDeclaredOperator(t *testing.T) {
	// A newline or a bidi override would let a declared name print as an authenticated tok_… in psql.
	for _, bad := range []string{"", "   ", strings.Repeat("é", maxOperatorLen+1), "x\ntok_ab12", "x\u202etok"} {
		if _, err := declaredOperator(bad); err == nil {
			t.Errorf("declaredOperator(%q) accepted", bad)
		}
	}
	got, err := declaredOperator(" alice ")
	if err != nil || got != "declared:alice" {
		t.Fatalf("declaredOperator(\" alice \") = %q, %v; want declared:alice", got, err)
	}
	if _, err := declaredOperator(strings.Repeat("é", maxOperatorLen)); err != nil {
		t.Errorf("a %d-rune name is refused: %v", maxOperatorLen, err)
	}
}

func TestAuditedReplayRefusesToDrainWithoutARow(t *testing.T) {
	trail := &fakeTrail{beginErr: errors.New("postgres down")}
	drained := false
	err := auditedReplay(context.Background(), trail, "declared:alice", "run-1", func(context.Context) error {
		drained = true
		return nil
	})
	if err == nil || drained {
		t.Fatalf("err = %v, drained = %v; want an error and no drain", err, drained)
	}
	if trail.finishedID != uuid.Nil {
		t.Fatal("Finish called for a row that was never written")
	}
}

func TestAuditedReplayRecordsWhoAndTheOutcome(t *testing.T) {
	drainErr := errors.New("clickhouse unreachable")
	want := cp.AuditIntent{Operator: "declared:alice", OperationID: "mt-replay", Method: "REPLAY", Target: "mt.dead-letter", RequestID: "run-1"}
	for _, tc := range []struct {
		name   string
		drain  error
		status int
	}{
		{"clean stop", nil, 200},
		{"interrupted", context.Canceled, 200},
		{"drain failed", drainErr, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			trail := &fakeTrail{}
			err := auditedReplay(ctx, trail, "declared:alice", "run-1", func(context.Context) error {
				cancel() // the signal that stops a drain also cancels the context Finish must outlive
				return tc.drain
			})
			if !errors.Is(err, tc.drain) {
				t.Fatalf("err = %v; want %v", err, tc.drain)
			}
			if trail.intent != want {
				t.Errorf("intent = %+v; want %+v", trail.intent, want)
			}
			if trail.finishedID != trail.id || trail.status != tc.status {
				t.Errorf("finished id match = %v, status = %d; want the begun row finished with %d",
					trail.finishedID == trail.id, trail.status, tc.status)
			}
			if !trail.finishCtxOK {
				t.Error("Finish ran on the cancelled context: the outcome would never be written")
			}
		})
	}
}
