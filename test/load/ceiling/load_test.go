package ceiling_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/test/load/bindgen"
	"github.com/martialanouman/go-gateway/test/load/ceiling"
)

// TestBindgenLoadInjectsAndSignalsItsStart pins the adapter's whole contract against a real SMPP peer:
// the tier's bind count and hold reach bindgen, OnStart fires while the sessions are up, and submit_sm
// really leave — an adapter that forgot to turn the injector on would bind, hold and report a silent
// run that the sweep would then have to catch as a zero.
func TestBindgenLoadInjectsAndSignalsItsStart(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() },
	})

	var connsAtStart int
	load := ceiling.BindgenLoad{Base: bindgen.Config{
		Addr:     s.Addr(),
		SystemID: "loadgen",
		Password: "pw",
	}}

	rep, err := load.Inject(context.Background(), ceiling.LoadParams{
		Binds:   4,
		Hold:    150 * time.Millisecond,
		OnStart: func() { connsAtStart = s.ConnCount() },
	})
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}

	if rep.Requested != 4 || rep.Bound != 4 {
		t.Errorf("requested/bound = %d/%d, want 4/4", rep.Requested, rep.Bound)
	}
	if connsAtStart != 4 {
		t.Errorf("live peer connections when OnStart fired = %d, want %d", connsAtStart, 4)
	}
	if rep.Submitted == 0 {
		t.Error("Submitted = 0, want the adapter to have turned the injector on")
	}
	if rep.Accepted == 0 {
		t.Errorf("Accepted = 0 out of %d submitted, want the peer's answers to have been matched", rep.Submitted)
	}
	if rep.Submitting <= 0 {
		t.Error("Submitting = 0, want the injection window's own duration")
	}
}

// TestBindgenLoadKeepsTheCallersSubmitShape checks the adapter does not overwrite an explicit
// SubmitConfig — the sweep must be able to size the in-flight window, which is the one injector knob
// that decides whether the peer is asked for its ceiling or its latency.
func TestBindgenLoadKeepsTheCallersSubmitShape(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() },
	})

	load := ceiling.BindgenLoad{Base: bindgen.Config{
		Addr:     s.Addr(),
		SystemID: "loadgen",
		Password: "pw",
		Submit:   &bindgen.SubmitConfig{Window: 1, Count: 3},
	}}

	rep, err := load.Inject(context.Background(), ceiling.LoadParams{Binds: 2, Hold: time.Second})
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}
	if want := 2 * 3; rep.Submitted != want {
		t.Errorf("Submitted = %d, want %d (Count=3 per session on 2 sessions)", rep.Submitted, want)
	}
}
