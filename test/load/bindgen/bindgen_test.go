package bindgen_test

import (
	"context"
	"net"
	"testing"
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/test/load/bindgen"
)

// concurrentBinds is the fan-out the harness must sustain against a single peer.
const concurrentBinds = 50

func TestRunHoldsEveryBindAtOnce(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{})

	// Sampled from inside the hold window: the peer's own count of live connections is the only
	// server-side proof that the 50 sessions overlapped instead of being opened and closed one by one.
	var peerConns int

	rep, err := bindgen.Run(context.Background(), bindgen.Config{
		Addr:       s.Addr(),
		Binds:      concurrentBinds,
		SystemID:   "loadgen",
		Password:   "pw",
		Hold:       50 * time.Millisecond,
		OnAllBound: func() { peerConns = s.ConnCount() },
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if peerConns != concurrentBinds {
		t.Errorf("peer live connections during the hold: got %d want %d", peerConns, concurrentBinds)
	}
	if rep.Requested != concurrentBinds {
		t.Errorf("Requested: got %d want %d", rep.Requested, concurrentBinds)
	}
	if rep.Bound != concurrentBinds {
		t.Errorf("Bound: got %d want %d (errors: %v)", rep.Bound, concurrentBinds, rep.Errors)
	}
	if rep.Failed != 0 {
		t.Errorf("Failed: got %d want 0 (errors: %v)", rep.Failed, rep.Errors)
	}
	if len(rep.Errors) != 0 {
		t.Errorf("Errors: got %v want none", rep.Errors)
	}
	if rep.Elapsed <= 0 {
		t.Error("Elapsed: expected a positive run duration")
	}

	// Teardown: every session is unbound and closed by the time Run returns.
	waitFor(t, "the peer to see no live connection", func() bool { return s.ConnCount() == 0 })
}

func TestRunBindsInParallel(t *testing.T) {
	const (
		binds     = 20
		bindDelay = 100 * time.Millisecond
	)
	// A peer that stalls every bind response: binding one at a time would cost binds*bindDelay.
	addr := startSlowPeer(t, bindDelay)

	start := time.Now()
	rep, err := bindgen.Run(context.Background(), bindgen.Config{
		Addr:     addr,
		Binds:    binds,
		SystemID: "loadgen",
		Password: "pw",
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Bound != binds {
		t.Fatalf("Bound: got %d want %d (errors: %v)", rep.Bound, binds, rep.Errors)
	}

	// Serial would be 2s; concurrent is ~one delay. The quarter-of-serial ceiling leaves room for a
	// loaded CI box without ever admitting a sequential implementation.
	if ceiling := bindDelay * binds / 4; elapsed > ceiling {
		t.Errorf("binding %d sessions took %s, over the %s ceiling: they are not concurrent", binds, elapsed, ceiling)
	}
}

func TestRunReportsRejectedBinds(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{RejectBind: errs.StatusInvalidPasswd})

	rep, err := bindgen.Run(context.Background(), bindgen.Config{
		Addr:     s.Addr(),
		Binds:    5,
		SystemID: "loadgen",
		Password: "wrong",
	})
	if err != nil {
		t.Fatalf("Run: a rejected bind is a reported outcome, not a run error: %v", err)
	}
	if rep.Bound != 0 {
		t.Errorf("Bound: got %d want 0", rep.Bound)
	}
	if rep.Failed != 5 {
		t.Errorf("Failed: got %d want 5", rep.Failed)
	}
	if len(rep.Errors) != 5 {
		t.Fatalf("Errors: got %d want 5", len(rep.Errors))
	}
	for _, err := range rep.Errors {
		if err == nil {
			t.Fatal("a failed bind must carry a non-nil error")
		}
	}
	waitFor(t, "the peer to see no live connection", func() bool { return s.ConnCount() == 0 })
}

func TestRunEndsTheHoldOnCancel(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	rep, err := bindgen.Run(ctx, bindgen.Config{
		Addr:       s.Addr(),
		Binds:      4,
		SystemID:   "loadgen",
		Password:   "pw",
		Hold:       time.Hour,
		OnAllBound: cancel,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("a cancelled hold must return promptly, took %s", elapsed)
	}
	if rep.Bound != 4 {
		t.Errorf("Bound: got %d want 4 (errors: %v)", rep.Bound, rep.Errors)
	}
	waitFor(t, "the peer to see no live connection", func() bool { return s.ConnCount() == 0 })
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	base := bindgen.Config{Addr: "127.0.0.1:1", Binds: 1, SystemID: "loadgen", Password: "pw"}

	tests := []struct {
		name  string
		patch func(*bindgen.Config)
	}{
		{"no address", func(c *bindgen.Config) { c.Addr = "" }},
		{"no bind", func(c *bindgen.Config) { c.Binds = 0 }},
		{"negative binds", func(c *bindgen.Config) { c.Binds = -1 }},
		{"no system id", func(c *bindgen.Config) { c.SystemID = "" }},
		// SMPP v3.4 §4.1.1: system_id is a 16-byte C-octet string, password a 9-byte one. The codec
		// truncates on the way back in, so an over-long identity must be refused before the wire.
		{"system id over 15 chars", func(c *bindgen.Config) { c.SystemID = "0123456789abcdef" }},
		{"password over 8 chars", func(c *bindgen.Config) { c.Password = "123456789" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.patch(&cfg)
			rep, err := bindgen.Run(context.Background(), cfg)
			if err == nil {
				t.Fatalf("Run: expected a config error, got report %+v", rep)
			}
			if rep.Requested != 0 {
				t.Errorf("a refused config must report nothing, got %+v", rep)
			}
		})
	}
}

func TestRunAcceptsMaximumLengthCredentials(t *testing.T) {
	s := fakesmsc.Start(t, fakesmsc.Config{})

	rep, err := bindgen.Run(context.Background(), bindgen.Config{
		Addr:     s.Addr(),
		Binds:    1,
		SystemID: "0123456789abcde", // 15
		Password: "12345678",        // 8
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Bound != 1 {
		t.Errorf("Bound: got %d want 1 (errors: %v)", rep.Bound, rep.Errors)
	}
}

// startSlowPeer runs an SMPP peer that answers every bind_transceiver with ESME_ROK, but only after
// delay. It returns the listen address; the listener is closed with the test.
func startSlowPeer(t *testing.T, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return // listener closed with the test
			}
			go func() {
				defer func() { _ = nc.Close() }()
				for {
					pdu, err := smpp.ReadPDU(nc)
					if err != nil {
						return
					}
					switch pdu.Body.(type) {
					case *smpp.BindTransceiver:
						time.Sleep(delay)
						if err := smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.BindTransceiverResp{
							BindRespFields: smpp.BindRespFields{SystemID: "slow-peer"},
						}}); err != nil {
							return
						}
					case *smpp.Unbind:
						_ = smpp.WritePDU(nc, smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.UnbindResp{}})
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// waitFor polls cond until it holds or the test times out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
