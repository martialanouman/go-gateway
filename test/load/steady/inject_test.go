package steady_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/steady"
)

// acceptor answers every POST with 202 and counts what it got.
func acceptor(t *testing.T, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"00000000-0000-0000-0000-000000000001","status":"accepted"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func injectCfg(url string) steady.InjectConfig {
	return steady.InjectConfig{
		URL:      url,
		APIKey:   "key",
		Sender:   "ACME",
		Text:     "reference run",
		Rate:     200,
		Workers:  8,
		Duration: 300 * time.Millisecond,
	}
}

// TestInjectSendsOnSchedule: an open-loop injector attempts what the schedule says and no more. The
// upper bound is the deterministic half — a closed-loop injector would blow straight through it.
func TestInjectSendsOnSchedule(t *testing.T) {
	var hits atomic.Int64
	srv := acceptor(t, &hits)

	cfg := injectCfg(srv.URL)
	rep, err := steady.Inject(t.Context(), cfg, nil)
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}

	scheduled := int(math.Floor(cfg.Rate*cfg.Duration.Seconds())) + 1
	if rep.Sent > uint64(scheduled) {
		t.Errorf("Sent = %d, want at most the %d the schedule allows", rep.Sent, scheduled)
	}
	if rep.Sent < uint64(scheduled/2) {
		t.Errorf("Sent = %d, want at least half of the %d scheduled against an instant server",
			rep.Sent, scheduled)
	}
	if got := uint64(hits.Load()); got != rep.Sent {
		t.Errorf("server saw %d requests, injector reported %d sent", got, rep.Sent)
	}
	if uint64(len(rep.Samples)) != rep.Sent {
		t.Errorf("len(Samples) = %d, want %d (one per attempt)", len(rep.Samples), rep.Sent)
	}
}

// TestInjectCountsAcceptancesAndErrorsApart: anything that is not a 202 is an error, and D2 tolerates
// none. A 500 that counted as an acceptance would make the whole run meaningless.
func TestInjectCountsAcceptancesAndErrorsApart(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1)%2 == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)

	rep, err := steady.Inject(t.Context(), injectCfg(srv.URL), nil)
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}
	win := rep.Between(time.Time{}, time.Now().Add(time.Hour))
	if win.Errors == 0 {
		t.Fatalf("Errors = 0, want the half of %d attempts the server refused", rep.Sent)
	}
	if win.Accepted+win.Errors != rep.Sent {
		t.Errorf("Accepted+Errors = %d, want %d", win.Accepted+win.Errors, rep.Sent)
	}
	if rep.FirstErr == nil {
		t.Error("FirstErr = nil, want the first refusal so a failing run can be diagnosed")
	}
}

// TestBetweenWindowsTheSamples: warmup and settle exist so their samples do NOT enter the measurement,
// and a Between that ignored its bounds would fold them back in.
func TestBetweenWindowsTheSamples(t *testing.T) {
	var hits atomic.Int64
	srv := acceptor(t, &hits)

	cfg := injectCfg(srv.URL)
	cfg.Duration = 600 * time.Millisecond
	start := time.Now()
	rep, err := steady.Inject(t.Context(), cfg, nil)
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}

	all := rep.Between(time.Time{}, time.Now().Add(time.Hour))
	half := rep.Between(start.Add(300*time.Millisecond), time.Now().Add(time.Hour))
	if all.Samples == 0 {
		t.Fatal("the full window carries no sample, so the windowing proves nothing")
	}
	if half.Samples >= all.Samples {
		t.Errorf("the second half carries %d of the %d samples: Between is not windowing",
			half.Samples, all.Samples)
	}
	if half.Samples == 0 {
		t.Errorf("the second half of a %v injection carries no sample at all", cfg.Duration)
	}
}

// TestBetweenP99IsTheWindowsOwn: the percentile must come from the windowed samples, not the whole run.
func TestBetweenP99IsTheWindowsOwn(t *testing.T) {
	// A server that is slow only for its first few answers, so the two windows differ sharply.
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) <= 5 {
			time.Sleep(120 * time.Millisecond)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)

	cfg := injectCfg(srv.URL)
	cfg.Duration = 600 * time.Millisecond
	start := time.Now()
	rep, err := steady.Inject(t.Context(), cfg, nil)
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}

	all := rep.Between(time.Time{}, time.Now().Add(time.Hour))
	late := rep.Between(start.Add(400*time.Millisecond), time.Now().Add(time.Hour))
	if late.Samples == 0 {
		t.Fatal("the late window carries no sample")
	}
	if late.P99 >= all.P99 {
		t.Errorf("late p99 %v is not below the whole-run p99 %v: the percentile is not windowed",
			late.P99, all.P99)
	}
}

// TestInjectCallsOnStartOnce: the harness counts its window from this callback, so a second call would
// silently reset the window and a missing one would leave it never opening.
func TestInjectCallsOnStartOnce(t *testing.T) {
	var hits atomic.Int64
	srv := acceptor(t, &hits)

	var starts atomic.Int64
	cfg := injectCfg(srv.URL)
	cfg.Duration = 200 * time.Millisecond
	if _, err := steady.Inject(t.Context(), cfg, func() { starts.Add(1) }); err != nil {
		t.Fatalf("Inject: %v", err)
	}
	if got := starts.Load(); got != 1 {
		t.Errorf("OnStart called %d times, want exactly 1", got)
	}
}

// TestInjectStopsOnCancellation: the harness cancels the injection the moment the tier is over, and a
// run that ignored it would keep hammering the stack while the next reading is taken.
func TestInjectStopsOnCancellation(t *testing.T) {
	var hits atomic.Int64
	srv := acceptor(t, &hits)

	cfg := injectCfg(srv.URL)
	cfg.Duration = time.Hour

	ctx, cancel := context.WithCancel(t.Context())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = steady.Inject(ctx, cfg, nil)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Inject did not return within 10s of cancellation")
	}
}

// TestInjectRefusesAnUnusableConfig: every one of these produces a run that looks like it happened and
// measured nothing.
func TestInjectRefusesAnUnusableConfig(t *testing.T) {
	var hits atomic.Int64
	srv := acceptor(t, &hits)

	tests := []struct {
		name  string
		spoil func(*steady.InjectConfig)
	}{
		{"no url", func(c *steady.InjectConfig) { c.URL = "" }},
		{"no rate", func(c *steady.InjectConfig) { c.Rate = 0 }},
		{"negative rate", func(c *steady.InjectConfig) { c.Rate = -1 }},
		{"no worker", func(c *steady.InjectConfig) { c.Workers = 0 }},
		{"no duration", func(c *steady.InjectConfig) { c.Duration = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := injectCfg(srv.URL)
			tc.spoil(&cfg)
			if _, err := steady.Inject(t.Context(), cfg, nil); err == nil {
				t.Errorf("Inject(%s) = nil error, want a refusal before a single request goes out", tc.name)
			}
		})
	}
}

// TestInjectSendsDistinctDestinations: every submission must be its own message. A constant destination
// would exercise one route entry and one anti-spam counter, which is not the shape of real traffic.
func TestInjectSendsDistinctDestinations(t *testing.T) {
	seen := make(chan string, 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		select {
		case seen <- string(buf[:n]):
		default:
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)

	cfg := injectCfg(srv.URL)
	cfg.Duration = 200 * time.Millisecond
	cfg.Dest = func(seq uint64) string { return fmt.Sprintf("+225070%06d", seq) }
	if _, err := steady.Inject(t.Context(), cfg, nil); err != nil {
		t.Fatalf("Inject: %v", err)
	}
	close(seen)

	bodies := make(map[string]struct{})
	for b := range seen {
		bodies[b] = struct{}{}
	}
	if len(bodies) < 2 {
		t.Errorf("the server saw %d distinct bodies, want the destinations to vary per submission", len(bodies))
	}
}

// TestInjectReportsATransportFailure: a peer that is not there must produce errors and a cause, never a
// clean report of zero.
func TestInjectReportsATransportFailure(t *testing.T) {
	srv := acceptor(t, new(atomic.Int64))
	url := srv.URL
	srv.Close() // nothing listens there any more

	cfg := injectCfg(url)
	cfg.Duration = 200 * time.Millisecond
	rep, err := steady.Inject(t.Context(), cfg, nil)
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}
	win := rep.Between(time.Time{}, time.Now().Add(time.Hour))
	if win.Errors == 0 {
		t.Error("Errors = 0 against a closed port, want every attempt counted as an error")
	}
	if win.Accepted != 0 {
		t.Errorf("Accepted = %d against a closed port, want 0", win.Accepted)
	}
	if rep.FirstErr == nil || errors.Is(rep.FirstErr, context.Canceled) {
		t.Errorf("FirstErr = %v, want the transport failure", rep.FirstErr)
	}
}
