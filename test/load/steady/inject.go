package steady

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// defaultText is the body every submission carries when the caller names none. It is short enough to
// stay a single GSM-7 segment, which is what lets [Criteria.SegmentsPerMessage] be 1 and the
// input/output balance mean what it says.
const defaultText = "reference run"

// InjectConfig describes one paced injection against the REST ingress.
type InjectConfig struct {
	// URL is the full submission endpoint, e.g. "http://127.0.0.1:8080/v1/messages". It is a full URL
	// rather than an origin because the harness always has one to hand, and a silently appended path is
	// how a run ends up measuring 404s.
	URL string

	// APIKey is the bearer token.
	APIKey string

	// Sender is the from address; it must be a sender ID the account is authorised for, or every
	// submission is refused by the pipeline and the run measures the reject path.
	Sender string

	// Text is the message body. Empty uses defaultText.
	Text string

	// Rate is the target acceptance rate in submissions per second — the schedule, not a ceiling
	// reached by luck. Submission i is due at start + i/Rate.
	Rate float64

	// Workers is how many submissions may be in flight at once. It bounds the catch-up a slow gateway
	// can trigger: a worker that finds its slot already past sends at once, so without a bound a stall
	// would be followed by an unbounded burst.
	Workers int

	// Duration is how long the injection lasts, warmup and settle included.
	Duration time.Duration

	// Client is the HTTP client. Nil builds one sized for Workers — the default transport keeps only
	// 2 idle connections per host, so an unconfigured client would spend the run in TCP handshakes and
	// blame the gateway for the latency.
	Client *http.Client

	// Dest maps a submission's sequence number to its destination MSISDN. Nil uses a spread over the
	// +2250700xxxxxx block the repository reserves for fixtures.
	Dest func(seq uint64) string

	// Key maps a submission's sequence number to the API key it is sent with, and therefore to the
	// ACCOUNT it lands on. Nil sends every submission with APIKey.
	//
	// It exists because mt.inbound is keyed by account (§1.6, so an account's submissions keep their
	// partition order). One account puts the entire run on ONE partition, whatever the topic's partition
	// count, however many pods join the group and whatever in-process fan-out the router grows — and the
	// per-topic totals read exactly as they would for a balanced run. Spreading the keys is what makes a
	// parallelism result mean anything (step-201d, D5).
	Key func(seq uint64) string
}

// Sample is one submission's outcome.
type Sample struct {
	// At is when the response came back (or the attempt failed). Windowing is done on this instant
	// rather than on the request's start: what a window must contain is the observation, and an
	// attempt still in flight at the window's edge belongs to neither side.
	At time.Time

	// Latency is how long the whole exchange took, connection included.
	Latency time.Duration

	// Err reports anything that was not a 202.
	Err bool
}

// Report is one injection's own view. It is the HTTP leg only: the output side comes from the
// gateway's counters, never from here.
type Report struct {
	// Samples is one entry per attempt, in no particular order — workers merge their own slices at the
	// end. Its length is bounded by Rate×Duration, unlike a peer-side recorder that grows without one.
	Samples []Sample

	// Sent is how many submissions were attempted.
	Sent uint64

	// Behind counts attempts that started after their scheduled instant, i.e. the injector itself could
	// not keep up. A large share means the achieved rate is a property of this harness and not of the
	// gateway, and the run should be re-read with more workers before any conclusion is drawn.
	Behind uint64

	// FirstErr is the first failure observed, kept so a failing run can be diagnosed without the whole
	// sample list. Nil when nothing failed.
	FirstErr error
}

// WindowStats is what a slice of the run looks like.
type WindowStats struct {
	// Samples is how many attempts landed inside the window.
	Samples uint64
	// Accepted is how many of them got a 202.
	Accepted uint64
	// Errors is how many did not.
	Errors uint64
	// P99 is the 99th percentile of the window's own latencies, zero when it carries none.
	P99 time.Duration
	// P50 is the median, reported beside P99 as context.
	P50 time.Duration
	// Max is the slowest attempt in the window.
	Max time.Duration
}

// Between slices the report to the attempts whose response landed in [from, to). It is how warmup and
// settle are kept out of the measurement: an injection runs longer than the window it is scored over.
func (r Report) Between(from, to time.Time) WindowStats {
	latencies := make([]time.Duration, 0, len(r.Samples))
	var out WindowStats
	for _, s := range r.Samples {
		if s.At.Before(from) || !s.At.Before(to) {
			continue
		}
		out.Samples++
		if s.Err {
			out.Errors++
		} else {
			out.Accepted++
		}
		latencies = append(latencies, s.Latency)
		if s.Latency > out.Max {
			out.Max = s.Latency
		}
	}
	out.P99 = Percentile(latencies, 0.99)
	out.P50 = Percentile(latencies, 0.5)
	return out
}

// Inject holds the configured rate against the endpoint for the whole duration and returns what it
// saw. onStart, when non-nil, is called once on the injector's own goroutine at the instant the first
// slot comes due — the caller's window opens there and nowhere else.
//
// It returns an error only for a configuration it refuses to run; a gateway that fails every request
// is a successful injection with a Report full of errors, which is the caller's verdict to draw and
// not this function's.
//
// Cancellation is honoured: every worker stops at its next slot boundary, and the report covers what
// happened up to then.
func Inject(ctx context.Context, cfg InjectConfig, onStart func()) (Report, error) {
	if err := cfg.validate(); err != nil {
		return Report{}, err
	}
	cfg = cfg.withDefaults()

	payloads := newPayloads(cfg)
	start := time.Now()
	deadline := start.Add(cfg.Duration)

	if onStart != nil {
		onStart()
	}

	var (
		seq  seqCounter
		wg   sync.WaitGroup
		mu   sync.Mutex
		rep  Report
		once sync.Once
	)
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := workerRun(ctx, cfg, payloads, &seq, start, deadline)

			mu.Lock()
			defer mu.Unlock()
			rep.Samples = append(rep.Samples, local.samples...)
			rep.Sent += uint64(len(local.samples))
			rep.Behind += local.behind
			if local.firstErr != nil {
				once.Do(func() { rep.FirstErr = local.firstErr })
			}
		}()
	}
	wg.Wait()
	return rep, nil
}

// seqCounter hands out submission indices. A plain mutex-free counter would do, but the injection is
// the one place a data race would be silent — two workers on one index send two identical messages —
// so it is spelled out.
type seqCounter struct {
	mu sync.Mutex
	n  uint64
}

func (s *seqCounter) next() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.n
	s.n++
	return n
}

// workerResult is one worker's private tally, merged once at the end so the hot path takes no shared
// lock and the injector's own contention does not become the latency it measures.
type workerResult struct {
	samples  []Sample
	behind   uint64
	firstErr error
}

func workerRun(ctx context.Context, cfg InjectConfig, payloads *payloadSet, seq *seqCounter,
	start, deadline time.Time) workerResult {
	var out workerResult
	for {
		i := seq.next()
		due := start.Add(time.Duration(float64(i) / cfg.Rate * float64(time.Second)))
		if !due.Before(deadline) {
			return out
		}
		if late, err := waitUntil(ctx, due); err != nil {
			return out
		} else if late {
			out.behind++
		}

		sentAt := time.Now()
		err := submit(ctx, cfg, cfg.Key(i), payloads.at(i))
		at := time.Now()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The harness closing the window is not a failure of the gateway, and counting it as one
			// would put an error on every run that ends cleanly.
			return out
		}
		out.samples = append(out.samples, Sample{At: at, Latency: at.Sub(sentAt), Err: err != nil})
		if err != nil && out.firstErr == nil {
			out.firstErr = err
		}
		if !at.Before(deadline) {
			return out
		}
	}
}

// submit performs one submission. Anything other than 202 is an error, body included in the message so
// a failing run names its own cause.
func submit(ctx context.Context, cfg InjectConfig, apiKey string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("steady: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := cfg.Client.Do(req)
	if err != nil {
		return fmt.Errorf("steady: submit: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// The body is read to EOF whatever the status, and only then dropped. A response left partly read
	// is a connection the transport cannot return to the pool: the run would then open a socket per
	// submission and report the handshakes as the gateway's ingest latency. The limit bounds what a
	// misbehaving peer can make the harness allocate.
	answer, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusAccepted {
		if len(answer) > errSnippet {
			answer = answer[:errSnippet]
		}
		return fmt.Errorf("steady: submit: status %d, want 202: %s", resp.StatusCode, answer)
	}
	// The 202 body is not decoded: the id is of no use to a rate measurement, and parsing it per
	// submission would put the harness's own JSON cost inside the latency it reports.
	return nil
}

// Bounds on what one response may cost the harness.
const (
	// maxResponseBody is how much of a response is read into memory before the rest is discarded.
	maxResponseBody = 8 << 10
	// errSnippet is how much of a refusal's body reaches the error message.
	errSnippet = 256
)

// payloadSet pre-renders the request bodies so JSON encoding never lands inside a measured latency.
//
// A ring of distinct bodies rather than one: a single destination would exercise one route entry and
// one anti-spam counter, and a body rendered per submission would put the encoder's cost in the p99.
type payloadSet struct {
	bodies [][]byte
}

// payloadRing is how many distinct bodies are pre-rendered. It is large enough that the destinations
// spread across the fixture block and small enough to stay in cache.
const payloadRing = 4096

func newPayloads(cfg InjectConfig) *payloadSet {
	set := &payloadSet{bodies: make([][]byte, payloadRing)}
	for i := range set.bodies {
		body, err := json.Marshal(map[string]any{
			"to": cfg.Dest(uint64(i)), "from": cfg.Sender, "text": cfg.Text,
		})
		if err != nil {
			// Unreachable: the map holds three strings. Panicking beats a nil body that would make every
			// submission a 400 and the run a measurement of the reject path.
			panic(fmt.Sprintf("steady: render payload: %v", err))
		}
		set.bodies[i] = body
	}
	return set
}

// at returns the pre-rendered body for a submission index, cycling through the ring.
func (p *payloadSet) at(seq uint64) []byte { return p.bodies[seq%payloadRing] }

func (c InjectConfig) validate() error {
	switch {
	case c.URL == "":
		return errors.New("steady: InjectConfig.URL is required")
	case c.Rate <= 0:
		return fmt.Errorf("steady: InjectConfig.Rate must be positive, got %v", c.Rate)
	case c.Workers <= 0:
		return fmt.Errorf("steady: InjectConfig.Workers must be at least 1, got %d", c.Workers)
	case c.Duration <= 0:
		return fmt.Errorf("steady: InjectConfig.Duration must be positive, got %v", c.Duration)
	}
	return nil
}

func (c InjectConfig) withDefaults() InjectConfig {
	if c.Text == "" {
		c.Text = defaultText
	}
	if c.Dest == nil {
		c.Dest = func(seq uint64) string { return fmt.Sprintf("+2250700%06d", seq%1000000) }
	}
	if c.Key == nil {
		key := c.APIKey
		c.Key = func(uint64) string { return key }
	}
	if c.Client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		// One idle connection per worker, so a worker never pays a handshake mid-run. The default
		// MaxIdleConnsPerHost is 2, which at any real rate turns the run into a connection benchmark.
		transport.MaxIdleConns = c.Workers * 2
		transport.MaxIdleConnsPerHost = c.Workers * 2
		transport.MaxConnsPerHost = c.Workers * 2
		c.Client = &http.Client{Transport: transport}
	}
	return c
}

// waitUntil sleeps until t, reporting whether the instant was already past — the injector falling
// behind its own schedule. Cancellation returns an error and the worker stops.
func waitUntil(ctx context.Context, t time.Time) (late bool, err error) {
	d := time.Until(t)
	if d <= 0 {
		return true, ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
		return false, nil
	}
}
