package connectorpool

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Run keeps the bind pool alive across reconnects AND reconfigures (step-127/128b). Each outer iteration
// re-reads the connector's live config (bind_pool_size + reconnect policy) from the control plane, runs a
// reconnect loop over one dial-and-consume cycle, and reacts to how it ended:
//   - clean ctx shutdown → return nil;
//   - reconfigure requested (Admin rebind / resize / policy) → re-read config and re-dial immediately;
//   - reconnection disabled or exhausted → PARK (stay alive, link down) until a reconfigure or shutdown
//     — never exit, so k8s does not turn into a harsher reconnect loop than the one the operator chose.
func (s *Service) Run(ctx context.Context) error {
	for {
		s.reloadConfig(ctx)
		loop := reconnect.New(s.reconnectCfg)
		err := loop.Run(ctx, s.runOnce, isLinkDrop)
		if ctx.Err() != nil {
			s.setLink(linkDown)
			return nil
		}
		if errors.Is(err, errReconfigure) {
			continue // Admin change: re-read config and re-dial (backoff reset)
		}
		if err == nil {
			s.setLink(linkDown)
			return nil
		}
		// Reconnection gave up (disabled, exhausted, or a permanent bind rejection). With a control plane
		// wired, PARK — stay alive with a down link and wait for an Admin rebind — rather than exit (which
		// would let k8s restart into a harsher reconnect loop than the operator chose). Without one there
		// is nothing to un-park us, so surface the error and let the supervisor restart (the pre-128b
		// behaviour).
		s.setLink(linkDown)
		if s.deps.StatusControl == nil {
			return err
		}
		s.deps.Logger.WarnContext(ctx, "connector: link down, parking until reconfigure", "err", err)
		if perr := s.park(ctx); perr != nil {
			return perr // ctx cancelled
		}
		// park returned nil → a reconfigure arrived; re-read config and re-dial.
	}
}

// reloadConfig refreshes the pool size and reconnect policy from the control plane. On a load error it
// KEEPS the current (last-good) config — set in New to the env defaults and updated only on success — so
// a transient Postgres blip during a reconfigure never silently reverts a live-configured pool to env.
func (s *Service) reloadConfig(ctx context.Context) {
	if s.deps.ConfigSource == nil {
		return // static config (already seeded in New)
	}
	n, rc, err := s.deps.ConfigSource.Load(ctx, s.deps.ConnectorID)
	if err != nil {
		s.deps.Logger.WarnContext(ctx, "connector: config reload failed, keeping current config", "err", err)
		return
	}
	if n >= 1 {
		s.poolSize = n
	}
	s.reconnectCfg = rc
}

// park keeps the pod alive with a down link, polling the reconfigure generation until it changes (an
// Admin rebind) or ctx is cancelled. It returns nil on a generation change (the caller re-dials) and the
// ctx error on shutdown. With no StatusControl wired there is nothing to poll, so it blocks until ctx.
func (s *Service) park(ctx context.Context) error {
	if s.deps.StatusControl == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	ticker := time.NewTicker(s.statusInterval())
	defer ticker.Stop()
	baseline, err := s.deps.StatusControl.Gen(ctx, s.deps.ConnectorID)
	haveBaseline := err == nil
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			g, err := s.deps.StatusControl.Gen(ctx, s.deps.ConnectorID)
			if err != nil {
				continue // a Redis blip is not a reconfigure
			}
			if !haveBaseline {
				baseline, haveBaseline = g, true // first successful read establishes the baseline
				continue
			}
			if g != baseline {
				return nil // reconfigure requested while parked
			}
		}
	}
}

// statusInterval is the per-bind status / generation-poll cadence.
func (s *Service) statusInterval() time.Duration {
	if s.deps.StatusHeartbeat > 0 {
		return s.deps.StatusHeartbeat
	}
	return breakerHeartbeat
}

// isLinkDrop reports whether an error is a live bind dropping (the only thing the reconnect loop backs
// off and retries). Everything else propagates: a bind handshake REJECTION — a permanent bad password
// (ESME_RINVPASWD), or a transient SMSC system error — and a non-link fault such as a Kafka error, so a
// transient consumer blip restarts the pod rather than churning healthy SMSC binds.
func isLinkDrop(err error) bool {
	return errors.Is(err, errBindClosed)
}

// runOnce brings up the bind pool, then consumes mt.routed until ctx is cancelled, unbinding cleanly on
// exit. A failure to bind any member returns an error (the reconnect loop decides whether to retry); a
// per-message infrastructure failure leaves the offset uncommitted for reprocessing.
//
// The binds are watched independently of the consumer: if one drops while idle — no mt.routed flowing,
// so no Submit is in flight to surface the failure — the consumer would otherwise block on Kafka forever
// with a dead bind while the pod stayed Ready. When any bind dies runOnce flips readiness, tears the
// consumer down and returns an error so the caller re-dials the whole pool (never re-sharding a live
// message onto another bind, which would break §7.3 ordering).
func (s *Service) runOnce(ctx context.Context) error {
	n := s.poolSize
	if n < 1 {
		n = 1
	}
	binds := make([]*bind, 0, n)
	for i := 0; i < n; i++ {
		b, err := dialAndBind(ctx, s.deps.Bind, s.deps.Logger, s.handleDeliver)
		if err != nil {
			for _, prev := range binds {
				prev.Close() //nolint:contextcheck // cleanup unbind detaches from ctx, like the shutdown path
			}
			return fmt.Errorf("connectorpool: bind %d/%d: %w", i+1, n, err)
		}
		binds = append(binds, b)
	}
	// Close detaches from ctx on purpose: the unbind must be sent AFTER ctx is cancelled (that is
	// what triggers the drain), on its own bounded context, exactly like observability's tracing drain.
	defer func() { //nolint:contextcheck // deliberate detach for the shutdown unbind (see Close)
		for _, b := range binds {
			b.Close()
		}
	}()

	s.bound.Store(true)
	s.setLink(linkUp)
	defer s.bound.Store(false)

	consumerCtx, cancel := context.WithCancel(ctx)
	// The heartbeats read s.breakers and binds; join them BEFORE this cycle returns (and thus before the
	// next cycle reassigns s.breakers or Close()s the binds). Deferred first so it runs AFTER cancel (LIFO)
	// — cancel stops the heartbeats, hbWG.Wait joins them, then the bind-Close defer runs.
	var hbWG sync.WaitGroup
	defer hbWG.Wait()
	defer cancel()

	// Per-bind circuit breakers (step-121), fed by each bind's submit outcomes, published to the
	// cross-pod aggregate by a heartbeat (step-122). Created here because the pool size is known now.
	if s.deps.Breaker != nil {
		s.breakers = make([]*breaker.Breaker, n)
		for i := range s.breakers {
			s.breakers[i] = breaker.New(s.deps.BreakerConfig, nil)
		}
		hbWG.Add(1)
		go func() { defer hbWG.Done(); s.runBreakerHeartbeat(consumerCtx) }()
	}

	// Runtime-status heartbeat + reconfigure poll (step-128b): publishes each bind's link_status +
	// in_flight for the Admin API, and closes reconfigure when the generation changes (an Admin rebind /
	// resize / policy change), so the select below tears the cycle down cleanly and re-dials.
	reconfigure := make(chan struct{})
	if s.deps.StatusControl != nil {
		var once sync.Once
		hbWG.Add(1)
		go func() {
			defer hbWG.Done()
			s.runStatusHeartbeat(consumerCtx, binds, func() { once.Do(func() { close(reconfigure) }) })
		}()
	}

	// A single signal fired by whichever bind dies first (idle drop, enquire_link timeout, peer close).
	anyDropped := make(chan struct{})
	var dropOnce sync.Once
	for _, b := range binds {
		go func(b *bind) {
			<-b.done
			dropOnce.Do(func() { close(anyDropped) })
		}(b)
	}

	consumerErr := make(chan error, 1)
	go func() { consumerErr <- s.deps.Consumer.RunBatch(consumerCtx, s.batchHandler(binds)) }()

	select {
	case err := <-consumerErr:
		return err
	case <-anyDropped:
		// A bind died on its own. Take the pod out of rotation, unwind the consumer, and surface the
		// failure so the reconnect loop backs off and re-dials the whole pool.
		s.bound.Store(false)
		s.setLink(linkReconnecting)
		cancel()
		<-consumerErr
		return fmt.Errorf("connectorpool: smsc bind dropped: %w", errBindClosed)
	case <-reconfigure:
		// An Admin change: tear down cleanly (cancel consumer → drain/commit the batch → unbind on
		// defer), then re-dial with fresh config. The clean path — not the "bind dead" path — avoids a
		// deliberate duplicate on a forced rebind.
		s.bound.Store(false)
		s.setLink(linkReconnecting)
		cancel()
		<-consumerErr
		return errReconfigure
	}
}

// runStatusHeartbeat publishes each bind's link_status + in_flight on a fixed cadence and polls the
// reconfigure generation, calling onReconfigure once when it changes. It stops on ctx (one dial cycle).
func (s *Service) runStatusHeartbeat(ctx context.Context, binds []*bind, onReconfigure func()) {
	ticker := time.NewTicker(s.statusInterval())
	defer ticker.Stop()
	id := s.deps.ConnectorID
	publish := func() {
		for i, b := range binds {
			if err := s.deps.StatusControl.PublishBind(ctx, id, s.podID, i, s.LinkStatus(), b.inFlight()); err != nil && ctx.Err() == nil {
				s.deps.Logger.WarnContext(ctx, "connector: publish bind status failed", "bind_index", i, "err", err)
			}
		}
	}
	publish() // publish immediately so status is fresh without waiting a full tick
	// The baseline is this cycle's OWN first successful generation read, so a Redis blip during
	// reloadConfig cannot make us re-dial healthy binds in a tight loop.
	baseline, err := s.deps.StatusControl.Gen(ctx, id)
	haveBaseline := err == nil
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
			g, err := s.deps.StatusControl.Gen(ctx, id)
			if err != nil {
				continue
			}
			if !haveBaseline {
				baseline, haveBaseline = g, true
				continue
			}
			if g != baseline {
				onReconfigure()
				return
			}
		}
	}
}

// batchHandler shards a poll batch across the pool: each record goes to bind[hash(message_id) % N], and
// each shard's records are processed sequentially by a dedicated goroutine so the binds run
// concurrently while a message's segments stay ordered on one bind. A record that fails halts the rest
// of ITS shard (later records marked errored, not committed) so a segment can never overtake an earlier
// one on redelivery (§7.3); other shards are unaffected.
func (s *Service) batchHandler(binds []*bind) kafka.BatchHandler {
	n := len(binds)
	return func(ctx context.Context, recs []kafka.Record) []error {
		results := make([]error, len(recs))
		shards := make(map[int][]int, n) // shard index -> record indices, in batch (offset) order
		for i, rec := range recs {
			sh := shardIndex(rec.Key, n)
			shards[sh] = append(shards[sh], i)
		}
		var wg sync.WaitGroup
		for sh, idxs := range shards {
			wg.Add(1)
			go func(sh int, idxs []int) {
				defer wg.Done()
				for pos, i := range idxs {
					if err := s.processOne(ctx, binds[sh], sh, recs[i]); err != nil {
						results[i] = err
						// Stop this shard: leave every later record of this shard unprocessed and uncommitted
						// so redelivery replays them in order behind the failure.
						for _, j := range idxs[pos+1:] {
							results[j] = errShardHalted
						}
						return
					}
				}
			}(sh, idxs)
		}
		wg.Wait()
		return results
	}
}

// runBreakerHeartbeat republishes every bind's current breaker state on a fixed cadence until ctx is
// cancelled. One periodic report (rather than one per submit) keeps the hot path off Redis while still
// keeping each sub-bind alive in the aggregate quorum and surfacing time-driven transitions (a State()
// read advances open → half_open). It starts no work when no breaker is wired.
func (s *Service) runBreakerHeartbeat(ctx context.Context) {
	interval := s.deps.BreakerHeartbeat
	if interval <= 0 {
		interval = breakerHeartbeat
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	connectorID := s.deps.ConnectorID.String()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			worst := breaker.Closed
			for i, b := range s.breakers {
				st := b.State()
				if severity(st) > severity(worst) {
					worst = st
				}
				if _, err := s.deps.Breaker.Report(ctx, connectorID, i, st); err != nil && ctx.Err() == nil {
					s.deps.Logger.WarnContext(ctx, "connector: breaker state report failed", "bind_index", i, "err", err)
				}
			}
			// Published from HERE, not from a supervisor goroutine: s.breakers is reassigned by every dial
			// cycle, and only the heartbeats are joined before that happens (see runOnce). A poller outside
			// the cycle reads the slice header while it is being rewritten — a data race, reproduced under
			// -race, not merely a stale value.
			//
			// One series per CONNECTOR, not per bind: a bind pool has several breakers under one connector id,
			// so reporting each in turn would leave whichever was polled last, an arbitrary answer. The WORST
			// bind state is reported instead — one bind open means the connector is degraded, which is what an
			// operator needs to see — and the per-bind detail lives in the bind status hash (step-128).
			state := worst.String()
			if s.deps.BreakerGauge != nil {
				s.deps.BreakerGauge.SetConnectorBreakerState(connectorID, state)
			}
			s.stream(func(e StreamEmitter) {
				e.SetOneHot("connector_breaker_state", metricstream.Labels{"connector_id": connectorID},
					"state", breakerStates, state)
			})
		}
	}
}

// feedBreaker records a submit outcome into the bind's local breaker. status is the submit_sm_resp
// command_status; a submitErr (transport failure, no response) is a connector-health failure. It is a
// no-op when no breaker is wired.
func (s *Service) feedBreaker(bindIndex int, status uint32, submitErr bool) {
	if s.breakers == nil {
		return
	}
	b := s.breakers[bindIndex]
	if submitErr {
		b.RecordFailure()
		return
	}
	b.Record(status)
}

// shardIndex maps a record's partition key (the message id, shared by every segment) to a bind, so all
// of a message's segments hash to the same bind and stay ordered. FNV-1a is enough — the property
// needed is only a stable, uniform in-run mapping, not cryptographic strength or cross-run stability.
func shardIndex(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n)) //nolint:gosec // n is 1..32, the modulo result fits an int
}

// breakerStates is the enum published as a one-hot gauge. It mirrors internal/observability/metrics, so the
// realtime feed and Prometheus name the same states — a dashboard and Grafana must not disagree on what
// "half_open" is called.
var breakerStates = []string{
	metrics.BreakerStateClosed,
	metrics.BreakerStateOpen,
	metrics.BreakerStateHalfOpen,
}

// severity orders breaker states from healthy to degraded, so the worst of a bind pool can be picked.
func severity(s breaker.State) int {
	switch s {
	case breaker.Open:
		return 2
	case breaker.HalfOpen:
		return 1
	default:
		return 0
	}
}
