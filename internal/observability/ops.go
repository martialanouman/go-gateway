package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/martialanouman/go-gateway/internal/config"
)

// readinessTimeout bounds a whole /readyz evaluation. A probe that hangs must not hang the
// endpoint: a kubelet that gets no answer treats the pod as unready anyway, so answering "not
// ready" quickly is strictly better than answering slowly.
const readinessTimeout = 3 * time.Second

// ReadinessCheck is one vital dependency of a service.
//
// Register a dependency here only if the service cannot do its job without it (plan §1.5).
// Readiness mirrors the failure policy: router-svc stays ready with Redis down — it fails closed
// on throttling and messages stay durable in Kafka — but goes unready when Kafka is gone, since
// nothing can be durably accepted then. Registering a non-vital dependency is a real outage risk:
// it removes healthy pods from the load balancer over a degradation they could have absorbed.
type ReadinessCheck struct {
	// Name identifies the dependency in the /readyz body. Keep it short and stable ("kafka").
	Name string
	// Probe reports whether the dependency is reachable. It must honour ctx and must be cheap:
	// a kubelet calls it every few seconds, for every pod.
	Probe func(ctx context.Context) error
}

// PingCheck builds a ReadinessCheck that calls ping under a per-probe timeout. Every store adapter's
// readiness check is this exact shape (a timeout-bounded ping), so they delegate here rather than
// re-inlining it; each passes its own client's Ping.
func PingCheck(name string, timeout time.Duration, ping func(context.Context) error) ReadinessCheck {
	return ReadinessCheck{
		Name: name,
		Probe: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return ping(ctx)
		},
	}
}

// OpsServer serves the operations endpoints every service exposes on the ops port (plan §1.4):
//
//	/metrics  Prometheus exposition
//	/healthz  liveness — the process is alive; checks NOTHING else, so a dependency outage can
//	          never cause a restart loop
//	/readyz   readiness — the service's vital dependencies answer
//
// It is internal only: never exposed publicly, absent from the OpenAPI contracts.
type OpsServer struct {
	srv      *http.Server
	registry *prometheus.Registry
	checks   []ReadinessCheck
	log      *slog.Logger

	// addr is the configured listen address and is immutable after construction. bound holds the
	// address actually assigned once Run has listened — it is written by Run and read by Addr
	// from another goroutine, so it is atomic rather than a plain field.
	addr  string
	bound atomic.Pointer[string]
}

// NewOpsServer builds the ops server for cfg with the given vital dependency checks. Passing no
// check makes /readyz report ready as soon as the process is up, which is correct only for a
// service that genuinely has no vital dependency.
//
// It returns an error on a duplicate or unnamed check: a /readyz body with two "kafka" entries
// tells an operator nothing.
func NewOpsServer(cfg config.Config, log *slog.Logger, checks ...ReadinessCheck) (*OpsServer, error) {
	seen := make(map[string]struct{}, len(checks))
	for _, c := range checks {
		if c.Name == "" {
			return nil, errors.New("new ops server: readiness check has no name")
		}
		if c.Probe == nil {
			return nil, fmt.Errorf("new ops server: readiness check %q has no probe", c.Name)
		}
		if _, dup := seen[c.Name]; dup {
			return nil, fmt.Errorf("new ops server: duplicate readiness check %q", c.Name)
		}
		seen[c.Name] = struct{}{}
	}

	// A dedicated registry rather than the global default: metrics are then owned by the server
	// that exposes them, and two servers in one test binary cannot collide.
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	o := &OpsServer{
		registry: registry,
		checks:   checks,
		log:      log,
		addr:     fmt.Sprintf(":%d", cfg.OpsPort),
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	mux.HandleFunc("GET /healthz", o.handleHealthz)
	mux.HandleFunc("GET /readyz", o.handleReadyz)

	o.srv = &http.Server{
		Addr:              o.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return o, nil
}

// Registry returns the Prometheus registry backing /metrics. Services register their collectors
// on it at startup. Labels must stay bounded: never an MSISDN, a message_id or a body
// (guide de codage §12).
func (o *OpsServer) Registry() *prometheus.Registry { return o.registry }

// Addr returns the address the server listens on. Before Run it is the configured address; once
// Run has bound its listener it is the address actually assigned, which is what a caller using
// port 0 needs. It is safe to call from any goroutine.
func (o *OpsServer) Addr() string {
	if bound := o.bound.Load(); bound != nil {
		return *bound
	}
	return o.addr
}

// Run serves until ctx is cancelled, then drains within cfg.ShutdownTimeout. It returns nil on a
// clean shutdown: a cancelled context is the expected way for a service to stop, not an error.
func (o *OpsServer) Run(ctx context.Context, shutdownTimeout time.Duration) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", o.addr)
	if err != nil {
		return fmt.Errorf("listen on ops port %s: %w", o.addr, err)
	}

	bound := ln.Addr().String()
	o.bound.Store(&bound)

	errCh := make(chan error, 1)
	go func() {
		// Serve always returns a non-nil error; ErrServerClosed is the normal one.
		errCh <- o.srv.Serve(ln)
	}()

	o.log.InfoContext(ctx, "ops server listening", "addr", bound)

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve ops: %w", err)
	case <-ctx.Done():
		// nolint:contextcheck // Detaching is the point: see shutdown's comment. Draining on the
		// context that just fired would abort instantly and drop in-flight requests.
		return o.shutdown(shutdownTimeout)
	}
}

// shutdown drains in-flight requests within timeout, detached from the cancelled service context
// — a shutdown driven by a context that is already done would abort instantly and drop them.
func (o *OpsServer) shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), timeout)
	defer cancel()

	if err := o.srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown ops server: %w", err)
	}
	return nil
}

// handleHealthz answers liveness. It checks NOTHING: liveness failure means the kubelet restarts
// the pod, and restarting a healthy process because a database is down turns a dependency outage
// into a crash loop (plan §1.5).
func (o *OpsServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyzResponse is the /readyz body. It names each vital dependency and why it failed, so an
// operator sees which one is down without opening a trace.
type readyzResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// handleReadyz answers readiness by probing every vital dependency concurrently: probes are
// independent, and running them in series would make the endpoint as slow as their sum.
func (o *OpsServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	results := make([]error, len(o.checks))
	var wg sync.WaitGroup
	wg.Add(len(o.checks))
	for i, c := range o.checks {
		go func() {
			defer wg.Done()
			results[i] = c.Probe(ctx)
		}()
	}
	wg.Wait()

	resp := readyzResponse{Status: "ok", Checks: make(map[string]string, len(o.checks))}
	code := http.StatusOK
	for i, err := range results {
		if err != nil {
			resp.Status = "unavailable"
			resp.Checks[o.checks[i].Name] = err.Error()
			code = http.StatusServiceUnavailable
			continue
		}
		resp.Checks[o.checks[i].Name] = "ok"
	}

	if code != http.StatusOK {
		o.log.WarnContext(ctx, "readiness check failed", "checks", resp.Checks)
	}
	writeJSON(w, code, resp)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// The response is already committed by WriteHeader, so a write failure cannot be reported to
	// the client; the connection is gone. Nothing useful to do but drop it.
	_ = json.NewEncoder(w).Encode(body)
}

// TCPDialCheck builds a readiness check that reports whether addr accepts a TCP connection.
//
// It is deliberately protocol-blind: it proves the broker's port is reachable, not that a
// protocol handshake succeeds. That is the right depth for M0 — it catches the outage that
// matters (the dependency is gone, the network is partitioned) without pulling a client library
// into a milestone that has no business talking to one. Replace it with a real client ping when
// that client lands.
func TCPDialCheck(name, addr string, timeout time.Duration) ReadinessCheck {
	return ReadinessCheck{
		Name: name,
		Probe: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			var d net.Dialer
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return fmt.Errorf("dial %s: %w", addr, err)
			}
			_ = conn.Close() // best-effort: the probe only needed the handshake
			return nil
		},
	}
}

// AnyTCPDialCheck builds a readiness check that passes when at least ONE of addrs is reachable.
//
// This is the right shape for a Kafka broker list: a cluster with one broker down is still a
// working cluster, and going unready over it would remove every pod from the load balancer during
// a routine rolling restart of the brokers.
func AnyTCPDialCheck(name string, addrs []string, timeout time.Duration) ReadinessCheck {
	return ReadinessCheck{
		Name: name,
		Probe: func(ctx context.Context) error {
			if len(addrs) == 0 {
				return errors.New("no addresses configured")
			}

			// Every address gets the full timeout, and they run concurrently. Dialling in sequence
			// on one shared budget lets a single blackholed broker — a firewall dropping the SYN, a
			// partition — consume all of it, leaving the healthy brokers to fail instantly on an
			// expired context: a working cluster reported unreachable, which is the outage this
			// check exists to avoid. Giving each dial its own budget sequentially would instead
			// stretch the probe to len(addrs)*timeout and overrun the kubelet's own probe timeout.
			// Concurrent dials keep the whole probe bounded by timeout.
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			var wg sync.WaitGroup
			errs := make([]error, len(addrs))
			for i, addr := range addrs {
				wg.Add(1)
				go func() {
					defer wg.Done()

					var d net.Dialer
					conn, err := d.DialContext(ctx, "tcp", addr)
					if err != nil {
						// Each goroutine owns its own index, so the slice needs no lock, and
						// wg.Wait orders every write before the reads below.
						errs[i] = fmt.Errorf("dial %s: %w", addr, err)
						return
					}
					_ = conn.Close() // best-effort: the probe only needed the handshake
					cancel()         // one reachable broker is enough; stop the others dialling
				}()
			}
			// Bounded by the context above, so no goroutine outlives the probe.
			wg.Wait()

			for _, err := range errs {
				if err == nil {
					return nil
				}
			}
			return fmt.Errorf("no reachable address: %w", errors.Join(errs...))
		},
	}
}
