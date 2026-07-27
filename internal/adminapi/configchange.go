package adminapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// publishTimeout bounds the config-change PUBLISH so a slow or hung Redis cannot add unbounded latency
// to an admin mutation (the publish runs in the request path). A healthy PUBLISH is sub-millisecond.
const publishTimeout = 5 * time.Second

// ConfigChangePublisher publishes a coarse config-changed event. *redisstore.PubSubPublisher satisfies
// it. The interface lives here, consumer-side.
type ConfigChangePublisher interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

// statusRecorder captures the response status so the middleware can publish only on success.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer's Flusher/Hijacker/etc., so wrapping
// the whole Admin API here never silently disables streaming or hijacking on any future endpoint.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// PublishConfigChanges wraps h so every successful mutating request (a non-GET/HEAD method that
// returns a 2xx) publishes one coarse event on channel — the single control-plane trigger that
// config-sync coalesces into a data-plane invalidation. It is one seam for the whole Admin API, so no
// individual handler has to remember to announce its write.
//
// A publish failure is logged, never surfaced: the database commit already happened, so a lost
// invalidation must not fail the admin request (other pods rebuild on the next change; a periodic
// resync is the eventual hardening). Reads never publish. The publish uses a cancel-detached context
// so a client that disconnects right after its write does not drop the invalidation.
func PublishConfigChanges(h http.Handler, pub ConfigChangePublisher, channel string, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rec, r)

		if !mutating(r.Method, r.URL.Path) || rec.status < 200 || rec.status >= 300 {
			return
		}
		// Detach from the request (survive a client disconnect) but keep a bound so a hung Redis cannot
		// stall the mutation indefinitely.
		pubCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), publishTimeout)
		defer cancel()
		if err := pub.Publish(pubCtx, channel, []byte(`{"reason":"admin"}`)); err != nil {
			logger.WarnContext(r.Context(), "config-change publish failed; invalidation skipped", "err", err)
		}
	})
}

// readOnlyPostSuffixes are POST endpoints that change no control-plane state — diagnostics that must
// NOT trigger a data-plane invalidation despite being POSTs (they are AdminRead-scoped).
var readOnlyPostSuffixes = []string{"/validate", "/test"}

// mutating reports whether a request changes control-plane state. A read method never does; a POST to
// a declared read-only diagnostic (validate/test) does not either — otherwise an operator iterating on
// a script would fire a fleet-wide snapshot rebuild on every dry-run.
func mutating(method, path string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	case http.MethodPost:
		for _, suffix := range readOnlyPostSuffixes {
			if strings.HasSuffix(path, suffix) {
				return false
			}
		}
		return true
	default:
		return true
	}
}
