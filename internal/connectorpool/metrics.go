package connectorpool

import (
	"time"

	"github.com/martialanouman/go-gateway/internal/metricstream"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// observeSubmit counts a terminal submit outcome on both sinks — the realtime feed and Prometheus — from
// one (status, code) so the two can never disagree.
func (s *Service) observeSubmit(resp smpp.PDU, e2e time.Duration) {
	// Both label values are closed vocabularies from the code — a connector id from the control plane and a
	// platform Code — which is what keeps the emitter's series count bounded.
	// status is a closed two-value vocabulary and the code goes in the label meant for it — mixing an error
	// code into `status` would give one label name two meanings across the two legs of a message.
	connectorID := s.deps.ConnectorID.String()
	status, code := "ok", ""
	if resp.Status != smpp.StatusOK {
		status, code = "rejected", string(errs.CodeFromSMPPStatus(resp.Status))
	}
	s.stream(func(e StreamEmitter) {
		e.Add("submits_total", metricstream.Labels{"connector_id": connectorID, "status": status}, 1)
		if status == "rejected" {
			e.Add("submit_rejected_total", metricstream.Labels{"connector_id": connectorID, "code": code}, 1)
		}
	})
	if s.deps.Metrics != nil {
		s.deps.Metrics.SubmitsTotal.WithLabelValues(connectorID, status).Inc()
		// Same site, same labels, same values as submits_total, so _count and the counter stay
		// comparable and one status vocabulary covers both legs. Only terminal outcomes reach here: a
		// throttle redelivers above and a dead-letter returns earlier.
		//
		// That covers the dead-letter half of the NFR's carve-out (§1.2) and NOT the backpressure half:
		// a throttled attempt is not observed, but the message it belonged to is — on the redelivery
		// that finally succeeds, and the clock still runs from ageBase, so it carries the whole wait.
		// Same for the AIMD pacing above. Reading a throttling episode out of this histogram means
		// reading submit_rejected_total{code="rate_limited"} beside it.
		s.deps.Metrics.MessageE2EDuration.WithLabelValues(connectorID, status).Observe(e2e.Seconds())
		if status == "rejected" {
			s.deps.Metrics.SubmitRejectedTotal.WithLabelValues(connectorID, code).Inc()
		}
	}
}
