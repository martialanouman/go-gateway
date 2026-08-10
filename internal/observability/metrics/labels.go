// Package metrics holds the gateway's Prometheus metric catalogue and the guard that keeps its labels
// bounded.
//
// A Prometheus time series exists for every distinct combination of label values, so a label whose values
// come from traffic — an MSISDN, a message_id — creates one series per message. That is two failures at
// once: it takes the monitoring stack down under load, and it leaks message metadata into a store that is
// scraped, cached and dashboarded (invariant a forbids the body anywhere; a destination number is barely
// better).
//
// The guard is therefore not a lint. [Guard] wraps the Prometheus registry and checks twice:
//
//   - at REGISTRATION, against the labels a collector declares. Services register at startup with
//     MustRegister, so a bad label crashes the process on boot instead of quietly inflating a production
//     TSDB. A collector that declares nothing at all is refused too — it would be a hole straight through.
//   - at GATHER, against what is actually about to be served. A hand-written collector can declare one label
//     and emit another, and Prometheus does not notice (it identifies a Desc by name and constant labels
//     only). The offending family is dropped rather than published.
package metrics

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// allowed is the bounded label vocabulary (§15). Every entry names a dimension whose value set is fixed by
// configuration or by code — never by traffic. It is a var, not a const map, only so the guard's own tests
// can prove the denylist outranks it; treat it as read-only.
//
// Adding an entry is a deliberate act: ask "who chooses this value?". If the answer is "the sender", it does
// not belong here.
var allowed = map[string]struct{}{
	// Who: identities from the control plane. Their count is bounded by contracts signed, not by traffic.
	"customer_id":  {},
	"connector_id": {},
	"route_id":     {},
	// What happened: closed vocabularies from the code.
	"status":     {}, // accepted | rejected | failed…
	"state":      {}, // breaker / link state names
	"outcome":    {}, // ok | error
	"reason":     {}, // the error taxonomy, never free text
	"code":       {}, // the flat error code (§11.3)
	"result":     {}, // hit | miss
	"action":     {}, // capture | release — how the reaper settled a reservation
	"cause":      {}, // promhttp's own handler-error metric: gathering | encoding
	"version":    {}, // go_info's build version: one value per binary
	"event_type": {}, // DLR / MO event kinds
	"direction":  {}, // mt | mo
	// Where: named pieces of the deployment, declared in config or in code.
	"source":   {}, // rest | smpp
	"queue":    {}, // a Kafka topic name
	"filter":   {}, // a named Bloom filter
	"runtime":  {}, // js | lua
	"provider": {}, // an external billing provider name
	"subject":  {}, // system_id | ip — the KIND of throttled subject, never its value
	"scope":    {}, // customer | smpp_account | global
	"protocol": {}, // smpp | http
	"encoding": {}, // gsm7 | ucs2 | binary
}

// forbidden names dimensions that must never label a metric, whatever the allowlist says. Two families:
// per-message identifiers (unbounded by construction) and message content or addressing (a leak, not just a
// cardinality problem).
var forbidden = map[string]string{
	"msisdn":      "a phone number is one series per subscriber",
	"phone":       "a phone number is one series per subscriber",
	"number":      "a phone number is one series per subscriber",
	"to":          "the destination address is per-message and is message metadata",
	"from":        "the source address is per-message and is message metadata",
	"dest_addr":   "the destination address is per-message and is message metadata",
	"source_addr": "the source address is per-message and is message metadata",
	"recipient":   "the destination address is per-message and is message metadata",
	"sender_id":   "a sender id is chosen per submission, not by configuration",
	"message_id":  "one series per message",
	"msg_id":      "one series per message",
	"request_id":  "one series per request",
	"trace_id":    "one series per trace — correlate through traces, not metrics",
	"span_id":     "one series per span — correlate through traces, not metrics",
	"session_id":  "one series per session",
	"ip":          "one series per client address",
	"remote_ip":   "one series per client address",
	"body":        "the message body must never leave the ciphertext column (invariant a)",
	"text":        "the message body must never leave the ciphertext column (invariant a)",
	"content":     "the message body must never leave the ciphertext column (invariant a)",
	"payload":     "the message body must never leave the ciphertext column (invariant a)",
}

// forbiddenReason reports whether a label name is banned outright, and why.
func forbiddenReason(name string) (string, bool) {
	if reason, ok := forbidden[strings.ToLower(name)]; ok {
		return reason, true
	}
	return "", false
}

// ValidateLabelNames reports whether every variable label of a metric belongs to the bounded vocabulary.
// The denylist is consulted FIRST, so widening [allowed] — the tempting way to silence a failing build —
// cannot smuggle an unbounded label through.
func ValidateLabelNames(metric string, names []string) error {
	for _, name := range names {
		if reason, bad := forbiddenReason(name); bad {
			return fmt.Errorf("metric %q: label %q is forbidden: %s", metric, name, reason)
		}
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf(
				"metric %q: label %q is not in the bounded vocabulary; add it to internal/observability/metrics"+
					" only if its values come from configuration or code, never from traffic",
				metric, name,
			)
		}
	}
	return nil
}

// variableLabelsOf extracts a Desc's metric name and variable labels.
//
// client_golang keeps both unexported and offers no accessor, so the only reading available is the Desc's own
// String() rendering. [parseDescString] treats an unfamiliar rendering as an error rather than as "no
// labels", and TestVariableLabelsOfReadsWhatPrometheusDeclares pins the format against upgrades.
func variableLabelsOf(d *prometheus.Desc) (name string, labels []string, err error) {
	return parseDescString(d.String())
}

const (
	descPrefix       = `Desc{fqName: "`
	descVariableMark = `, variableLabels: {`
	descSuffix       = "}}"
)

// parseDescString reads `Desc{fqName: "n", help: "h", constLabels: {…}, variableLabels: {a,b}}`.
//
// It anchors on the LAST occurrence of the variableLabels marker and on the trailing brace: help and const
// label values are quoted free text rendered BEFORE, so a metric whose help mentions "variableLabels: {…}"
// would fool a forward search — and that is exactly how someone would hide a label from the guard.
func parseDescString(s string) (name string, labels []string, err error) {
	if !strings.HasPrefix(s, descPrefix) || !strings.HasSuffix(s, descSuffix) {
		return "", nil, fmt.Errorf("metrics: unrecognised Desc rendering %q; the label guard cannot read it", s)
	}
	rest := s[len(descPrefix):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return "", nil, fmt.Errorf("metrics: unrecognised Desc rendering %q; the label guard cannot read it", s)
	}
	name = rest[:end]

	start := strings.LastIndex(s, descVariableMark)
	if start < 0 {
		return "", nil, fmt.Errorf("metrics: unrecognised Desc rendering %q; the label guard cannot read it", s)
	}
	joined := s[start+len(descVariableMark) : len(s)-len(descSuffix)]
	if joined == "" {
		return name, nil, nil
	}
	for _, label := range strings.Split(joined, ",") {
		// A label carrying a value constraint renders as c(name); the constraint bounds the VALUES, which is
		// welcome, but the name is what we validate.
		labels = append(labels, strings.TrimSuffix(strings.TrimPrefix(label, "c("), ")"))
	}
	return name, labels, nil
}
