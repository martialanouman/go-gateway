package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestVariableLabelsOfReadsWhatPrometheusDeclares is the CANARY for the whole guard. Prometheus exposes a
// Desc's variable labels only through Desc.String(), so the guard parses that rendering. If a client_golang
// upgrade changes the format, this test fails loudly — instead of the parser quietly seeing "no labels" and
// waving every metric through.
func TestVariableLabelsOfReadsWhatPrometheusDeclares(t *testing.T) {
	desc := prometheus.NewDesc("probe_total", "canary", []string{"connector_id", "status"}, prometheus.Labels{"fixed": "1"})

	name, labels, err := variableLabelsOf(desc)
	if err != nil {
		t.Fatalf("variableLabelsOf: %v", err)
	}
	if name != "probe_total" {
		t.Errorf("name = %q, want probe_total", name)
	}
	if got := strings.Join(labels, ","); got != "connector_id,status" {
		t.Errorf("labels = %q, want connector_id,status", got)
	}
}

// TestVariableLabelsOfIgnoresConstLabels: const labels are fixed at construction, so they cannot explode
// cardinality and are none of the guard's business.
func TestVariableLabelsOfIgnoresConstLabels(t *testing.T) {
	desc := prometheus.NewDesc("probe_total", "canary", nil, prometheus.Labels{"msisdn": "33612345678"})

	_, labels, err := variableLabelsOf(desc)
	if err != nil {
		t.Fatalf("variableLabelsOf: %v", err)
	}
	if len(labels) != 0 {
		t.Errorf("labels = %v, want none", labels)
	}
}

// TestVariableLabelsOfIsNotFooledByHelpText: help is rendered before the variable labels and is attacker-shaped
// text (a developer can write anything). A parser that searched forward would read the help as the label list.
func TestVariableLabelsOfIsNotFooledByHelpText(t *testing.T) {
	desc := prometheus.NewDesc("probe_total", "beware: variableLabels: {msisdn}", []string{"status"}, nil)

	_, labels, err := variableLabelsOf(desc)
	if err != nil {
		t.Fatalf("variableLabelsOf: %v", err)
	}
	if got := strings.Join(labels, ","); got != "status" {
		t.Errorf("labels = %q, want status (the help text must not be read as labels)", got)
	}
}

// TestVariableLabelsOfRejectsAnUnknownRendering: an unrecognised format is an ERROR, never an empty label
// list. Failing open here would silently disable the guard.
func TestVariableLabelsOfRejectsAnUnknownRendering(t *testing.T) {
	if _, _, err := parseDescString("something client_golang has never rendered"); err == nil {
		t.Fatal("want an error on an unrecognised Desc rendering, got nil")
	}
}

func TestValidateLabelNames(t *testing.T) {
	tests := []struct {
		name    string
		labels  []string
		wantErr bool
	}{
		{"bounded vocabulary", []string{"connector_id", "status"}, false},
		{"no labels at all", nil, false},
		{"customer_id is bounded (operators, not end users)", []string{"customer_id"}, false},
		{"msisdn is the canonical explosion", []string{"msisdn"}, true},
		{"message_id is per-message", []string{"message_id"}, true},
		{"the body must never be a label (invariant a)", []string{"body"}, true},
		{"an unknown label is refused even when it looks harmless", []string{"widget"}, true},
		// One bad label among good ones must still fail: the check is per-label, not "any label is fine".
		{"one bad label spoils the metric", []string{"status", "msisdn"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateLabelNames("probe_total", tc.labels)
			if tc.wantErr != (err != nil) {
				t.Fatalf("ValidateLabelNames(%v) = %v, wantErr %v", tc.labels, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "probe_total") {
				t.Errorf("error %q should name the metric, so the fix is obvious", err)
			}
		})
	}
}

// TestForbiddenLabelsBeatTheAllowlist is what makes the guard hard to defeat: the denylist is consulted FIRST,
// so adding "msisdn" to the allowlist — the obvious way to silence a failing build — does not work.
func TestForbiddenLabelsBeatTheAllowlist(t *testing.T) {
	allowed["msisdn"] = struct{}{} // simulate someone "fixing" the guard by widening the allowlist
	t.Cleanup(func() { delete(allowed, "msisdn") })

	if err := ValidateLabelNames("probe_total", []string{"msisdn"}); err == nil {
		t.Fatal("msisdn passed once allow-listed: the denylist must win")
	}
}

// TestTheAllowlistItselfIsClean: no entry of the bounded vocabulary may be a forbidden name. This catches the
// mistake at its source rather than at the first registration.
func TestTheAllowlistItselfIsClean(t *testing.T) {
	for name := range allowed {
		if reason, bad := forbiddenReason(name); bad {
			t.Errorf("allowlist contains %q, which is forbidden: %s", name, reason)
		}
	}
}
