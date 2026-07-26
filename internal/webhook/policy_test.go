package webhook

import (
	"testing"
	"time"
)

// TestParseRetryPolicy covers the fallbacks: empty and malformed JSON yield the defaults, valid fields
// override, and out-of-range values are ignored.
func TestParseRetryPolicy(t *testing.T) {
	def := defaultRetryPolicy()

	cases := []struct {
		name string
		raw  string
		want RetryPolicy
	}{
		{"empty -> defaults", "", def},
		{"malformed -> defaults", "{not json", def},
		{"zero attempts ignored -> default", `{"max_attempts":0}`, def},
		{"multiplier below 1 ignored", `{"multiplier":0.5}`, def},
		{
			"full override",
			`{"max_attempts":7,"initial_backoff_ms":500,"max_backoff_ms":60000,"multiplier":3}`,
			RetryPolicy{MaxAttempts: 7, InitialBackoff: 500 * time.Millisecond, MaxBackoff: 60 * time.Second, Multiplier: 3},
		},
		{
			"max below initial is raised to initial",
			`{"initial_backoff_ms":5000,"max_backoff_ms":1000}`,
			RetryPolicy{MaxAttempts: def.MaxAttempts, InitialBackoff: 5 * time.Second, MaxBackoff: 5 * time.Second, Multiplier: def.Multiplier},
		},
		{
			"pathological values are clamped",
			`{"max_attempts":1000000,"max_backoff_ms":999999999,"initial_backoff_ms":999999999}`,
			RetryPolicy{MaxAttempts: maxAttemptsCap, InitialBackoff: maxBackoffMs * time.Millisecond, MaxBackoff: maxBackoffMs * time.Millisecond, Multiplier: def.Multiplier},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseRetryPolicy([]byte(c.raw))
			if got != c.want {
				t.Errorf("parseRetryPolicy(%q) = %+v, want %+v", c.raw, got, c.want)
			}
		})
	}
}
