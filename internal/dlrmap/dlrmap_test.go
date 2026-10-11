package dlrmap

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestTTLForValidity covers the derivation rule (step-287o): no usable validity keeps the mapping for the
// configured retention; a relative one the client asked for is honoured, validity+margin clamped to [1h, 72h],
// even past the retention; an absolute one is not interpreted and keeps the 72h cap (fail-long).
func TestTTLForValidity(t *testing.T) {
	str := func(s string) *string { return &s }
	const retention = 12 * time.Hour

	cases := []struct {
		name string
		in   *string
		want time.Duration
	}{
		{"nil keeps the retention", nil, retention},
		{"empty keeps the retention", str(""), retention},
		{"garbage keeps the retention", str("not-a-validity!"), retention},
		{"relative zero keeps the retention", str("000000000000000R"), retention},
		{"non-digit field keeps the retention", str("0000XX000000000R"), retention},
		{"wrong length keeps the retention", str("000001R"), retention},
		{"absolute (+ suffix) keeps the cap", str("251023120000000+"), maxTTL},
		{"absolute (- suffix) keeps the cap", str("251023120000004-"), maxTTL},
		{"relative 2 days -> past the retention", str("000002000000000R"), 48*time.Hour + ttlMargin},
		{"relative 2 hours -> under the retention", str("000000020000000R"), 2*time.Hour + ttlMargin},
		{"relative 5 minutes -> 5m + margin", str("000000000500000R"), 5*time.Minute + ttlMargin},
		{"relative 10 days -> capped to max", str("000010000000000R"), maxTTL},
		{"relative 1 year -> capped to max", str("010000000000000R"), maxTTL},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ttlForValidity(c.in, retention); got != c.want {
				t.Errorf("ttlForValidity(%v) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

// TestTTLNeverBelowMinOrAboveMax is the safety invariant: whatever the input, the TTL stays in the
// bounded window, so a mapping never expires before a receipt can plausibly arrive nor lingers past
// the SMS maximum.
func TestTTLNeverBelowMinOrAboveMax(t *testing.T) {
	inputs := []string{
		"", "000000000100000R", "000000010000000R", "000030000000000R", "990000000000000R", "junk",
	}
	for _, in := range inputs {
		in := in
		got := ttlForValidity(&in, 24*time.Hour)
		if got < minTTL || got > maxTTL {
			t.Errorf("ttlForValidity(%q) = %v, out of [%v, %v]", in, got, minTTL, maxTTL)
		}
	}
}

// TestKeyFormat pins the key shape step-044 must rebuild: one Cluster hash tag over the whole
// (connector_id, smsc_msg_id) composite.
func TestKeyFormat(t *testing.T) {
	id := uuid.MustParse("018f8e00-0000-7000-8000-000000000001")
	got := key(id, "0000000000000001")
	want := "dlrmap:{018f8e00-0000-7000-8000-000000000001:0000000000000001}"
	if got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}
