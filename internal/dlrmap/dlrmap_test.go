package dlrmap

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestTTLForValidity covers the derivation rule: a parsable relative period yields validity+margin
// clamped to [1h, 72h]; everything dubious falls back to the 72h cap (fail-long).
func TestTTLForValidity(t *testing.T) {
	str := func(s string) *string { return &s }

	cases := []struct {
		name string
		in   *string
		want time.Duration
	}{
		{"nil falls back to max", nil, maxTTL},
		{"empty falls back to max", str(""), maxTTL},
		{"absolute (+ suffix, 16 chars) falls back to max", str("251023120000000+"), maxTTL},
		{"garbage falls back to max", str("not-a-validity!"), maxTTL},
		{"relative 1 day -> 1d + margin", str("000001000000000R"), 24*time.Hour + ttlMargin},
		{"relative 2 hours -> 2h + margin", str("000000020000000R"), 2*time.Hour + ttlMargin},
		{"relative 5 minutes -> 5m + margin", str("000000000500000R"), 5*time.Minute + ttlMargin},
		{"relative 10 days -> capped to max", str("000010000000000R"), maxTTL},
		{"relative 1 year -> capped to max", str("010000000000000R"), maxTTL},
		{"relative zero -> falls back to max", str("000000000000000R"), maxTTL},
		{"non-digit field -> falls back to max", str("0000XX000000000R"), maxTTL},
		{"wrong length -> falls back to max", str("000001R"), maxTTL},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ttlForValidity(c.in); got != c.want {
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
		got := ttlForValidity(&in)
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
