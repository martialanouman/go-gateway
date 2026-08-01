package adminapi

import "testing"

func TestMaskMSISDN(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  string
		grant bool
	}{
		{"revealed for a granted role", "33612345678", "33612345678", true},
		{"masked keeps the country prefix and the tail", "33612345678", "3361*****78", false},
		{"a short number keeps only its tail", "1234", "**34", false},
		{"nothing to mask", "", "", false},
		// Two subscribers of one operator must not collapse to the same mask: an operator correlating
		// complaints would otherwise see one number where there are two.
		{"distinguishable neighbours", "33612345679", "3361*****79", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskMSISDN(tc.in, tc.grant); got != tc.want {
				t.Errorf("maskMSISDN(%q, reveal=%v) = %q, want %q", tc.in, tc.grant, got, tc.want)
			}
		})
	}
}

// TestMaskMSISDNRevealsNothingByDefault: the zero value of the grant must be "masked". A refactor that
// accidentally passes an unset bool must fail closed.
func TestMaskMSISDNRevealsNothingByDefault(t *testing.T) {
	var granted bool
	if got := maskMSISDN("33612345678", granted); got == "33612345678" {
		t.Fatal("an unset grant revealed the number")
	}
}

// TestMaskAddressesMasksTheSubscriberSide. Masking both addresses is wrong in both directions: on an MT the
// source is an alphanumeric sender ID ("GATEWAY" → "GATE*AY" — no privacy gained, the most diagnostic field
// lost), and on an MO the destination is the operator's own inbound number while the real subscriber is the
// source.
func TestMaskAddressesMasksTheSubscriberSide(t *testing.T) {
	tests := []struct {
		name       string
		direction  string
		source     string
		dest       string
		reveal     bool
		wantSource string
		wantDest   string
	}{
		{
			name:      "MT masks the destination, keeps the sender id",
			direction: "mt", source: "GATEWAY", dest: "33612345678",
			wantSource: "GATEWAY", wantDest: "3361*****78",
		},
		{
			name:      "MO masks the source, keeps the inbound number",
			direction: "mo", source: "33612345678", dest: "36000",
			wantSource: "3361*****78", wantDest: "36000",
		},
		{
			name:      "revealed leaves both alone",
			direction: "mt", source: "GATEWAY", dest: "33612345678", reveal: true,
			wantSource: "GATEWAY", wantDest: "33612345678",
		},
		{
			// An unknown direction must fail closed: mask both rather than expose the wrong one.
			name:      "unknown direction masks both",
			direction: "", source: "33611111111", dest: "33612345678",
			wantSource: "3361*****11", wantDest: "3361*****78",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source, dest := maskAddresses(tc.direction, tc.source, tc.dest, tc.reveal)
			if source != tc.wantSource {
				t.Errorf("source = %q, want %q", source, tc.wantSource)
			}
			if dest != tc.wantDest {
				t.Errorf("dest = %q, want %q", dest, tc.wantDest)
			}
		})
	}
}
