package controlplane

import "testing"

// TestEffectivePriority pins ADR-0020 §2: min(max(requested, default), ceiling) per category.
func TestEffectivePriority(t *testing.T) {
	cases := []struct {
		category  TrafficCategory
		requested int
		want      int
	}{
		{TrafficMarketing, 0, 0},
		{TrafficMarketing, 3, 0},
		{TrafficTransactional, 0, 1},
		{TrafficTransactional, 2, 2},
		{TrafficTransactional, 3, 2},
		{TrafficOTP, 0, 3},
		{TrafficOTP, 1, 3},
		{TrafficMarketing, -1, 0},
		{"", 3, 0},
	}
	for _, c := range cases {
		if got := c.category.EffectivePriority(c.requested); got != c.want {
			t.Errorf("%q.EffectivePriority(%d) = %d, want %d", c.category, c.requested, got, c.want)
		}
	}
}

func TestRank(t *testing.T) {
	for category, want := range map[TrafficCategory]int{TrafficMarketing: 0, TrafficTransactional: 1, TrafficOTP: 2, "": 0} {
		if got := category.Rank(); got != want {
			t.Errorf("%q.Rank() = %d, want %d", category, got, want)
		}
	}
}
