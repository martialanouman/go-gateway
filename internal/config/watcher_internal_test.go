package config

import (
	"testing"
	"time"
)

// TestResyncDelayJittersTenPercentEitherWay: the draws cover both sides of the period and never leave
// ±10 %. A fixed delay would keep every pod that one notification aligned rebuilding at the same second.
func TestResyncDelayJittersTenPercentEitherWay(t *testing.T) {
	const period = time.Minute
	lo, hi := period, period
	for range 1000 {
		d := resyncDelay(period)
		if d < period*9/10 || d > period*11/10 {
			t.Fatalf("resyncDelay(%v) = %v, outside ±10 %%", period, d)
		}
		lo, hi = min(lo, d), max(hi, d)
	}
	if lo > period*95/100 || hi < period*105/100 {
		t.Errorf("1000 draws span only [%v, %v]: the jitter does not cover ±10 %% of %v", lo, hi, period)
	}
}
