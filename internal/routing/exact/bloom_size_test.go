package exact

import (
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
)

// TestBloomSizePerMillionEntries pins the memory the L0 filter actually costs, because both the spec
// (§724) and this package's own comment carried a figure that belonged to a different false-positive
// rate: "~1.2 MB per million entries" is what 0.01 buys, not the 0.001 the code has always used. The
// real cost is ~1.8 MB per million — 50% more than every document claimed, for two milestones.
//
// The rate is worth keeping and the figure is what was wrong. Since step-250e a Bloom false positive no
// longer costs a cheap Redis miss but a durable Postgres lookup, so a tighter rate is more justified
// now than when it was chosen: 0.001 means ~8 lookups/s at the 8000 SMS/s target, 0.01 would mean ~80.
//
// This test exists so the number cannot drift again: it fails both if the filter grows and if someone
// loosens the rate to chase the old figure.
func TestBloomSizePerMillionEntries(t *testing.T) {
	const million = 1_000_000
	bytesPerMillion := bloom.NewWithEstimates(million, bloomFP).Cap() / 8

	const (
		lo = 1_700_000 // ~1.7 MB
		hi = 1_900_000 // ~1.9 MB
	)
	if bytesPerMillion < lo || bytesPerMillion > hi {
		t.Errorf("filter for %d entries at fp=%v is %d bytes (%.2f MB); want within [%d, %d]. "+
			"Below the band means the false-positive rate was loosened, and every extra false positive "+
			"is now a Postgres round trip on the hot path; above it means the filter grew",
			million, bloomFP, bytesPerMillion, float64(bytesPerMillion)/1e6, lo, hi)
	}
}

// TestBloomSizeGrowsLinearly: the per-million figure is only usable as a budget if it scales. A national
// MNP base is several million numbers, and the operator sizing a pod needs to multiply.
func TestBloomSizeGrowsLinearly(t *testing.T) {
	one := bloom.NewWithEstimates(1_000_000, bloomFP).Cap()
	five := bloom.NewWithEstimates(5_000_000, bloomFP).Cap()

	if ratio := float64(five) / float64(one); ratio < 4.9 || ratio > 5.1 {
		t.Errorf("5M/1M filter size ratio = %.2f, want ~5 (a per-million budget that does not scale "+
			"cannot be multiplied by an operator sizing a pod)", ratio)
	}
}
