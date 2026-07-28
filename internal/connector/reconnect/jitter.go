package reconnect

import (
	"math/rand/v2"
	"time"
)

// jitter randomises d by ± pct%, uniformly, so many pods reconnecting after the same SMSC outage do not
// retry in lockstep (thundering herd). pct 0 (or a non-positive d) returns d unchanged. It is not
// security-sensitive, so the default PRNG is fine.
func jitter(d time.Duration, pct int) time.Duration {
	if pct <= 0 || d <= 0 {
		return d
	}
	span := float64(pct) / 100.0
	//nolint:gosec // G404: jitter de-synchronises reconnect backoff across pods, it is not a secret
	factor := 1 + (rand.Float64()*2-1)*span // uniform in [1-span, 1+span]
	j := time.Duration(float64(d) * factor)
	if j < 0 {
		return 0
	}
	return j
}
