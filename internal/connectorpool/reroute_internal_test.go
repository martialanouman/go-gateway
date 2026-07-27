package connectorpool

import (
	"testing"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestClassifyReroute pins the business table: which SMSC rejections warrant trying another connector.
func TestClassifyReroute(t *testing.T) {
	cases := []struct {
		name   string
		status uint32
		want   rerouteClass
	}{
		{"system error → failover", errs.StatusSysErr, failover},
		{"submit failed → failover", errs.StatusSubmitFail, failover},
		{"bind failed → failover", errs.StatusBindFail, failover},
		{"queue full → keep same (backpressure, redeliver in place)", errs.StatusMsgQueueFull, keepSame},
		{"throttled → keep same (backpressure)", errs.StatusThrottled, keepSame},
		{"invalid destination → terminal", errs.StatusInvalidDstAddr, terminal},
		{"invalid source → terminal", errs.StatusInvalidSrcAddr, terminal},
		{"invalid length → terminal", errs.StatusInvalidMsgLen, terminal},
		{"unknown → terminal (do not hammer the fleet)", 0x00000099, terminal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classifyReroute(c.status); got != c.want {
				t.Errorf("classifyReroute(0x%02x) = %d, want %d", c.status, got, c.want)
			}
		})
	}
}
