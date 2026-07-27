package clickhouse_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// TestReroutedRankBelowEnroute locks step-125's fix: a destination connector's enroute must supersede a
// prior rerouted row, so a successfully-rerouted message does not read "rerouted" forever.
func TestReroutedRankBelowEnroute(t *testing.T) {
	if clickhouse.StatusRerouted.Rank() >= clickhouse.StatusEnroute.Rank() {
		t.Errorf("rerouted rank %d >= enroute rank %d — enroute must win",
			clickhouse.StatusRerouted.Rank(), clickhouse.StatusEnroute.Rank())
	}
	if clickhouse.StatusRerouted.Rank() <= clickhouse.StatusAccepted.Rank() {
		t.Errorf("rerouted rank %d <= accepted rank %d — an in-flight reroute must show over accepted",
			clickhouse.StatusRerouted.Rank(), clickhouse.StatusAccepted.Rank())
	}
}
