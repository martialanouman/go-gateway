package connectorpool

import (
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// TestEveryPoolRowCarriesTheCategory: cdr is ReplacingMergeTree(version), so a row written without the
// category would erase it from the segment it supersedes (step-292).
func TestEveryPoolRowCarriesTheCategory(t *testing.T) {
	r := pipeline.RoutedMT{
		MessageID: uuid.New(), ConnectorID: uuid.New(), SegmentSeq: 1, SegmentCount: 1, SubmittedAt: time.Now(),
		TrafficCategory: cp.TrafficTransactional, Priority: 2,
	}

	out := submitOutcome(r, smpp.PDU{})
	if out.TrafficCategory != "transactional" || out.Priority != 2 {
		t.Errorf("outcome category/priority = %q/%d, want transactional/2", out.TrafficCategory, out.Priority)
	}
	for name, row := range map[string]clickhouse.CDRRow{
		"rerouted":  reroutedRow(r, errs.ErrServiceUnavailable),
		"failed":    failedRow(r, errs.ErrServiceUnavailable),
		"cancelled": cancelledRow(r),
	} {
		if row.TrafficCategory != "transactional" || row.Priority != 2 {
			t.Errorf("%s row category/priority = %q/%d, want transactional/2", name, row.TrafficCategory, row.Priority)
		}
	}
}
