package adminapi

import (
	"testing"

	"github.com/danielgtaylor/huma/v2"
)

// TestRevealReadsNameRegisteredOperations: the audit middleware recognises the number-revealing reads by
// operation id; a renamed operation would silently drop out of the trail, so every id must be served.
func TestRevealReadsNameRegisteredOperations(t *testing.T) {
	_, api := New(Deps{})
	served := map[string]bool{}
	for _, item := range api.OpenAPI().Paths {
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			if op != nil {
				served[op.OperationID] = true
			}
		}
	}
	for id := range revealReads {
		if !served[id] {
			t.Errorf("revealReads names %q, which the Admin API does not serve", id)
		}
	}
	if len(revealReads) == 0 {
		t.Error("revealReads is empty: nothing guards the reveal reads")
	}
}
