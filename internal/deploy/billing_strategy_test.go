package deploy_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/deploy"
)

// TestBillingSvcNeverRunsTwoVersions pins ADR-0022 §6: a billing-svc from before the balance deltas reads the
// folded balance alone and would rehydrate the cache without the unfolded reserves. A rolling update mixes
// the two versions; Recreate does not.
func TestBillingSvcNeverRunsTwoVersions(t *testing.T) {
	t.Parallel()

	manifests, err := deploy.Load(deploy.Dir)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	for _, m := range manifests {
		if m.Kind == "Deployment" && m.Metadata.Name == "billing-svc" {
			if m.Spec.Strategy.Type != "Recreate" {
				t.Errorf("billing-svc strategy = %q, want Recreate", m.Spec.Strategy.Type)
			}
			return
		}
	}
	t.Fatal("no billing-svc Deployment in the deployed tree")
}
