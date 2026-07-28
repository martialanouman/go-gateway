package smscsim_test

import (
	"context"
	"testing"

	"github.com/martialanouman/go-gateway/internal/testutil/smscsim"
)

// TestConfigBuildersLaunch validates every scenario-profile config builder against the real simulator:
// each must produce a config the simulator ACCEPTS (the container starts and answers /health) and reports
// the expected active profile. This turns the builders — otherwise first exercised only in 130b/130c —
// into live, schema-checked code, so a bad profile name, a missing required param or an out-of-bounds
// latency fails here rather than deep inside a later fault scenario.
func TestConfigBuildersLaunch(t *testing.T) {
	cases := []struct {
		name    string
		config  string
		profile string
	}{
		{"healthy", smscsim.HealthyConfig("c1", "pw"), "healthy"},
		{"throttling", smscsim.ThrottlingConfig("c1", "pw", 100), "throttling-carrier"},
		{"dead", smscsim.DeadCarrierConfig("c1", "pw"), "dead-carrier"},
		{"flaky", smscsim.FlakyCarrierConfig("c1", "pw"), "flaky-carrier"},
		{"slow", smscsim.SlowCarrierConfig("c1", "pw", 2000), "slow-carrier"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sim := smscsim.Launch(t, tc.config)
			if err := sim.Health(context.Background()); err != nil {
				t.Fatalf("health: %v", err)
			}
			snap, err := sim.Snapshot(context.Background(), "carrier")
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			if snap.ActiveProfile != tc.profile {
				t.Errorf("active_profile = %q, want %q", snap.ActiveProfile, tc.profile)
			}
		})
	}
}
