package main

import (
	"log/slog"
	"testing"

	"github.com/martialanouman/go-gateway/internal/config"
)

// TestPodAddrComposesWhatTheReturnPathDials covers the one line in this repo that brackets an IPv6
// address. The IPv6 argument is what ruled out making pod_id the pod's IP (step-302's design), yet the
// registry tests all write "[fd00::2]:7000" bracketed BY HAND — they prove the registry carries a
// well-formed string, never that this code knows how to form one.
//
// It also pins the guard: SMPP_POD_ADDR is a bare IP. A value carrying a port composes a target that
// grpc.NewClient accepts and every RPC then fails on, which is this pod's binds on webhook-only.
func TestPodAddrComposesWhatTheReturnPathDials(t *testing.T) {
	tests := []struct {
		name    string
		podAddr string
		want    string
		wantErr bool
	}{
		{name: "IPv4", podAddr: "10.4.5.6", want: "10.4.5.6:7000"},
		{name: "IPv6 gets its brackets", podAddr: "fd00::2", want: "[fd00::2]:7000"},
		{name: "unset degrades to webhooks", podAddr: "", want: ""},
		{name: "already carries a port", podAddr: "10.4.5.6:9000", wantErr: true},
		{name: "a name, not an IP", podAddr: "smpp-server-svc", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{SMPP: config.SMPP{PodAddr: tc.podAddr}, GRPC: config.GRPC{Port: 7000}}
			got, err := podAddr(cfg, slog.New(slog.DiscardHandler))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("podAddr(%q) = %q, want an error", tc.podAddr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("podAddr(%q): %v", tc.podAddr, err)
			}
			if got != tc.want {
				t.Errorf("podAddr(%q) = %q, want %q", tc.podAddr, got, tc.want)
			}
		})
	}
}
