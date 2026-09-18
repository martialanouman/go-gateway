package config_test

import (
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/config"
)

// TestTLSIsMandatoryInProduction: the section is off by default, so unit tests and the integration
// suites keep working unchanged — which also means a production deployment inherits plaintext unless
// something refuses it. This is that something, on the model of the other dev-default guards.
func TestTLSIsMandatoryInProduction(t *testing.T) {
	for _, tt := range []struct {
		name string
		env  map[string]string
		want string // a fragment the refusal must name, or "" when the config must be accepted
	}{
		{
			name: "production without TLS",
			env:  map[string]string{"TLS_ENABLED": "false"},
			want: "TLS_ENABLED",
		},
		{
			name: "production with TLS but no certificate",
			env:  map[string]string{"TLS_ENABLED": "true", "TLS_KEY_FILE": "/k", "TLS_CLIENT_CA_FILE": "/ca"},
			want: "TLS_CERT_FILE",
		},
		{
			name: "production with TLS but no key",
			env:  map[string]string{"TLS_ENABLED": "true", "TLS_CERT_FILE": "/c", "TLS_CLIENT_CA_FILE": "/ca"},
			want: "TLS_KEY_FILE",
		},
		{
			name: "production with TLS but no authority",
			env:  map[string]string{"TLS_ENABLED": "true", "TLS_CERT_FILE": "/c", "TLS_KEY_FILE": "/k"},
			want: "TLS_CLIENT_CA_FILE",
		},
		{
			name: "production fully configured",
			env: map[string]string{
				"TLS_ENABLED": "true", "TLS_CERT_FILE": "/c", "TLS_KEY_FILE": "/k",
				"TLS_CLIENT_CA_FILE": "/ca",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			t.Setenv("ENVIRONMENT", "production")

			_, err := config.Load("svc", config.SectionTLS)
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("Load() = %v, want the configuration accepted", err)
			case tt.want != "" && err == nil:
				t.Fatalf("Load() = nil, want a refusal naming %s", tt.want)
			case tt.want != "" && !strings.Contains(err.Error(), tt.want):
				t.Errorf("Load() = %v, want it to name %s", err, tt.want)
			}
		})
	}
}

// TestTLSStaysOptionalOutsideProduction: a developer's laptop and the integration suites run without
// certificates. A guard that fired everywhere would have been reverted the same day.
func TestTLSStaysOptionalOutsideProduction(t *testing.T) {
	for _, env := range []string{"development", "staging"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", env)
			if _, err := config.Load("svc", config.SectionTLS); err != nil {
				t.Errorf("Load() = %v, want TLS to stay optional in %s", err, env)
			}
		})
	}
}
