package config_test

import (
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/config"
)

func TestOIDCSection(t *testing.T) {
	complete := map[string]string{
		"OIDC_ISSUER":   "https://idp.example/realms/gw",
		"OIDC_AUDIENCE": "gateway-admin",
		"OIDC_JWKS_URL": "https://idp.example/realms/gw/certs",
	}
	with := func(overrides map[string]string) map[string]string {
		env := map[string]string{}
		for k, v := range complete {
			env[k] = v
		}
		for k, v := range overrides {
			if v == "" {
				delete(env, k)
				continue
			}
			env[k] = v
		}
		return env
	}

	for _, tt := range []struct {
		name        string
		environment string
		env         map[string]string
		want        string // a fragment the refusal must name, or "" when the config must be accepted
	}{
		{name: "development without OIDC keeps the static verifier", environment: "development"},
		{name: "development fully configured", environment: "development", env: complete},
		{
			name: "a partial configuration is refused outside production too", environment: "development",
			env: with(map[string]string{"OIDC_AUDIENCE": ""}), want: "OIDC_AUDIENCE",
		},
		{name: "production without OIDC", environment: "production", want: "OIDC_ISSUER"},
		{name: "production fully configured", environment: "production", env: complete},
		{
			name: "production refuses a plaintext key set", environment: "production",
			env: with(map[string]string{"OIDC_JWKS_URL": "http://idp.example/certs"}), want: "OIDC_JWKS_URL",
		},
		{
			name: "development accepts a plaintext key set", environment: "development",
			env: with(map[string]string{"OIDC_JWKS_URL": "http://localhost:8080/certs"}),
		},
		{
			name: "a partial configuration without a key set is refused", environment: "production",
			env: with(map[string]string{"OIDC_JWKS_URL": ""}), want: "OIDC_JWKS_URL",
		},
		{
			name: "a key set URL without a scheme is refused", environment: "development",
			env: with(map[string]string{"OIDC_JWKS_URL": "//idp.example/certs"}), want: "OIDC_JWKS_URL",
		},
		{
			name: "a key set URL without a host is refused", environment: "development",
			env: with(map[string]string{"OIDC_JWKS_URL": "https:///certs"}), want: "OIDC_JWKS_URL",
		},
		{
			// Compared byte for byte with the token's iss: the padded issuer would refuse every token.
			name: "a padded issuer is refused", environment: "production",
			env: with(map[string]string{"OIDC_ISSUER": "https://idp.example/realms/gw "}), want: "OIDC_ISSUER",
		},
		{
			// Blank reads as unset to a human and as set to the verifier choice.
			name: "a blank issuer is refused", environment: "development",
			env: map[string]string{"OIDC_ISSUER": "  "}, want: "OIDC_ISSUER",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range tt.env {
				env[k] = v
			}
			env["ENVIRONMENT"] = tt.environment
			setEnv(t, env)

			_, err := config.Load("svc", config.SectionOIDC)
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
