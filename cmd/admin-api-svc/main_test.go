package main

import (
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/realtime"
)

// TestRunRequiresOIDCInProduction: production has no static tokens, so a production Admin API without an
// identity provider must fail the boot rather than come up and answer every request with 401. The guard
// runs in config.Load, so no database is needed.
func TestRunRequiresOIDCInProduction(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	// Non-localhost dependency addresses so the production config guards pass; the pool and the
	// session-manager client are never opened because the config check fires first.
	t.Setenv("POSTGRES_URL", "postgres://u:p@db.internal:5432/gw")
	t.Setenv("SMPP_SESSION_MANAGER_ADDR", "session-manager.internal:7000")
	t.Setenv("BILLING_ADDR", "billing.internal:7001")
	t.Setenv("CLICKHOUSE_ADDR", "clickhouse.internal:9000")
	t.Setenv("CLICKHOUSE_PASSWORD", "s3cret")
	t.Setenv("CONTENT_KEY_ADDR", "content-key.internal:7002")
	t.Setenv("REDIS_URL", "redis://redis.internal:6379")
	t.Setenv("KAFKA_BROKERS", "kafka.internal:9092")
	t.Setenv("TLS_ENABLED", "true")
	t.Setenv("TLS_CERT_FILE", "/etc/gateway/tls/tls.crt")
	t.Setenv("TLS_KEY_FILE", "/etc/gateway/tls/tls.key")
	t.Setenv("TLS_CLIENT_CA_FILE", "/etc/gateway/tls/ca.crt")
	// OIDC_* deliberately unset.

	err := run()
	if err == nil {
		t.Fatal("run() = nil, want a boot failure for a production Admin API without an identity provider")
	}
	if !strings.Contains(err.Error(), "OIDC_ISSUER") {
		t.Errorf("error %q should name OIDC_ISSUER", err)
	}

	// With a provider, the static tokens beside it are what refuses the boot — before Postgres is opened.
	t.Setenv("OIDC_ISSUER", "https://idp.internal/realms/gw")
	t.Setenv("OIDC_AUDIENCE", "gateway-admin")
	t.Setenv("OIDC_JWKS_URL", "https://idp.internal/realms/gw/certs")
	t.Setenv("HTTP_ADMIN_TOKENS", "static-operator-token:admin:read")
	if err := run(); err == nil || !strings.Contains(err.Error(), "HTTP_ADMIN_TOKENS") {
		t.Errorf("run() = %v, want the boot refused over HTTP_ADMIN_TOKENS", err)
	}
}

// TestValidateAdminConfigRefusesStaticTokensBesideOIDC: once a provider is configured the static tokens
// are never read, so setting them is a mistake to report, not a fallback. The error names the variable,
// never a token — a boot error lands in logs.
func TestValidateAdminConfigRefusesStaticTokensBesideOIDC(t *testing.T) {
	t.Parallel()

	const token = "static-operator-token"
	oidc := config.OIDC{Issuer: "https://idp", Audience: "gw", JWKSURL: "https://idp/certs"}

	for _, env := range []config.Environment{config.EnvProduction, config.EnvDevelopment} {
		cfg := config.Config{Environment: env, OIDC: oidc, HTTP: config.HTTP{AdminTokens: []string{token + ":admin:read"}}}
		err := validateAdminConfig(cfg)
		if err == nil || !strings.Contains(err.Error(), "HTTP_ADMIN_TOKENS") {
			t.Errorf("%s: validateAdminConfig() = %v, want a refusal naming HTTP_ADMIN_TOKENS", env, err)
		}
		if err != nil && strings.Contains(err.Error(), token) {
			t.Errorf("%s: error %q leaks the token", env, err)
		}
	}

	// A trailing separator in the variable is not a configured token.
	cfg := config.Config{Environment: config.EnvProduction, OIDC: oidc, HTTP: config.HTTP{AdminTokens: []string{" "}}}
	if err := validateAdminConfig(cfg); err != nil {
		t.Errorf("validateAdminConfig() = %v, want nil for blank entries only", err)
	}

	cfg = config.Config{Environment: config.EnvDevelopment, HTTP: config.HTTP{AdminTokens: []string{"t:admin:read"}}}
	if err := validateAdminConfig(cfg); err != nil {
		t.Errorf("validateAdminConfig() = %v, want the static verifier accepted without a provider", err)
	}
}

// TestRunExitsWhenPostgresIsUnreachable pins the boot contract: Postgres is vital to the Admin API
// (plan §1.5), so NewPool pings it eagerly and run must fail fast rather than start a server that
// answers every request with an error. No signal is sent — run returns on the boot failure alone.
func TestRunExitsWhenPostgresIsUnreachable(t *testing.T) {
	// A refused connection resolves immediately, so the ping fails without waiting out its timeout.
	t.Setenv("POSTGRES_URL", "postgres://gateway:gateway@127.0.0.1:1/gateway?sslmode=disable")
	t.Setenv("POSTGRES_TIMEOUT", "1s")
	// No collector is listening; a real exporter would stall the drain on a flush that never lands.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("SHUTDOWN_TIMEOUT", "2s")

	done := make(chan error, 1)
	go func() { done <- run() }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run() = nil, want the unreachable-Postgres boot failure")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run() never returned: an unreachable vital dependency did not fail the boot")
	}
}

// TestPublishRecordRoutesByFeed covers the one place the three feeds are told apart. A misrouted frame is
// invisible to a dashboard — it renders whatever it is handed — so only a test catches it.
func TestPublishRecordRoutesByFeed(t *testing.T) {
	tests := []struct {
		name   string
		record string
		want   realtime.Stream // "" means: published nowhere
	}{
		{"metrics snapshot", `{"v":1,"feed":"metrics","samples":[]}`, realtime.StreamMetrics},
		{"session event", `{"v":1,"feed":"sessions","state":"bound"}`, realtime.StreamSessions},
		{"billing alert", `{"v":1,"feed":"billing-alerts","alert":"mo_floor_reached"}`, realtime.StreamBillingAlerts},
		// A snapshot produced before step-184 added the discriminator.
		{"no feed", `{"v":1,"samples":[]}`, realtime.StreamMetrics},
		{"unknown feed", `{"v":1,"feed":"invented"}`, ""},
		{"future version", `{"v":2,"feed":"metrics"}`, ""},
		{"not json", `{`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hub := realtime.NewHub(realtime.Config{})
			subs := map[realtime.Stream]*realtime.Subscription{
				realtime.StreamMetrics:       hub.Subscribe(realtime.StreamMetrics),
				realtime.StreamSessions:      hub.Subscribe(realtime.StreamSessions),
				realtime.StreamBillingAlerts: hub.Subscribe(realtime.StreamBillingAlerts),
			}
			for _, sub := range subs {
				defer sub.Close()
			}

			publishRecord(hub, []byte(tc.record))

			for stream, sub := range subs {
				select {
				case frame := <-sub.Frames():
					if stream != tc.want {
						t.Errorf("frame landed on %s: %q", stream, frame)
					}
				default:
					if stream == tc.want {
						t.Errorf("nothing landed on %s", stream)
					}
				}
			}
		})
	}
}

// TestEventPublisherOutputRoutesCorrectly joins the two halves: what a producer actually serializes must be
// what the consumer routes. Hand-written JSON in the table above cannot catch a field-name drift.
func TestEventPublisherOutputRoutesCorrectly(t *testing.T) {
	captured := make(chan []byte, 4)
	p := metricstream.NewEventPublisher("smpp-server-svc", sinkFunc(func(_, value []byte) {
		captured <- value
	}))
	active := 2
	p.SessionChanged("acct-1", "ACME01", "bound", &active)
	p.Alerted("cust-1", "customer", "cust-1", "mo_floor_reached", 0)

	hub := realtime.NewHub(realtime.Config{})
	sessions := hub.Subscribe(realtime.StreamSessions)
	defer sessions.Close()
	alerts := hub.Subscribe(realtime.StreamBillingAlerts)
	defer alerts.Close()

	for range 2 {
		publishRecord(hub, <-captured)
	}
	if len(sessions.Frames()) != 1 {
		t.Errorf("sessions received %d frames, want 1", len(sessions.Frames()))
	}
	if len(alerts.Frames()) != 1 {
		t.Errorf("billing-alerts received %d frames, want 1", len(alerts.Frames()))
	}
}

type sinkFunc func(key, value []byte)

func (f sinkFunc) TryPublish(key, value []byte) { f(key, value) }
