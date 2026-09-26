package kafkaprovision_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/storage/kafkaprovision"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// TestTheProvisionerDialsOverTLSWhenEnabled: the provisioning Job reaches the same brokers as the services, so
// it follows the same KAFKA_TLS_ settings.
func TestTheProvisionerDialsOverTLSWhenEnabled(t *testing.T) {
	ca := tlstest.NewCA(t)
	addr, handshakes := ca.HandshakePeer(t)
	adm, err := kafkaprovision.NewAdmin(config.Kafka{Brokers: []string{addr}, Timeout: 2 * time.Second, TLSEnabled: true, TLSCAFile: ca.CAFile})
	if err != nil {
		t.Fatalf("NewAdmin: %v", err)
	}
	defer adm.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = adm.ListTopics(ctx)
	select {
	case <-handshakes:
	case <-time.After(3 * time.Second):
		t.Fatal("no TLS handshake reached the broker")
	}

	if _, err := kafkaprovision.NewAdmin(config.Kafka{Brokers: []string{addr}, TLSEnabled: true, TLSCAFile: filepath.Join(t.TempDir(), "absent.crt")}); err == nil {
		t.Error("NewAdmin accepted a CA file that does not exist")
	}
}
