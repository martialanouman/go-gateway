package main

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"testing"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/content"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestLoadContentKMSFromMasterKey: with a valid master key the service builds a LocalKMS that really wraps
// and unwraps — the key custodian must fail at BOOT if its KEK is unusable, never at the first request.
func TestLoadContentKMSFromMasterKey(t *testing.T) {
	master, err := content.GenerateDataKey()
	if err != nil {
		t.Fatalf("master key: %v", err)
	}
	t.Setenv(contentKMSMasterKeyEnv, base64.StdEncoding.EncodeToString(master))

	kms, err := loadContentKMS(config.EnvDevelopment, discardLogger())
	if err != nil {
		t.Fatalf("loadContentKMS: %v", err)
	}
	dek, _ := content.GenerateDataKey()
	wrapped, err := kms.WrapDataKey(context.Background(), dek)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	got, err := kms.UnwrapDataKey(context.Background(), wrapped)
	if err != nil || string(got) != string(dek) {
		t.Errorf("round-trip through the configured KMS failed: %v", err)
	}
}

// TestLoadContentKMSRejectsBadMasterKey: a malformed or wrong-sized master key is a boot failure, not a
// silent fallback to an ephemeral key — that would quietly make every content key unreadable after a restart.
func TestLoadContentKMSRejectsBadMasterKey(t *testing.T) {
	// An EMPTY value is not tested here: it is indistinguishable from "unset", which legitimately means
	// "use the ephemeral dev key" (covered below).
	for name, value := range map[string]string{
		"not base64": "!!! not base64 !!!",
		"wrong size": base64.StdEncoding.EncodeToString([]byte("too short")),
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(contentKMSMasterKeyEnv, value)
			if _, err := loadContentKMS(config.EnvDevelopment, discardLogger()); err == nil {
				t.Error("loadContentKMS accepted an unusable master key, want a boot failure")
			}
		})
	}
}

// TestLoadContentKMSRequiresMasterKeyInProduction: in production an absent master key is a BOOT failure.
// Falling back would be silently destructive — existing keys would not unwrap (the router drops bodies and
// only a counter moves), and every key sealed under the ephemeral KEK would die with the process. The KEK
// comes from the environment, not from Config, so the shared "no loopback default" guard cannot catch it.
func TestLoadContentKMSRequiresMasterKeyInProduction(t *testing.T) {
	t.Setenv(contentKMSMasterKeyEnv, "")
	if _, err := loadContentKMS(config.EnvProduction, discardLogger()); err == nil {
		t.Fatal("production started without a master key, want a boot failure")
	}
}

// TestLoadContentKMSFallsBackToDevKey: with no master key configured the service still starts, on an
// ephemeral dev key (the laptop/test path). It warns, because keys wrapped under it die with the process.
func TestLoadContentKMSFallsBackToDevKey(t *testing.T) {
	t.Setenv(contentKMSMasterKeyEnv, "")
	kms, err := loadContentKMS(config.EnvDevelopment, discardLogger())
	if err != nil {
		t.Fatalf("loadContentKMS: %v", err)
	}
	if kms == nil || kms.KeyRef() == "" {
		t.Error("expected a usable ephemeral dev KMS")
	}
}
