// Command content-key-svc serves the ContentKeys gRPC API: the custody of the per-customer content
// encryption keys (plan §6.14, M10, gRPC :7002). It is the SOLE holder of the KMS master key (the KEK that
// seals every per-customer data key), which is the whole reason it exists as its own binary — see ADR-0011.
//
// Its surface is deliberately minimal: Postgres (the content_keys table) and gRPC. No Redis, no Kafka, no
// HTTP, no outbound client. Every dependency it does not have is one that cannot be used to reach the KEK.
//
// It follows the canonical service lifecycle of cmd/session-manager-svc.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/content"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/platform/supervisor"
)

// serviceName identifies this binary in logs, traces and metrics.
const serviceName = "content-key-svc"

func main() {
	if err := run(); err != nil {
		log.Fatalf("%s: %v", serviceName, err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	// Postgres (content_keys) and gRPC, nothing else: the key custodian declares the smallest possible set
	// of sections. The plan assigns it port 7002; the deploy sets GRPC_PORT=7002 (the shared config default
	// 7000 is session-manager-svc's, §1.4), which is what CONTENT_KEY_ADDR's default points at.
	cfg, err := config.Load(serviceName, config.SectionOTel, config.SectionPostgres, config.SectionGRPC)
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(os.Stdout, cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	shutdownTracing, err := observability.InitTracing(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	//nolint:contextcheck // Detaching is the point: see DrainTracing's comment.
	defer observability.DrainTracing(shutdownTracing, cfg.ShutdownTimeout, logger)

	app, err := newContentKeyApp(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer app.close()

	logger.InfoContext(ctx, "starting", "config", cfg)

	var g supervisor.Group
	g.Add("ops server", func(c context.Context) error { return app.ops.Run(c, cfg.ShutdownTimeout) })
	g.Add("grpc server", func(c context.Context) error {
		return runGRPC(c, app.grpc, cfg.GRPC.Port, cfg.ShutdownTimeout, logger)
	})
	if err := g.Run(ctx, logger); err != nil {
		return err
	}

	logger.Info("stopped")
	return nil
}

// contentKMSMasterKeyEnv holds a base64-std-encoded 32-byte AES-256 master key (KEK) for the local content
// KMS. It is a dev/staging convenience until the real KMS provider (AWS/GCP/Vault) is wired (§14).
const contentKMSMasterKeyEnv = "CONTENT_KMS_MASTER_KEY"

// loadContentKMS builds the content KMS. With CONTENT_KMS_MASTER_KEY set (base64 of 32 bytes) it uses a
// stable local master key, so wrapped content keys survive restarts. The real provider replaces this behind
// the content.KMS interface with no call-site change.
//
// In PRODUCTION an absent master key is a boot failure, never a fallback. The KEK is read from the
// environment rather than from Config, so the "no loopback default in production" discipline cannot catch
// it — and a brand-new deployment unit is exactly where an env var gets forgotten. Starting anyway would be
// silently destructive twice over: existing keys would not unwrap (the router drops bodies from the CDR and
// only a counter moves), and every key sealed under the ephemeral KEK dies with the process.
//
// Off the production tier it still falls back to an ephemeral dev key and warns, which is what a laptop and
// the tests want.
func loadContentKMS(env config.Environment, logger *slog.Logger) (content.KMS, error) {
	raw := os.Getenv(contentKMSMasterKeyEnv)
	if raw == "" {
		if env.IsProduction() {
			return nil, fmt.Errorf("%s must be set in production: the key custodian would otherwise seal keys "+
				"under an ephemeral master key that dies with the process", contentKMSMasterKeyEnv)
		}
		logger.Warn("no " + contentKMSMasterKeyEnv + " set: using an ephemeral dev content KMS master key (content keys will not survive a restart)")
		return content.NewDevKMS(), nil
	}
	master, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", contentKMSMasterKeyEnv, err)
	}
	kms, err := content.NewLocalKMS(master, "local/v1")
	if err != nil {
		return nil, fmt.Errorf("build content KMS from %s: %w", contentKMSMasterKeyEnv, err)
	}
	return kms, nil
}

// runGRPC serves the ContentKeys API until ctx is cancelled, then drains within timeout. It mirrors
// OpsServer.Run: same lifecycle, different port. GracefulStop lets in-flight RPCs finish; if they overrun
// the window a hard Stop cuts them, so the goroutine always has a stop condition.
func runGRPC(ctx context.Context, srv *grpc.Server, port int, timeout time.Duration, logger *slog.Logger) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("content key service listening", "addr", lis.Addr().String())
		serveErr <- srv.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()

		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-stopped:
			return nil
		case <-timer.C:
			srv.Stop()
			return nil
		}
	}
}
