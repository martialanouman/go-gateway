package adminapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/realtime"
)

const (
	streamWriteTimeout = 5 * time.Second
	// Idle proxies cut a silent socket after 30-60 s.
	streamPingInterval = 20 * time.Second
)

// StreamHub is the fan-out the stream endpoints subscribe to.
type StreamHub interface {
	Subscribe(stream realtime.Stream) *realtime.Subscription
}

// Quit tells live stream handlers to close.
//
// net/http drops a hijacked connection from its active set, so Server.Shutdown neither waits for a WebSocket
// nor closes it: without this signal a handler keeps running against a pool the caller is closing, and the
// client sees a TCP reset instead of a Going Away. Wire it to Server.RegisterOnShutdown.
type Quit <-chan struct{}

// registerStreams wires the realtime endpoints.
//
// They are Huma operations even though they hijack the connection: that is what puts them in the generated
// spec and runs the scope middleware. A bare router route would be a live feed of operational data with no
// contract and no authorization.
func registerStreams(api huma.API, hub StreamHub, quit Quit, logger *slog.Logger) {
	h := &streamHandler{hub: hub, quit: quit, logger: logger}

	registerUpgrade(api, huma.Operation{
		OperationID: "stream-metrics",
		Method:      http.MethodGet,
		Path:        "/admin/stream/metrics",
		Summary:     "WebSocket — live metrics stream",
		Description: "Upgrade to a WebSocket. Emits one metricstream Snapshot per frame (`{v, feed, service, instance, emitted_at, samples[]}`); branch on `v`. `101 Switching Protocols` on upgrade.",
	}, h.serve(realtime.StreamMetrics))

	registerUpgrade(api, huma.Operation{
		OperationID: "stream-sessions",
		Method:      http.MethodGet,
		Path:        "/admin/stream/sessions",
		Summary:     "WebSocket — live session events",
		Description: "Upgrade to a WebSocket. Emits one metricstream SessionEvent per frame (`{v, feed, account_id, system_id, state, sessions}`). `101 Switching Protocols` on upgrade.",
	}, h.serve(realtime.StreamSessions))

	registerUpgrade(api, huma.Operation{
		OperationID: "stream-billing-alerts",
		Method:      http.MethodGet,
		Path:        "/admin/stream/billing-alerts",
		Summary:     "WebSocket — MO floor-reached alerts",
		Description: "Upgrade to a WebSocket. Emits one metricstream BillingAlert per frame (`{v, feed, customer_id, owner_type, owner_id, alert, balance}`). Only `mo_floor_reached` is emitted today; low-balance and breaker-open alerts have no configured threshold yet. `101 Switching Protocols` on upgrade.",
	}, h.serve(realtime.StreamBillingAlerts))
}

// registerUpgrade registers a WebSocket operation.
//
// Huma derives responses from an output schema, and a protocol upgrade has none, so the generated entry
// carries no response codes. The committed contract declares 101 and 401; upgradeOperations below records
// that gap so the contract test checks these operations by their own rule rather than silently.
func registerUpgrade(api huma.API, op huma.Operation, handler func(context.Context, *streamInput) (*huma.StreamResponse, error)) {
	op.Tags = []string{"Realtime"}
	op.DefaultStatus = http.StatusSwitchingProtocols
	op.Security = scopeSecurity(auth.ScopeAdminRead)
	huma.Register(api, op, handler)
}

type streamHandler struct {
	hub    StreamHub
	quit   Quit
	logger *slog.Logger
}

type streamInput struct{}

// serve answers with a StreamResponse: Huma calls the body with the request's huma.Context and writes no
// status of its own, which is what an upgrade needs.
func (h *streamHandler) serve(stream realtime.Stream) func(context.Context, *streamInput) (*huma.StreamResponse, error) {
	return func(_ context.Context, _ *streamInput) (*huma.StreamResponse, error) {
		if h.hub == nil {
			return nil, huma.Error503ServiceUnavailable("realtime streams are not enabled on this instance")
		}
		//nolint:contextcheck // Huma hands the request context through hctx; there is no other one here.
		return &huma.StreamResponse{Body: func(hctx huma.Context) {
			ctx := hctx.Context()
			r, w := humachi.Unwrap(hctx)

			// The API sits behind mTLS and an operator bearer a browser cannot replay cross-origin; origin
			// checking would only reject the legitimate dashboard.
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				h.logger.DebugContext(ctx, "stream: upgrade refused", "stream", stream, "err", err)
				return
			}
			defer func() { _ = conn.CloseNow() }()

			// Write-only feed: CloseRead drains the client's close frame so a disconnect is noticed.
			h.pump(conn.CloseRead(ctx), conn, stream)
		}}, nil
	}
}

func (h *streamHandler) pump(ctx context.Context, conn *websocket.Conn, stream realtime.Stream) {
	sub := h.hub.Subscribe(stream)
	defer sub.Close()

	ticker := time.NewTicker(streamPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-h.quit:
			_ = conn.Close(websocket.StatusGoingAway, "server shutting down")
			return

		case frame, open := <-sub.Frames():
			if !open {
				// The hub cut this client for lagging; say so rather than vanishing.
				_ = conn.Close(websocket.StatusPolicyViolation, "client too slow")
				return
			}
			if err := h.write(ctx, conn, frame); err != nil {
				h.logger.DebugContext(ctx, "stream: write failed", "stream", stream, "err", err)
				return
			}

		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, streamWriteTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// write bounds one frame, so a client whose TCP window closed cannot pin a goroutine.
func (h *streamHandler) write(ctx context.Context, conn *websocket.Conn, frame []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, streamWriteTimeout)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, frame)
}
