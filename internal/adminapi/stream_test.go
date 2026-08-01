package adminapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/realtime"
)

// TestStreamMetricsPushesToAConnectedClient is step-183's acceptance criterion: a frame published on the hub
// reaches a live WebSocket client, well inside the 5 s freshness bar.
func TestStreamMetricsPushesToAConnectedClient(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{})
	srv := httptest.NewServer(newTestAPIWith(t, adminapi.Deps{StreamHub: hub}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL)+"/v1/admin/stream/metrics", &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + operatorToken}},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()

	// Published only once a subscriber exists; the handler subscribes after the upgrade completes.
	waitForSubscriber(t, hub)
	hub.Publish(realtime.StreamMetrics, []byte(`{"v":1,"service":"router-svc"}`))

	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	kind, frame, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if kind != websocket.MessageText {
		t.Errorf("message type = %v, want text", kind)
	}
	if got := string(frame); got != `{"v":1,"service":"router-svc"}` {
		t.Errorf("frame = %q", got)
	}
}

// TestStreamMetricsRequiresTheOperatorScope: the feed carries live operational data and must not be readable
// without authorization.
func TestStreamMetricsRequiresTheOperatorScope(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{})
	srv := httptest.NewServer(newTestAPIWith(t, adminapi.Deps{StreamHub: hub}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL(srv.URL)+"/v1/admin/stream/metrics", nil)
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("an unauthenticated client completed the upgrade")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %v, want 401", resp)
	}
}

func waitForSubscriber(t *testing.T, hub *realtime.Hub) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if hub.Subscribers(realtime.StreamMetrics) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the handler never subscribed to the hub")
}

func wsURL(httpURL string) string { return "ws" + strings.TrimPrefix(httpURL, "http") }
