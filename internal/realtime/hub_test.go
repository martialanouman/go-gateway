package realtime_test

import (
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/realtime"
)

func recv(t *testing.T, sub *realtime.Subscription) []byte {
	t.Helper()
	select {
	case frame, ok := <-sub.Frames():
		if !ok {
			t.Fatal("subscription closed while a frame was expected")
		}
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("no frame within the timeout")
		return nil
	}
}

// TestPublishReachesEverySubscriberOfAStream is the fan-out contract: N browsers watching the same feed all
// see the same frame, and a stream never leaks into another.
func TestPublishReachesEverySubscriberOfAStream(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{})

	a := hub.Subscribe(realtime.StreamMetrics)
	defer a.Close()
	b := hub.Subscribe(realtime.StreamMetrics)
	defer b.Close()
	other := hub.Subscribe(realtime.StreamSessions)
	defer other.Close()

	hub.Publish(realtime.StreamMetrics, []byte(`{"v":1}`))

	if got := string(recv(t, a)); got != `{"v":1}` {
		t.Errorf("subscriber a got %q", got)
	}
	if got := string(recv(t, b)); got != `{"v":1}` {
		t.Errorf("subscriber b got %q", got)
	}
	select {
	case frame := <-other.Frames():
		t.Errorf("a sessions subscriber received a metrics frame: %q", frame)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestPublishNeverBlocksOnASlowSubscriber is the property the whole design rests on. The publisher is the
// Kafka consumer goroutine: if one stalled browser could block it, one bad client would freeze the feed for
// everyone — and, through the consumer, stop committing offsets.
func TestPublishNeverBlocksOnASlowSubscriber(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{BufferSize: 1})
	slow := hub.Subscribe(realtime.StreamMetrics)
	defer slow.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			hub.Publish(realtime.StreamMetrics, []byte(`{"v":1}`))
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a subscriber that never read")
	}
}

// TestASlowSubscriberIsDisconnected: dropping frames silently would leave a browser showing figures it thinks
// are live but are minutes old — worse than a closed socket, which it can reconnect from. A client that
// cannot keep up with one frame per second is broken, so the hub cuts it.
func TestASlowSubscriberIsDisconnected(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{BufferSize: 1})
	slow := hub.Subscribe(realtime.StreamMetrics)
	defer slow.Close()

	for range 10 {
		hub.Publish(realtime.StreamMetrics, []byte(`{"v":1}`))
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-slow.Frames():
			if !ok {
				return // closed, as it must be
			}
		case <-deadline:
			t.Fatal("a subscriber that never kept up was not disconnected")
		}
	}
}

// TestCloseIsIdempotentAndUnregisters: a WS handler closes on every exit path, and a browser that vanished
// must not keep a slot on the hub.
func TestCloseIsIdempotentAndUnregisters(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{})
	sub := hub.Subscribe(realtime.StreamMetrics)

	sub.Close()
	sub.Close() // must not panic on a double close

	if got := hub.Subscribers(realtime.StreamMetrics); got != 0 {
		t.Errorf("Subscribers = %d after Close, want 0", got)
	}
}

// TestPublishWithNoSubscribersIsFree: the consumer runs whether or not a browser is connected, which is the
// common case.
func TestPublishWithNoSubscribersIsFree(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{})
	hub.Publish(realtime.StreamMetrics, []byte(`{"v":1}`))
}

// TestConcurrentSubscribeAndPublishIsSafe: browsers connect and drop while frames flow.
func TestConcurrentSubscribeAndPublishIsSafe(t *testing.T) {
	hub := realtime.NewHub(realtime.Config{BufferSize: 4})
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				hub.Publish(realtime.StreamMetrics, []byte(`{"v":1}`))
			}
		}
	}()
	for range 50 {
		sub := hub.Subscribe(realtime.StreamMetrics)
		<-time.After(time.Millisecond)
		sub.Close()
	}
	close(stop)

	if got := hub.Subscribers(realtime.StreamMetrics); got != 0 {
		t.Errorf("Subscribers = %d after every client left, want 0 — the hub leaks slots", got)
	}
}
