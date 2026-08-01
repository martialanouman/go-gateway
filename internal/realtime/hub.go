// Package realtime fans out the live feeds the Admin dashboard subscribes to (§15). It does no I/O of its
// own: frames arrive from a Kafka consumer and leave through WebSocket connections.
package realtime

import "sync"

// Stream names a feed. A closed set, so a typo cannot create a feed nobody publishes to.
type Stream string

// The feeds served over WebSocket.
const (
	StreamMetrics       Stream = "metrics"
	StreamSessions      Stream = "sessions"
	StreamBillingAlerts Stream = "billing-alerts"
)

// DefaultBufferSize is how many frames a subscriber may fall behind before it is cut. Every producing
// replica emits once a second, so this is well under a second of slack; deepening it would only serve stale
// figures a client believes are live. Tune per deployment via Config.
const DefaultBufferSize = 64

// Config tunes a Hub. The zero value is the default.
type Config struct {
	// BufferSize overrides DefaultBufferSize.
	BufferSize int
}

// Hub broadcasts frames to the subscribers of each stream. Publish never blocks: the publisher is the Kafka
// consumer goroutine, so one stalled client must not be able to freeze the feed for everyone.
type Hub struct {
	bufferSize int

	mu      sync.RWMutex
	streams map[Stream]map[*Subscription]struct{}
}

// NewHub builds a Hub. It starts no goroutine.
func NewHub(cfg Config) *Hub {
	size := cfg.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &Hub{bufferSize: size, streams: make(map[Stream]map[*Subscription]struct{})}
}

// Subscription is one client's view of a stream. Close is idempotent — a handler closes on every exit path.
type Subscription struct {
	hub    *Hub
	stream Stream
	frames chan []byte
	once   sync.Once
}

// Frames is closed when the subscription ends, whether the client left or fell behind.
func (s *Subscription) Frames() <-chan []byte { return s.frames }

// Close ends the subscription and releases its slot on the hub. Safe to call more than once.
//
// Unregistering BEFORE closing the channel is what makes this panic-free: unregister takes the hub's write
// lock, which cannot be acquired while any Publish holds the read lock, so no send can be in flight by the
// time the channel closes. Closing first would be a send-on-closed-channel panic under load.
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.hub.unregister(s)
		close(s.frames)
	})
}

// Subscribe registers a client on a stream.
func (h *Hub) Subscribe(stream Stream) *Subscription {
	sub := &Subscription{hub: h, stream: stream, frames: make(chan []byte, h.bufferSize)}

	h.mu.Lock()
	defer h.mu.Unlock()
	subs, ok := h.streams[stream]
	if !ok {
		subs = make(map[*Subscription]struct{})
		h.streams[stream] = subs
	}
	subs[sub] = struct{}{}
	return sub
}

// Publish broadcasts a frame without blocking. A subscriber whose buffer is full is disconnected rather than
// skipped: silently dropping would leave a client rendering stale figures it believes are live.
func (h *Hub) Publish(stream Stream, frame []byte) {
	h.mu.RLock()
	var lagging []*Subscription
	for sub := range h.streams[stream] {
		select {
		case sub.frames <- frame:
		default:
			lagging = append(lagging, sub)
		}
	}
	h.mu.RUnlock()

	// Closed outside the read lock: Close takes the write lock to unregister, which would deadlock here.
	for _, sub := range lagging {
		sub.Close()
	}
}

// Subscribers reports how many clients are connected to a stream.
func (h *Hub) Subscribers(stream Stream) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.streams[stream])
}

func (h *Hub) unregister(sub *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.streams[sub.stream]; ok {
		delete(subs, sub)
		if len(subs) == 0 {
			delete(h.streams, sub.stream)
		}
	}
}
