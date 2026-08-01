package metricstream

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// EventPublisher puts discrete records on metrics.stream: bind changes and billing alerts, as opposed to the
// periodic aggregates an Emitter produces.
//
// Best-effort like everything on this topic: Publish never blocks, never returns an error, and a lost alert
// costs a counter. Nothing here may delay or fail a message.
type EventPublisher struct {
	service  string
	instance string
	sink     Sink
	now      func() time.Time
	dropped  atomic.Int64

	mu          sync.Mutex
	windowStart time.Time
	inWindow    int
}

// NewEventPublisher builds a publisher for a service. A nil sink disables it.
func NewEventPublisher(service string, sink Sink) *EventPublisher {
	return &EventPublisher{
		service: service, instance: defaultInstance(), sink: sink, now: time.Now,
		windowStart: time.Now(),
	}
}

// Dropped counts records this publisher refused: over the session-event rate cap, or unserializable.
func (p *EventPublisher) Dropped() int64 { return p.dropped.Load() }

// SessionChanged publishes a bind state change.
func (p *EventPublisher) SessionChanged(accountID, systemID, state string, sessions *int) {
	if p == nil || p.sink == nil || !p.allow() {
		return
	}
	p.publish(SessionEvent{
		V:         SchemaVersion,
		Feed:      FeedSessions,
		Service:   p.service,
		Instance:  p.instance,
		EmittedAt: p.now().UTC(),
		AccountID: accountID,
		SystemID:  systemID,
		State:     state,
		Sessions:  sessions,
	})
}

// Alerted publishes a billing alert.
func (p *EventPublisher) Alerted(customerID, ownerType, ownerID, alert string, balance int64) {
	if p == nil || p.sink == nil {
		return
	}
	p.publish(BillingAlert{
		V:          SchemaVersion,
		Feed:       FeedBillingAlerts,
		Service:    p.service,
		Instance:   p.instance,
		EmittedAt:  p.now().UTC(),
		CustomerID: customerID,
		OwnerType:  ownerType,
		OwnerID:    ownerID,
		Alert:      alert,
		Balance:    balance,
	})
}

func (p *EventPublisher) publish(record any) {
	value, err := json.Marshal(record)
	if err != nil {
		p.dropped.Add(1)
		return
	}
	p.sink.TryPublish([]byte(p.instance), value)
}

// maxSessionEventsPerSecond bounds the session feed.
//
// A bind is one record, and nothing throttles a legitimate ESME's reconnect loop (the anti-brute-force
// counter only counts failures) — a flapping fleet or a pod drain would otherwise flood the shared topic,
// push the metrics snapshots past their 5 s freshness bar, and cut the very subscribers watching the
// incident. Excess is counted, not queued: a dashboard needs the rate, not every event.
const maxSessionEventsPerSecond = 50

// allow is a token bucket over session events.
func (p *EventPublisher) allow() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if elapsed := now.Sub(p.windowStart); elapsed >= time.Second {
		p.windowStart = now
		p.inWindow = 0
	}
	if p.inWindow >= maxSessionEventsPerSecond {
		p.dropped.Add(1)
		return false
	}
	p.inWindow++
	return true
}
