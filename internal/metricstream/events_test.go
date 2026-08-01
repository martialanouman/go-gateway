package metricstream_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/metricstream"
)

func TestSessionEventCarriesItsFeed(t *testing.T) {
	sink := &fakeSink{}
	p := metricstream.NewEventPublisher("smpp-server-svc", sink)

	active := 3
	p.SessionChanged("acct-1", "ACME01", "bound", &active)

	var ev metricstream.SessionEvent
	decodeOne(t, sink, &ev)
	if ev.Feed != metricstream.FeedSessions {
		t.Errorf("feed = %q, want sessions", ev.Feed)
	}
	if ev.V != metricstream.SchemaVersion || ev.State != "bound" || ev.Sessions == nil || *ev.Sessions != 3 {
		t.Errorf("event = %+v", ev)
	}
	if ev.EmittedAt.IsZero() {
		t.Error("emitted_at is zero")
	}
}

func TestBillingAlertCarriesItsFeed(t *testing.T) {
	sink := &fakeSink{}
	p := metricstream.NewEventPublisher("billing-svc", sink)

	p.Alerted("cust-1", "customer", "acct-9", "mo_floor_reached", 0)

	var alert metricstream.BillingAlert
	decodeOne(t, sink, &alert)
	if alert.Feed != metricstream.FeedBillingAlerts || alert.Alert != "mo_floor_reached" ||
		alert.CustomerID != "cust-1" || alert.OwnerID != "acct-9" {
		t.Errorf("alert = %+v", alert)
	}
}

// TestEventsCarryNoSecret guards invariant (a) at the one place a bind event could leak one: the record is
// built from identifiers only, so a password handed to the publisher must not appear on the wire.
func TestEventsCarryNoSecret(t *testing.T) {
	sink := &fakeSink{}
	p := metricstream.NewEventPublisher("smpp-server-svc", sink)

	active := 1
	p.SessionChanged("acct-1", "ACME01", "bound", &active)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, v := range sink.values {
		if strings.Contains(string(v), "password") || strings.Contains(string(v), "secret") {
			t.Fatalf("a secret reached the stream: %s", v)
		}
	}
}

// TestNilPublisherIsSafe: a service wired without a stream must not need a nil check at every call site.
func TestNilPublisherIsSafe(t *testing.T) {
	var p *metricstream.EventPublisher
	one := 1
	p.SessionChanged("a", "b", "bound", &one)
	p.Alerted("c", "customer", "c", "mo_floor_reached", 0)

	metricstream.NewEventPublisher("svc", nil).Alerted("c", "customer", "c", "mo_floor_reached", 0)
}

func decodeOne(t *testing.T, sink *fakeSink, into any) {
	t.Helper()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.values) != 1 {
		t.Fatalf("got %d records, want 1", len(sink.values))
	}
	if err := json.Unmarshal(sink.values[0], into); err != nil {
		t.Fatalf("decode: %v", err)
	}
}
