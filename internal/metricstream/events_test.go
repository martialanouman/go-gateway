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

// TestRateCappedDropsAreCountedApart: the cap is expected to fire under load, an unserializable record is a
// bug. One counter for both would bury the bug in the noise of the cap, so each reason gets its own — and the
// exposition splits them by label (see metrics.StreamDropCollector).
//
// The assertion is exact and independent of where the one-second window falls: every call is either published
// or counted, never both, never neither.
func TestRateCappedDropsAreCountedApart(t *testing.T) {
	const calls = 200 // comfortably past the 50/s cap even if the burst straddles a window boundary

	sink := &fakeSink{}
	p := metricstream.NewEventPublisher("smpp-server-svc", sink)

	one := 1
	for range calls {
		p.SessionChanged("acct-1", "ACME01", "bound", &one)
	}

	sink.mu.Lock()
	published := len(sink.values)
	sink.mu.Unlock()

	capped := p.DroppedRateCapped()
	if published+int(capped) != calls {
		t.Errorf("published %d + rate-capped %d = %d, want %d — a call was lost from both sides",
			published, capped, published+int(capped), calls)
	}
	if capped == 0 {
		t.Errorf("rate-capped = 0 after %d calls in one burst, want the cap to have fired", calls)
	}
	if got := p.DroppedUnserializable(); got != 0 {
		t.Errorf("unserializable = %d, want 0 — a rate-cap drop must not land on the encode counter", got)
	}
}

// TestAlertsAreNotRateCapped: the cap guards the session feed, where a pod drain can produce thousands of
// records. A billing alert fires once per owner per period, and silencing one would hide the very transition
// the operator is waiting for.
func TestAlertsAreNotRateCapped(t *testing.T) {
	const calls = 200

	sink := &fakeSink{}
	p := metricstream.NewEventPublisher("billing-svc", sink)
	for range calls {
		p.Alerted("cust-1", "customer", "cust-1", "mo_floor_reached", 0)
	}

	sink.mu.Lock()
	published := len(sink.values)
	sink.mu.Unlock()
	if published != calls {
		t.Errorf("published %d alerts, want all %d", published, calls)
	}
	if capped := p.DroppedRateCapped(); capped != 0 {
		t.Errorf("rate-capped = %d, want alerts to be exempt", capped)
	}
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
