package main

import (
	"context"
	"testing"
	"time"
)

type ensureCall struct {
	from time.Time
	days int
}

type fakeEnsurer struct{ calls chan ensureCall }

func (f fakeEnsurer) EnsureLedgerPartitions(_ context.Context, from time.Time, days int) error {
	f.calls <- ensureCall{from: from, days: days}
	return nil
}

// TestLedgerPartitionsAreEnsuredAtBoot: the first pass cannot wait for the hourly tick, or a fresh deployment
// writes its first hour into DEFAULT, where the ATTACH of that day then refuses it.
func TestLedgerPartitionsAreEnsuredAtBoot(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := fakeEnsurer{calls: make(chan ensureCall, 1)}
	done := make(chan error, 1)
	before := time.Now()
	go func() { done <- runLedgerPartitions(ctx, f, silentLogger()) }()

	select {
	case c := <-f.calls:
		if c.from.Before(before) || c.from.After(time.Now()) {
			t.Errorf("first pass starts at %v, want now", c.from)
		}
		if c.days != 8 {
			t.Errorf("first pass covers %d days, want today and the 7 next", c.days)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no pass at boot")
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("runLedgerPartitions returned %v on cancellation", err)
	}
}
