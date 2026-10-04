package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type ensureCall struct {
	from time.Time
	days int
}

// fakeEnsurer fails its first pass, as a day whose range DEFAULT already holds would.
type fakeEnsurer struct {
	calls chan ensureCall
	done  <-chan struct{}
}

func (f fakeEnsurer) EnsureLedgerPartitions(_ context.Context, from time.Time, days int) error {
	select {
	case f.calls <- ensureCall{from: from, days: days}:
	case <-f.done:
	}
	return errors.New("partition of the day overlaps rows in DEFAULT")
}

// TestLedgerPartitionsAreEnsuredAtBootThenAgain: the first pass cannot wait for the hourly tick, or a fresh
// deployment writes its first hour into DEFAULT; and a failed pass must neither stop the loop nor the
// service, nor freeze the days it covers.
func TestLedgerPartitionsAreEnsuredAtBootThenAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := fakeEnsurer{calls: make(chan ensureCall), done: ctx.Done()}
	done := make(chan error, 1)
	before := time.Now()
	const every = 300 * time.Millisecond
	go func() { done <- runLedgerPartitions(ctx, f, every, silentLogger()) }()

	next := func() ensureCall {
		t.Helper()
		select {
		case c := <-f.calls:
			return c
		case <-time.After(5 * time.Second):
			t.Fatal("no pass")
			return ensureCall{}
		}
	}
	first := next()
	if waited := time.Since(before); waited >= every/2 {
		t.Errorf("first pass came after %v, at the tick: none at boot", waited)
	}
	if first.from.Before(before) || first.from.After(time.Now()) {
		t.Errorf("first pass starts at %v, want now", first.from)
	}
	if first.days != 8 {
		t.Errorf("first pass covers %d days, want today and the 7 next", first.days)
	}
	if second := next(); !second.from.After(first.from) {
		t.Errorf("second pass starts at %v, not after the first (%v): the days never move", second.from, first.from)
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("runLedgerPartitions returned %v", err)
	}
}
