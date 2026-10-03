package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReplayWaitsOnlyAfterAnAttemptThatHandledNothing(t *testing.T) {
	c, rec, fault := &Consumer{group: "g"}, &kgo.Record{Topic: "t"}, errors.New("deadline exceeded")

	if a := nextAttempt(3, true); a != 0 {
		t.Errorf("an attempt that made progress left attempt at %d, want 0", a)
	}
	start := time.Now()
	if !c.replay(context.Background(), nextAttempt(3, true), rec, fault) || time.Since(start) > 100*time.Millisecond {
		t.Errorf("replay after progress waited %v, want an immediate replay", time.Since(start))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if c.replay(ctx, nextAttempt(0, false), rec, fault) {
		t.Error("an attempt that handled nothing replayed within 300 ms: an outage would be hammered without backoff")
	}
}
