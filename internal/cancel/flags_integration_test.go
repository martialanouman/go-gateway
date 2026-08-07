package cancel_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestClaimSingleWinner is the test that carries step-209: the cancel token must have EXACTLY ONE
// winner under concurrency. It is what lets the Canceller and the connector pool arbitrate without a
// shared lock, and it runs against real Redis because the guarantee lives in `SET … NX GET`, not in
// our code — a mock would only assert what we assumed.
//
// Half the goroutines claim as the connector, half as a cancel_sm, all on the SAME message.
func TestClaimSingleWinner(t *testing.T) {
	rdb := redistest.Client(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()
	id := uuid.New()

	const claimants = 16
	holders := make([]cancel.Holder, claimants)
	errsOut := make([]error, claimants)

	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := range claimants {
		done.Add(1)
		go func() {
			defer done.Done()
			as := cancel.HolderDispatched
			if i%2 == 0 {
				as = cancel.HolderCancel
			}
			start.Wait() // release them together, so they really do contend
			holders[i], errsOut[i] = flags.Claim(ctx, id, as)
		}()
	}
	start.Done()
	done.Wait()

	winners := 0
	for i, h := range holders {
		if errsOut[i] != nil {
			t.Fatalf("Claim[%d]: %v", i, errsOut[i])
		}
		if h == cancel.HolderNone {
			winners++
			continue
		}
		if h != cancel.HolderCancel && h != cancel.HolderDispatched {
			t.Errorf("Claim[%d] = %q, want a known holder", i, h)
		}
	}
	if winners != 1 {
		t.Errorf("winners = %d, want exactly 1 — the token is not single-winner", winners)
	}
}

// TestClaimReportsTheHolder pins the three answers the callers branch on. The third case is the one
// step-209 exists for: a cancel_sm arriving after the connector claimed must LOSE and learn who won,
// so it can refuse instead of writing a cancelled row on a message already on the wire.
func TestClaimReportsTheHolder(t *testing.T) {
	rdb := redistest.Client(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()

	t.Run("free token is taken by the caller", func(t *testing.T) {
		got, err := flags.Claim(ctx, uuid.New(), cancel.HolderCancel)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if got != cancel.HolderNone {
			t.Errorf("Claim on a free token = %q, want HolderNone", got)
		}
	})

	t.Run("the connector recognises its own token after a redelivery", func(t *testing.T) {
		id := uuid.New()
		if _, err := flags.Claim(ctx, id, cancel.HolderDispatched); err != nil {
			t.Fatalf("first Claim: %v", err)
		}

		got, err := flags.Claim(ctx, id, cancel.HolderDispatched)
		if err != nil {
			t.Fatalf("second Claim: %v", err)
		}
		if got != cancel.HolderDispatched {
			t.Errorf("re-claim = %q, want HolderDispatched — the connector must not read its own "+
				"token as a cancellation", got)
		}
	})

	t.Run("a cancel arriving after dispatch loses and learns why", func(t *testing.T) {
		id := uuid.New()
		if _, err := flags.Claim(ctx, id, cancel.HolderDispatched); err != nil {
			t.Fatalf("connector Claim: %v", err)
		}

		got, err := flags.Claim(ctx, id, cancel.HolderCancel)
		if err != nil {
			t.Fatalf("cancel Claim: %v", err)
		}
		if got != cancel.HolderDispatched {
			t.Errorf("cancel after dispatch = %q, want HolderDispatched", got)
		}

		// And it must not have OVERWRITTEN the token on its way out. Reporting the right holder is
		// not enough: if the losing claim still wrote its own value, the connector would read
		// "cancel" on a Kafka redelivery and skip a message it has already put on the wire.
		survived, err := flags.Claim(ctx, id, cancel.HolderDispatched)
		if err != nil {
			t.Fatalf("connector re-Claim: %v", err)
		}
		if survived != cancel.HolderDispatched {
			t.Errorf("token = %q after a losing cancel claim, want HolderDispatched — the loser "+
				"overwrote the winner", survived)
		}
	})

	t.Run("the connector honours a cancel that got there first", func(t *testing.T) {
		id := uuid.New()
		if _, err := flags.Claim(ctx, id, cancel.HolderCancel); err != nil {
			t.Fatalf("cancel Claim: %v", err)
		}

		got, err := flags.Claim(ctx, id, cancel.HolderDispatched)
		if err != nil {
			t.Fatalf("connector Claim: %v", err)
		}
		if got != cancel.HolderCancel {
			t.Errorf("dispatch after cancel = %q, want HolderCancel", got)
		}
	})
}

// TestClaimDoesNotExtendTheWinnersTTL pins that a losing claim leaves the winner's expiry alone. It
// matters for the connector's short-lived token: a stream of late cancel_sm retries must not keep
// renewing it, and a cancel that lost must not shorten a token it does not own.
func TestClaimDoesNotExtendTheWinnersTTL(t *testing.T) {
	rdb := redistest.Client(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()
	id := uuid.New()

	if _, err := flags.Claim(ctx, id, cancel.HolderDispatched); err != nil {
		t.Fatalf("connector Claim: %v", err)
	}
	before, err := rdb.TTL(ctx, "cancel:{"+id.String()+"}").Result()
	if err != nil {
		t.Fatalf("TTL before: %v", err)
	}

	// A cancel_sm loses. Its own TTL (72h) must not land on the connector's token.
	if _, err := flags.Claim(ctx, id, cancel.HolderCancel); err != nil {
		t.Fatalf("cancel Claim: %v", err)
	}
	after, err := rdb.TTL(ctx, "cancel:{"+id.String()+"}").Result()
	if err != nil {
		t.Fatalf("TTL after: %v", err)
	}

	if after > before {
		t.Errorf("TTL grew from %v to %v — a losing claim must not touch the winner's expiry",
			before, after)
	}
}
