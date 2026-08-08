package cancel_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
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

// TestClaimAsTheFreeHolderIsRefused pins that HolderNone cannot be claimed AS anything.
//
// The token arbitrates by value, and HolderNone is the value that means "free". Writing it would
// leave a key that every later claimant reads as an unheld token: the connector would dispatch and
// the Canceller would record a cancellation, both believing they had won — the arbitration silently
// stops arbitrating, with no error and no failing test anywhere. Refusing at the door is the only
// place that failure is visible.
func TestClaimAsTheFreeHolderIsRefused(t *testing.T) {
	rdb := redistest.Client(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()
	id := uuid.New()

	if _, err := flags.Claim(ctx, id, cancel.HolderNone); err == nil {
		t.Error("claiming as the free holder must be refused")
	}

	// And it must leave no trace: a written empty value would poison every later claim.
	n, err := rdb.Exists(ctx, "cancel:{"+id.String()+"}").Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if n != 0 {
		t.Error("a refused claim must not write the key")
	}
}

// TestRaceScenarioLeavesADeliveredMessageDelivered is the end-to-end statement of step-209, over the
// real token and the real rank table: accepted → cancel → enroute → delivered must NOT end at
// `cancelled`.
//
// It drives both actors in the order that used to produce the bug — the connector claims the token
// and dispatches, THEN a cancel_sm arrives while the CDR projection still reads `accepted` — and then
// resolves the final status the way ClickHouse does: the highest rank among the rows actually
// written. The row the Canceller does not write is what makes the outcome right, so the assertion has
// to span both the arbitration and the ranking.
func TestRaceScenarioLeavesADeliveredMessageDelivered(t *testing.T) {
	rdb := redistest.Client(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()

	row := rowWith(clickhouse.StatusAccepted) // the projection lags: it still says "queued"
	writer := &fakeWriter{}
	c := cancel.NewCanceller(fakeReader{row: row, found: true}, writer, flags, nil)

	// 1. The connector claims the token and puts the message on the wire.
	if _, err := flags.Claim(ctx, row.MessageID, cancel.HolderDispatched); err != nil {
		t.Fatalf("connector claim: %v", err)
	}

	// 2. A cancel_sm lands inside the projection-lag window.
	if err := c.Cancel(ctx, row.CustomerID, row.AccountID, row.MessageID); err == nil {
		t.Error("the cancel must be refused: the message is already on the wire")
	}

	// 3. The outcome projection and the carrier receipt write what really happened.
	written := append([]clickhouse.CDRRow{{Status: clickhouse.StatusAccepted}}, writer.rows...)
	written = append(written,
		clickhouse.CDRRow{Status: clickhouse.StatusEnroute},
		clickhouse.CDRRow{Status: clickhouse.StatusDelivered},
	)

	// ReplacingMergeTree keeps the highest rank, whatever the insertion order.
	final := written[0].Status
	for _, r := range written {
		if r.Status.Rank() > final.Rank() {
			final = r.Status
		}
	}
	if final != clickhouse.StatusDelivered {
		t.Errorf("final status = %q, want delivered — a delivered message must never read %q",
			final, final)
	}
}

// TestCancelledStillOutranksEveryOtherState pins that step-209 changed no rank. The fix works by not
// writing the wrong row, NOT by re-ordering the ladder — and that distinction is what leaves every
// historical row judged exactly as it was before. A rank change would silently re-resolve them all,
// since the rank IS the ReplacingMergeTree version.
func TestCancelledStillOutranksEveryOtherState(t *testing.T) {
	want := map[clickhouse.Status]uint64{
		clickhouse.StatusAccepted:  10,
		clickhouse.StatusRerouted:  15,
		clickhouse.StatusEnroute:   20,
		clickhouse.StatusRejected:  20,
		clickhouse.StatusDelivered: 40,
		clickhouse.StatusFailed:    50,
		clickhouse.StatusExpired:   50,
		clickhouse.StatusCancelled: 60,
	}
	for status, rank := range want {
		if got := status.Rank(); got != rank {
			t.Errorf("Rank(%q) = %d, want %d — moving a rank re-judges every historical row",
				status, got, rank)
		}
	}
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
