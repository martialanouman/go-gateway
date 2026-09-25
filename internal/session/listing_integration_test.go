package session_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

func describedBind(accountID, bindID string) session.Bind {
	return session.Bind{
		AccountID:  accountID,
		PodID:      "pod-list",
		BindID:     bindID,
		SystemID:   "sys-list",
		BindType:   "trx",
		RemoteAddr: "203.0.113.7",
		WindowSize: 10,
	}
}

func TestListAccountCarriesWhatTheBindDeclared(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithClock(clk.now))
	ctx := context.Background()
	account := uuid.NewString()
	first := describedBind(account, "b-"+uuid.NewString())

	if _, err := reg.Bind(ctx, first, 3); err != nil {
		t.Fatalf("bind: %v", err)
	}
	connectedAt := clk.now()
	clk.advance(20 * time.Second)
	if _, err := reg.Bind(ctx, first, 3); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	sessions, active, err := reg.ListAccount(ctx, account)
	if err != nil {
		t.Fatalf("list account: %v", err)
	}
	if active != 1 || len(sessions) != 1 {
		t.Fatalf("list account: active=%d sessions=%d, want 1 and 1", active, len(sessions))
	}
	got := sessions[0]
	if got.Bind != first {
		t.Fatalf("session = %+v, want %+v", got.Bind, first)
	}
	if !got.ConnectedAt.Equal(connectedAt) {
		t.Fatalf("connected_at = %v, want the first bind's %v — a refresh is not a reconnection", got.ConnectedAt, connectedAt)
	}
}

func TestARefusedBindLeavesNoTrace(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()
	refused := describedBind(account, "b-"+uuid.NewString())

	if _, err := reg.Bind(ctx, describedBind(account, "b-"+uuid.NewString()), 1); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if _, err := reg.Bind(ctx, refused, 1); !errors.Is(err, errs.ErrMaxSessionsExceeded) {
		t.Fatalf("bind over quota: err=%v, want ErrMaxSessionsExceeded", err)
	}

	// Read before Resolve, which would purge it: an entry nobody reads never expires.
	if n := indexEntries(t, rdb, refused.BindID); n != 0 {
		t.Fatalf("index holds %d entries for a refused bind, want 0", n)
	}
	if found, err := reg.Resolve(ctx, refused.BindID); err != nil || found {
		t.Fatalf("resolve refused bind: found=%v err=%v, want not found", found, err)
	}
	if n := rdb.HLen(ctx, "sess:{"+account+"}:meta").Val(); n != 1 {
		t.Fatalf("meta holds %d entries, want only the accepted bind's", n)
	}
}

func TestALapsedSessionVanishesFromEveryRead(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithSessionTTL(30*time.Second), session.WithClock(clk.now))
	ctx := context.Background()
	account := uuid.NewString()
	lapsed := describedBind(account, "b-"+uuid.NewString())

	if _, err := reg.Bind(ctx, lapsed, 2); err != nil {
		t.Fatalf("bind: %v", err)
	}
	clk.advance(31 * time.Second)

	// List first: Resolve purges the entry, after which List could not return it even unchecked.
	if listedAfter(t, reg, lapsed.BindID[:len(lapsed.BindID)-1], lapsed.BindID) {
		t.Fatal("global list returned a lapsed session")
	}
	if found, err := reg.Resolve(ctx, lapsed.BindID); err != nil || found {
		t.Fatalf("resolve lapsed: found=%v err=%v, want not found", found, err)
	}
	if n := indexEntries(t, rdb, lapsed.BindID); n != 0 {
		t.Fatalf("index still holds the lapsed session after a read, want it purged")
	}

	// A live bind of the same account sweeps the lapsed member; its meta must go with it, or a busy
	// account whose key never expires accumulates one meta per dead bind.
	if _, err := reg.Bind(ctx, describedBind(account, "b-"+uuid.NewString()), 2); err != nil {
		t.Fatalf("bind: %v", err)
	}
	sessions, active, err := reg.ListAccount(ctx, account)
	if err != nil || active != 1 || len(sessions) != 1 {
		t.Fatalf("list account: active=%d sessions=%d err=%v, want 1 live", active, len(sessions), err)
	}
	if n := rdb.HLen(ctx, "sess:{"+account+"}:meta").Val(); n != 1 {
		t.Fatalf("meta holds %d entries after the sweep, want 1", n)
	}
}

func TestUnbindRemovesTheSessionFromEveryRead(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	account := uuid.NewString()
	gone := describedBind(account, "b-"+uuid.NewString())

	if _, err := reg.Bind(ctx, gone, 2); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if found, err := reg.Resolve(ctx, gone.BindID); err != nil || !found {
		t.Fatalf("resolve live: found=%v err=%v, want found", found, err)
	}
	if _, err := reg.Unbind(ctx, gone); err != nil {
		t.Fatalf("unbind: %v", err)
	}

	if n := indexEntries(t, rdb, gone.BindID); n != 0 {
		t.Fatal("index still holds an unbound session")
	}
	if found, _ := reg.Resolve(ctx, gone.BindID); found {
		t.Fatal("resolve after unbind: found, want not found")
	}
	if n := rdb.HLen(ctx, "sess:{"+account+"}:meta").Val(); n != 0 {
		t.Fatalf("meta holds %d entries after unbind, want 0", n)
	}
}

func TestListPaginatesEveryLiveSessionOnceInBindOrder(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	reg := session.NewRegistry(rdb, session.WithSessionTTL(30*time.Second), session.WithClock(clk.now))
	ctx := context.Background()

	// The index is global and the container is shared: the run owns a prefix, and the page walk starts
	// just below it, so foreign entries only ever appear after this run's own.
	prefix := "p" + strings.ReplaceAll(uuid.NewString(), "-", "") + "-"
	// Two lapsed entries in one page-sized batch force a second fetch, which must resume after them.
	for _, suffix := range []string{"1", "2"} {
		if _, err := reg.Bind(ctx, describedBind(uuid.NewString(), prefix+suffix), 5); err != nil {
			t.Fatalf("bind lapsed %s: %v", suffix, err)
		}
	}
	clk.advance(20 * time.Second)
	suffixes := []string{"0", "3", "4", "5"}
	want := make([]string, 0, len(suffixes))
	for _, suffix := range suffixes {
		b := describedBind(uuid.NewString(), prefix+suffix)
		if _, err := reg.Bind(ctx, b, 5); err != nil {
			t.Fatalf("bind %s: %v", suffix, err)
		}
		want = append(want, b.BindID)
	}
	clk.advance(15 * time.Second)

	var got []string
	cursor := prefix
	for {
		page, next, err := reg.List(ctx, cursor, 2)
		if err != nil {
			t.Fatalf("list after %q: %v", cursor, err)
		}
		if len(page) > 2 {
			t.Fatalf("page of %d sessions, want at most the limit 2", len(page))
		}
		mine := 0
		for _, s := range page {
			if strings.HasPrefix(s.BindID, prefix) {
				got = append(got, s.BindID)
				mine++
			}
		}
		if next == "" || mine < len(page) {
			break
		}
		cursor = next
	}

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("paged bind ids = %v, want %v (each live session once, in order, the lapsed ones skipped)", got, want)
	}
}

func listedAfter(t *testing.T, reg *session.Registry, cursor, bindID string) bool {
	t.Helper()
	page, _, err := reg.List(context.Background(), cursor, 500)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, s := range page {
		if s.BindID == bindID {
			return true
		}
	}
	return false
}

func indexEntries(t *testing.T, rdb *redis.Client, bindID string) int {
	t.Helper()
	n, err := rdb.ZLexCount(context.Background(), "sess:idx", "["+bindID+" ", "("+bindID+" \xff").Result()
	if err != nil {
		t.Fatalf("zlexcount: %v", err)
	}
	return int(n)
}

// TestAnUnindexedBindStillHoldsItsAdmission: the index is written after bind.lua has taken the slot, so
// failing the bind on it would leave the caller refused while its slot stays counted for a whole TTL.
func TestAnUnindexedBindStillHoldsItsAdmission(t *testing.T) {
	rdb := redistest.Client(t)
	reg := session.NewRegistry(rdb)
	ctx := context.Background()
	if err := rdb.Set(ctx, "sess:idx", "not-a-zset", 0).Err(); err != nil {
		t.Fatalf("break the index: %v", err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), "sess:idx") })

	if _, err := reg.Bind(ctx, describedBind(uuid.NewString(), "b-"+uuid.NewString()), 1); err != nil {
		t.Fatalf("bind with an unwritable index: %v — the admission stands, the next refresh indexes it", err)
	}
}
