package bindfailure_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/bindfailure"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

func failureAt(at time.Time, reason bindfailure.Reason) bindfailure.Failure {
	return bindfailure.Failure{At: at, RemoteIP: "203.0.113.7", BindType: "trx", CommandStatus: "ESME_RINVPASWD", Reason: reason}
}

func TestListReturnsNewestFirstFromSince(t *testing.T) {
	log := bindfailure.New(redistest.Client(t))
	ctx := context.Background()
	account := uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)

	for i, reason := range []bindfailure.Reason{bindfailure.ReasonPasswordMismatch, bindfailure.ReasonThrottled, bindfailure.ReasonCredentialRevoked} {
		if err := log.Record(ctx, account, failureAt(now.Add(time.Duration(i-2)*time.Hour), reason)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	if err := log.Record(ctx, uuid.New(), failureAt(now, bindfailure.ReasonThrottled)); err != nil {
		t.Fatalf("record other account: %v", err)
	}

	got, err := log.List(ctx, account, now.Add(-90*time.Minute))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []bindfailure.Failure{
		failureAt(now, bindfailure.ReasonCredentialRevoked),
		failureAt(now.Add(-time.Hour), bindfailure.ReasonThrottled),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("list = %+v, want %+v", got, want)
	}
}

func TestRecordCapsTheListAndBoundsItsLifetime(t *testing.T) {
	rdb := redistest.Client(t)
	log := bindfailure.New(rdb)
	ctx := context.Background()
	account := uuid.New()
	now := time.Now().UTC()

	for i := range bindfailure.Cap + 50 {
		if err := log.Record(ctx, account, failureAt(now.Add(time.Duration(i)*time.Millisecond), bindfailure.ReasonThrottled)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	got, err := log.List(ctx, account, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != bindfailure.Cap {
		t.Fatalf("len = %d, want the cap %d", len(got), bindfailure.Cap)
	}
	if want := now.Add(time.Duration(bindfailure.Cap+49) * time.Millisecond); !got[0].At.Equal(want) {
		t.Fatalf("head = %v, want the newest %v", got[0].At, want)
	}

	keys, err := rdb.Keys(ctx, "*"+account.String()+"*").Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys = %v (%v), want exactly one", keys, err)
	}
	ttl, err := rdb.TTL(ctx, keys[0]).Result()
	if err != nil || ttl <= 0 || ttl > bindfailure.Retention {
		t.Fatalf("ttl = %v (%v), want within (0, %v]", ttl, err, bindfailure.Retention)
	}
}

// Invariant a: the stored entry carries exactly these fields, none able to hold the presented secret.
func TestStoredEntryHasOnlyTheDeclaredFields(t *testing.T) {
	rdb := redistest.Client(t)
	ctx := context.Background()
	account := uuid.New()
	if err := bindfailure.New(rdb).Record(ctx, account, failureAt(time.Now(), bindfailure.ReasonPasswordMismatch)); err != nil {
		t.Fatalf("record: %v", err)
	}
	keys, _ := rdb.Keys(ctx, "*"+account.String()+"*").Result()
	if len(keys) != 1 {
		t.Fatalf("keys = %v", keys)
	}
	raw, err := rdb.LIndex(ctx, keys[0], 0).Result()
	if err != nil {
		t.Fatalf("lindex: %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	fields := make([]string, 0, len(entry))
	for k := range entry {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	if want := []string{"at", "bind_type", "command_status", "reason", "remote_ip"}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("stored fields = %v, want %v", fields, want)
	}
}
