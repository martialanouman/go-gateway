package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// seedFor writes one CDR row for the given customer and destination, in today's partition.
func seedFor(t *testing.T, conn *clickhouse.Conn, customerID uuid.UUID, dest string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	row := clickhouse.CDRRow{
		MessageID: id, AccountID: uuid.New(), CustomerID: customerID,
		Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: dest,
		SubmittedAt: time.Now().UTC(), Status: clickhouse.StatusAccepted,
		SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
	}
	if err := clickhouse.NewCDRWriter(conn).Insert(context.Background(), row); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	return id
}

func rowExists(t *testing.T, conn *clickhouse.Conn, messageID uuid.UUID) bool {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM cdr WHERE message_id = ?", messageID).Scan(&n); err != nil {
		t.Fatalf("count message %s: %v", messageID, err)
	}
	return n > 0
}

// TestEraseMSISDNAcrossCustomers: an RGPD erasure by phone number removes that number's rows wherever they
// are — across every customer (§14) — and leaves every other message untouched.
func TestEraseMSISDNAcrossCustomers(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	eraser := clickhouse.NewCDREraser(conn)

	// The same subject wrote to by two different customers, plus an unrelated message.
	subject := "22507000001"
	other := "22507000002"
	custA, custB := uuid.New(), uuid.New()
	inA := seedFor(t, conn, custA, subject)
	inB := seedFor(t, conn, custB, subject)
	untouched := seedFor(t, conn, custA, other)

	erased, err := eraser.EraseMSISDN(ctx, subject)
	if err != nil {
		t.Fatalf("EraseMSISDN: %v", err)
	}
	if erased != 2 {
		t.Errorf("erased %d rows, want 2 (one per customer)", erased)
	}
	if rowExists(t, conn, inA) || rowExists(t, conn, inB) {
		t.Error("the subject's rows must be gone from every customer")
	}
	if !rowExists(t, conn, untouched) {
		t.Error("an unrelated message must survive the erasure")
	}

	// Idempotent: erasing again finds nothing.
	if again, err := eraser.EraseMSISDN(ctx, subject); err != nil || again != 0 {
		t.Errorf("second erase = (%d, %v), want (0, nil)", again, err)
	}
}

// TestEraseCustomer: a customer erasure removes that customer's CDR rows and nobody else's.
func TestEraseCustomer(t *testing.T) {
	conn := retentionConn(t)
	ctx := context.Background()
	eraser := clickhouse.NewCDREraser(conn)

	target, bystander := uuid.New(), uuid.New()
	gone1 := seedFor(t, conn, target, "22507000003")
	gone2 := seedFor(t, conn, target, "22507000004")
	kept := seedFor(t, conn, bystander, "22507000005")

	erased, err := eraser.EraseCustomer(ctx, target)
	if err != nil {
		t.Fatalf("EraseCustomer: %v", err)
	}
	if erased != 2 {
		t.Errorf("erased %d rows, want 2", erased)
	}
	if rowExists(t, conn, gone1) || rowExists(t, conn, gone2) {
		t.Error("the erased customer's rows must be gone")
	}
	if !rowExists(t, conn, kept) {
		t.Error("another customer's rows must survive")
	}
}

// TestEraseMSISDNRejectsMalformed: the number is interpolated into the mutation, so anything that is not a
// plain normalised number is refused rather than executed.
func TestEraseMSISDNRejectsMalformed(t *testing.T) {
	conn := retentionConn(t)
	eraser := clickhouse.NewCDREraser(conn)
	for _, bad := range []string{"", "+22507000001", "225'0700", "225 070", "abc", "1' OR '1'='1"} {
		if _, err := eraser.EraseMSISDN(context.Background(), bad); err == nil {
			t.Errorf("EraseMSISDN(%q) succeeded, want a refusal", bad)
		}
	}
}
