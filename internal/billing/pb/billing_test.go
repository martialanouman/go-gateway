package pb_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
)

// TestBillingMessagesRoundTrip is the generation freshness guard (step-140): it instantiates the
// generated messages, exercises the enums the contract shares with the schema, and round-trips a request
// through proto marshal/unmarshal. A stale or missing regeneration breaks the build here; a wire-format
// regression breaks the round-trip.
func TestBillingMessagesRoundTrip(t *testing.T) {
	// Enum values mirror db/schema_passerelle_sms.sql (balances/billing_ledger).
	if pb.Direction_DIRECTION_MT == pb.Direction_DIRECTION_MO {
		t.Fatal("mt and mo must be distinct directions")
	}
	if pb.OwnerType_OWNER_TYPE_CUSTOMER == pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT {
		t.Fatal("customer and smpp_account must be distinct owner types")
	}

	req := &pb.ReserveRequest{
		MessageId: "0190a0b1-c2d3-7e4f-8a9b-0c1d2e3f4a5b",
		Owner: &pb.Owner{
			OwnerType:  pb.OwnerType_OWNER_TYPE_CUSTOMER,
			OwnerId:    "0190a0b1-c2d3-7e4f-8a9b-0c1d2e3f4a5c",
			CustomerId: "0190a0b1-c2d3-7e4f-8a9b-0c1d2e3f4a5c",
		},
		Credits: 3,
	}

	wire, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got pb.ReserveRequest
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(req, &got) {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", &got, req)
	}
	if got.GetCredits() != 3 || got.GetOwner().GetOwnerType() != pb.OwnerType_OWNER_TYPE_CUSTOMER {
		t.Errorf("fields not preserved: credits=%d owner_type=%v", got.GetCredits(), got.GetOwner().GetOwnerType())
	}
}

// Compile-time surface guards: the generated server/client interfaces exist and the embeddable
// UnimplementedBillingServer satisfies BillingServer, so a downstream change is a compile error here.
var (
	_ pb.BillingServer = pb.UnimplementedBillingServer{}
	_ pb.BillingClient // the client interface exists (zero value is a nil interface)
)
