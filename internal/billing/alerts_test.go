package billing_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/billing/pb"
)

type recordingAlerter struct {
	mu        sync.Mutex
	alerts    []string
	customers []string
	owners    []string
}

func (a *recordingAlerter) Alerted(customerID, ownerType, ownerID, alert string, balance int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.alerts = append(a.alerts, alert)
	a.customers = append(a.customers, customerID)
	a.owners = append(a.owners, ownerID)
}

func (a *recordingAlerter) last() (customer, owner string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.customers[len(a.customers)-1], a.owners[len(a.owners)-1]
}

func (a *recordingAlerter) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.alerts)
}

// fixedCore returns a canned RecordMO result so the alert edge can be tested without a store.
type fixedCore struct {
	billing.Core
	result billing.MOResult
}

func (c fixedCore) RecordMO(context.Context, billing.Owner, uuid.UUID, int) (billing.MOResult, error) {
	return c.result, nil
}

// TestMOFloorAlertsExactlyOnce: FloorReached is true on the single MO that drove the meter to its floor, so
// the alert must follow it and not the suppressed MOs that come after — an alert per MO past the floor would
// flood the dashboard for as long as traffic continues.
func TestMOFloorAlertsExactlyOnce(t *testing.T) {
	cust := uuid.NewString()
	owner := &pb.Owner{
		OwnerType:  pb.OwnerType_OWNER_TYPE_CUSTOMER,
		OwnerId:    cust,
		CustomerId: cust,
	}

	t.Run("the MO that reaches the floor", func(t *testing.T) {
		alerter := &recordingAlerter{}
		srv := billing.NewServer(fixedCore{result: billing.MOResult{FloorReached: true}}, nil, alerter)
		if _, err := srv.RecordMO(context.Background(), &pb.RecordMORequest{
			MessageId: uuid.NewString(), Owner: owner, Credits: 1,
		}); err != nil {
			t.Fatalf("RecordMO: %v", err)
		}
		if alerter.count() != 1 {
			t.Errorf("alerts = %d, want 1", alerter.count())
		}
	})

	t.Run("a later suppressed MO", func(t *testing.T) {
		alerter := &recordingAlerter{}
		srv := billing.NewServer(fixedCore{result: billing.MOResult{Suppressed: true}}, nil, alerter)
		if _, err := srv.RecordMO(context.Background(), &pb.RecordMORequest{
			MessageId: uuid.NewString(), Owner: owner, Credits: 1,
		}); err != nil {
			t.Fatalf("RecordMO: %v", err)
		}
		if alerter.count() != 0 {
			t.Errorf("alerts = %d, want none past the floor", alerter.count())
		}
	})
}

// TestRecordMOWithoutAnAlerter: a deployment with no realtime stream must not need a nil check per call site.
func TestRecordMOWithoutAnAlerter(t *testing.T) {
	cust := uuid.NewString()
	owner := &pb.Owner{
		OwnerType:  pb.OwnerType_OWNER_TYPE_CUSTOMER,
		OwnerId:    cust,
		CustomerId: cust,
	}
	srv := billing.NewServer(fixedCore{result: billing.MOResult{FloorReached: true}}, nil, nil)
	if _, err := srv.RecordMO(context.Background(), &pb.RecordMORequest{
		MessageId: uuid.NewString(), Owner: owner, Credits: 1,
	}); err != nil {
		t.Fatalf("RecordMO: %v", err)
	}
}

// TestAlertNamesTheCustomerNotTheBalanceOwner is the regression test for a bug the first version shipped with
// and the first test hid: the MO meter is owner-scoped, so under balance_scope=smpp_account the owner is an
// ACCOUNT. Alerting with owner.ID left a dashboard unable to resolve any customer. Both ids now travel.
func TestAlertNamesTheCustomerNotTheBalanceOwner(t *testing.T) {
	customer, account := uuid.NewString(), uuid.NewString()
	alerter := &recordingAlerter{}
	srv := billing.NewServer(fixedCore{result: billing.MOResult{FloorReached: true}}, nil, alerter)

	if _, err := srv.RecordMO(context.Background(), &pb.RecordMORequest{
		MessageId: uuid.NewString(),
		Owner: &pb.Owner{
			OwnerType:  pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT,
			OwnerId:    account,
			CustomerId: customer,
		},
		Credits: 1,
	}); err != nil {
		t.Fatalf("RecordMO: %v", err)
	}

	gotCustomer, gotOwner := alerter.last()
	if gotCustomer != customer {
		t.Errorf("customer_id = %q, want the customer %q", gotCustomer, customer)
	}
	if gotOwner != account {
		t.Errorf("owner_id = %q, want the account %q", gotOwner, account)
	}
}
