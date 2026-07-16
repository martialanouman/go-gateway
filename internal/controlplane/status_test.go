package controlplane_test

import (
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// TestEffectiveAccountStatusIsTheMostRestrictiveOfTheTwo pins ADR-0006: a client experiences the
// harsher of the customer's status and the account's own, so suspending a customer suspends every
// account under it without editing each account row.
func TestEffectiveAccountStatusIsTheMostRestrictiveOfTheTwo(t *testing.T) {
	tests := []struct {
		name     string
		customer cp.CustomerStatus
		account  cp.AccountStatus
		want     cp.AccountStatus
	}{
		{"both active", cp.CustomerActive, cp.AccountActive, cp.AccountActive},
		{"customer suspended wins", cp.CustomerSuspended, cp.AccountActive, cp.AccountSuspended},
		{"account suspended wins", cp.CustomerActive, cp.AccountSuspended, cp.AccountSuspended},
		{"customer closed wins over suspended", cp.CustomerClosed, cp.AccountSuspended, cp.AccountClosed},
		{"account closed wins over active", cp.CustomerActive, cp.AccountClosed, cp.AccountClosed},
		{"equal suspended", cp.CustomerSuspended, cp.AccountSuspended, cp.AccountSuspended},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cp.EffectiveAccountStatus(tc.customer, tc.account); got != tc.want {
				t.Errorf("EffectiveAccountStatus(%q, %q) = %q, want %q", tc.customer, tc.account, got, tc.want)
			}
		})
	}
}
