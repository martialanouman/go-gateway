package controlplane

// EffectiveAccountStatus returns the status a client actually experiences on an account: the more
// restrictive of the owning customer's status and the account's own (ADR-0006). Suspending a
// customer therefore suspends every account under it, without touching each account's stored
// status. Ordering is active < suspended < closed; the higher rank wins.
func EffectiveAccountStatus(customer CustomerStatus, account AccountStatus) AccountStatus {
	if customer.Rank() > account.Rank() {
		// The customer is the more restrictive of the two. Project its status onto the account's
		// type: the values are identical, only the Go type differs.
		return AccountStatus(customer)
	}
	return account
}
