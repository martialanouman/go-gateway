package postgres

// Queued is the number of movements waiting to be taken by the batch writer.
func (b *BillingBatcher) Queued() int { return int(b.waiting.Load()) }
