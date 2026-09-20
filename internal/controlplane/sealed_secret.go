package controlplane

// SealedSecret is a control-plane secret the gateway REPLAYS to a third party — an outbound SMSC bind
// password, an external billing provider's credentials — in the only form it is ever stored in: sealed by
// content-key-svc, beside the reference of the master key that sealed it (ADR-0016).
//
// Holding one is not holding the secret: the bytes are meaningless without a call to
// ConfigSecrets.Open, which is why this type can travel through the control plane where a plaintext
// could not. KMSKeyRef is not needed to open it — the KMS unwraps with the key it holds — it says which
// master key a row belongs to, so an operator can tell, and a future key rotation knows what to re-seal.
//
// The secrets the gateway VERIFIES instead of replaying (an inbound bind password, an API key) are NOT
// this: they stay one-way argon2id/SHA-256 hashes, and nothing about them is reversible.
type SealedSecret struct {
	Sealed    []byte
	KMSKeyRef string
}
