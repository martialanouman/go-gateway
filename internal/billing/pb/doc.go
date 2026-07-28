// Package pb holds the generated Go bindings for the billing gRPC contract (api/proto/billing.proto,
// plan §6.9/§13, M9). It is produced by `make proto` (buf + protoc-gen-go/-go-grpc) and committed;
// never hand-edit the generated files. billing-svc implements the BillingServer, and the pipeline
// (router, connector pool) calls it through the BillingClient. The contract — not this package — is the
// source of truth; regenerate after any change to billing.proto.
package pb
