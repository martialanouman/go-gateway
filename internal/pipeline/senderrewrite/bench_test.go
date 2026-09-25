package senderrewrite_test

import (
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderrewrite"
)

// BenchmarkRewrite is the per-segment cost the pool adds before each submit_sm (step-350 PR2). "worst"
// walks every scope and misses on patterns before the platform rule matches — the worst case of a
// small rule set.
func BenchmarkRewrite(b *testing.B) {
	sanitize := cp.SenderRewriteRule{Scope: cp.RewriteScopePlatform, Direction: "mt", Status: "active", RewriteType: cp.RewriteSanitize}
	missConnector := static(cp.RewriteScopeConnector, &connectorID, 1, "X")
	missConnector.MatchSenderPattern = ptr("[0-9]{6,15}")
	missAccount := static(cp.RewriteScopeAccount, &accountID, 1, "X")
	missAccount.MatchDestPattern = ptr("33.*")
	pool := cp.SenderRewriteRule{
		Scope: cp.RewriteScopeCustomer, ScopeID: &customerID, Direction: "mt", Status: "active",
		RewriteType: cp.RewriteFallbackPool, FallbackPool: []string{"A", "B"}, MatchSenderPattern: ptr("OTHER"),
	}
	truncate := cp.SenderRewriteRule{
		Scope: cp.RewriteScopePlatform, Direction: "mt", Status: "active",
		RewriteType: cp.RewriteTruncate, MaxLength: ptr[int32](11), MatchSenderPattern: ptr("[A-Z]{12,}"),
	}
	for name, rules := range map[string][]cp.SenderRewriteRule{
		"none":  nil,
		"worst": {missConnector, missAccount, pool, truncate, sanitize},
	} {
		s := senderrewrite.Build(rules, nil)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				rewrite(s, "ACME-SHOP")
			}
		})
	}
}
