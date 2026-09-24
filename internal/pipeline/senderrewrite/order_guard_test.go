package senderrewrite_test

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	senderidPkg      = "github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	senderrewritePkg = "github.com/martialanouman/go-gateway/internal/pipeline/senderrewrite"
)

// deps is every package the binary at cmd/<svc> reaches, directly or not.
func deps(t *testing.T, svc string) map[string]bool {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "./cmd/"+svc)
	cmd.Dir = filepath.Join("..", "..", "..")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps ./cmd/%s: %v", svc, err)
	}
	set := map[string]bool{}
	for p := range strings.FieldsSeq(string(out)) {
		set[p] = true
	}
	return set
}

// TestRewriteRunsAfterAuthorizationAndIsNeverReauthorized holds the order §6.16 sets against §6.19: the
// sender ID authorization sees the client's sender, and a rewritten one is never checked again — the
// rewrite is the provider's decision, so a sender refused at ingestion still leaves once rewritten.
// Both halves are a property of which binary reaches which package: the router authorizes and never
// rewrites, the pool rewrites and never authorizes.
func TestRewriteRunsAfterAuthorizationAndIsNeverReauthorized(t *testing.T) {
	router, pool := deps(t, "router-svc"), deps(t, "connector-pool-svc")
	if !router[senderidPkg] || !pool[senderrewritePkg] {
		t.Fatal("the router no longer authorizes or the pool no longer rewrites: this guard looks at the wrong binaries")
	}
	if router[senderrewritePkg] {
		t.Error("router-svc reaches senderrewrite: a rewrite before the sender ID authorization would let a " +
			"client's sender through §6.19 under the provider's name")
	}
	if pool[senderidPkg] {
		t.Error("connector-pool-svc reaches senderid: the pool could re-authorize a rewritten sender, which §6.16 forbids")
	}
}
