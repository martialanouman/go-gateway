package senderrewrite_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	senderidPkg      = "github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	senderrewritePkg = "github.com/martialanouman/go-gateway/internal/pipeline/senderrewrite"
)

// imports lists the packages the non-test Go files of dir import, one set for the whole directory.
func imports(t *testing.T, dir string) map[string]bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no Go file in %s (%v): the guard would pass on nothing", dir, err)
	}
	out := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), f, src, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, imp := range file.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			out[path] = true
		}
	}
	return out
}

// TestRewriteRunsAfterAuthorizationAndIsNeverReauthorized holds the order §6.16 sets against §6.19: the
// sender ID authorization sees the client's sender, and a rewritten one is never checked again — the
// rewrite is the provider's decision, so a sender refused at ingestion still leaves once rewritten.
// Both halves are a property of which package calls which, so that is what this checks: the router's
// side never reaches the rewrite, the pool's side never reaches the authorization.
func TestRewriteRunsAfterAuthorizationAndIsNeverReauthorized(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	routerSvc := filepath.Join(root, "cmd", "router-svc")
	if !imports(t, routerSvc)[senderidPkg] {
		t.Fatalf("%s no longer wires %s: the guard below no longer looks where authorization runs", routerSvc, senderidPkg)
	}
	for _, dir := range []string{routerSvc, filepath.Join(root, "internal", "pipeline"), filepath.Join(root, "internal", "router")} {
		if imports(t, dir)[senderrewritePkg] {
			t.Errorf("%s imports %s: a rewrite before the sender ID authorization would let a client's "+
				"sender through §6.19 under the provider's name", dir, senderrewritePkg)
		}
	}
	for _, dir := range []string{filepath.Join(root, "internal", "connectorpool"), filepath.Join(root, "cmd", "connector-pool-svc")} {
		if imports(t, dir)[senderidPkg] {
			t.Errorf("%s imports %s: the pool would re-authorize a rewritten sender, which §6.16 forbids", dir, senderidPkg)
		}
	}
}
