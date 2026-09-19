package grpctls_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// The guard below is what keeps the wiring honest across ten binaries. Proving grpctls itself says
// nothing about whether a service uses it, and proving each of twelve call sites by hand would mean a
// dozen container-backed tests that still would not catch the thirteenth. What actually goes wrong is
// a future step adding one more plaintext dial without thinking about it — so that is what is checked.

// scanRoots are the trees holding production code. test/ is tooling run with go run, never shipped.
var scanRoots = []string{"../../cmd", "../../internal"}

// minScanned is the floor a broken path falls through. Without it, a root that stopped resolving would
// scan nothing and report a clean repository.
const minScanned = 200

func TestNoProductionCodeDialsGRPCInPlaintext(t *testing.T) {
	t.Parallel()

	var offenders []string
	scanned := walkProduction(t, func(path string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NewCredentials" {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "insecure" {
				offenders = append(offenders, position(path, fset, call.Pos()))
			}
			return true
		})
	})

	if scanned < minScanned {
		t.Fatalf("scanned only %d production files, expected at least %d: the roots stopped resolving", scanned, minScanned)
	}
	for _, o := range offenders {
		t.Errorf("%s dials gRPC in plaintext — every internal call goes through grpctls.DialOption (step-300b)", o)
	}
}

func TestEveryGRPCServerIsBuiltWithItsCredentials(t *testing.T) {
	t.Parallel()

	var bare []string
	walkProduction(t, func(path string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NewServer" || len(call.Args) > 0 {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "grpc" {
				bare = append(bare, position(path, fset, call.Pos()))
			}
			return true
		})
	})

	// An option count, not an option identity: what this catches is a server built with nothing at all,
	// which is how all four of them looked before step-300b. Which option it carries is the business of
	// the tests that dial it.
	for _, b := range bare {
		t.Errorf("%s builds a gRPC server with no options — it needs grpctls.ServerOption (step-300b)", b)
	}
}

// walkProduction parses every non-test Go file under the scan roots and returns how many it read.
// internal/testutil holds helpers compiled only into tests; they are free to dial in plaintext.
func walkProduction(t *testing.T, visit func(path string, file *ast.File, fset *token.FileSet)) int {
	t.Helper()
	fset := token.NewFileSet()
	scanned := 0
	for _, root := range scanRoots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			switch {
			case err != nil:
				return err
			case d.IsDir():
				return nil
			case !strings.HasSuffix(path, ".go"), strings.HasSuffix(path, "_test.go"):
				return nil
			case strings.Contains(filepath.ToSlash(path), "/testutil/"):
				return nil
			// This package builds the plaintext path — it is the one place allowed to, and the whole point
			// of the guard is that it stays the only one.
			case strings.Contains(filepath.ToSlash(path), "/internal/grpctls/"):
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			scanned++
			visit(path, file, fset)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	return scanned
}

func position(path string, fset *token.FileSet, pos token.Pos) string {
	return filepath.ToSlash(path) + ":" + strings.TrimPrefix(fset.Position(pos).String(), fset.Position(pos).Filename+":")
}
