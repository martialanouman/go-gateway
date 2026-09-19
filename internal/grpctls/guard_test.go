package grpctls_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Proving grpctls says nothing about whether a service uses it, and what goes wrong in practice is a
// future step adding one more plaintext dial without thinking about it. That is what these two check.

// scanRoots are the trees holding production code. test/ is tooling run with go run, never shipped.
var scanRoots = []string{"../../cmd", "../../internal"}

// minScanned is the floor a broken path falls through. Without it, a root that stopped resolving would
// scan nothing and report a clean repository.
const minScanned = 200

const (
	insecurePkg = "google.golang.org/grpc/credentials/insecure"
	grpcPkg     = "google.golang.org/grpc"
	grpctlsPkg  = "github.com/martialanouman/go-gateway/internal/grpctls"
)

func TestNoProductionCodeDialsGRPCInPlaintext(t *testing.T) {
	t.Parallel()

	var offenders []string
	scanned := walkProduction(t, func(path string, file *ast.File, fset *token.FileSet) {
		imports := importNames(file)
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && calls(imports, call, insecurePkg, "NewCredentials") {
				offenders = append(offenders, position(path, fset, call.Pos()))
			}
			return true
		})
	})

	requireFloor(t, scanned)
	for _, o := range offenders {
		t.Errorf("%s dials gRPC in plaintext — every internal call goes through grpctls.DialOption (step-300b)", o)
	}
}

func TestEveryGRPCServerIsBuiltWithItsCredentials(t *testing.T) {
	t.Parallel()

	var bare []string
	scanned := walkProduction(t, func(path string, file *ast.File, fset *token.FileSet) {
		imports := importNames(file)
		// An argument count proves nothing: grpc.NewServer(grpc.EmptyServerOption{}) serves in plaintext
		// and carries an option. So the argument has to be traced back to grpctls.ServerOption, either
		// called inline or through the variable it was assigned to.
		fromServerOption := map[string]bool{}
		ast.Inspect(file, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, rhs := range assign.Rhs {
				call, ok := rhs.(*ast.CallExpr)
				if !ok || !calls(imports, call, grpctlsPkg, "ServerOption") {
					continue
				}
				// One call, several results: the option is the first.
				if i < len(assign.Lhs) || len(assign.Rhs) == 1 {
					if id, ok := assign.Lhs[0].(*ast.Ident); ok {
						fromServerOption[id.Name] = true
					}
				}
			}
			return true
		})

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !calls(imports, call, grpcPkg, "NewServer") {
				return true
			}
			for _, arg := range call.Args {
				switch a := arg.(type) {
				case *ast.Ident:
					if fromServerOption[a.Name] {
						return true
					}
				case *ast.CallExpr:
					if calls(imports, a, grpctlsPkg, "ServerOption") {
						return true
					}
				}
			}
			bare = append(bare, position(path, fset, call.Pos()))
			return true
		})
	})

	requireFloor(t, scanned)
	for _, b := range bare {
		t.Errorf("%s builds a gRPC server without grpctls.ServerOption — it would serve in plaintext (step-300b)", b)
	}
}

// calls reports whether call is pkgPath.name, resolving the package through the file's own imports so
// that an alias — noTLS "…/credentials/insecure" — does not walk past the guard.
func calls(imports map[string]string, call *ast.CallExpr, pkgPath, name string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && imports[id.Name] == pkgPath
}

// importNames maps the name a file uses for each import onto its path. A dot-import has no name and is
// left out: it would make every bare identifier ambiguous, and this repository has none.
func importNames(file *ast.File) map[string]string {
	out := make(map[string]string, len(file.Imports))
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		out[name] = path
	}
	return out
}

func requireFloor(t *testing.T, scanned int) {
	t.Helper()
	if scanned < minScanned {
		t.Fatalf("scanned only %d production files, expected at least %d: the roots stopped resolving", scanned, minScanned)
	}
}

// walkProduction parses every non-test Go file under the scan roots and returns how many it read.
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
			// The one place allowed to build the plaintext path.
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
