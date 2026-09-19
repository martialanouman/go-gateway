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
// future step building one more server or client without thinking about the identity it carries.
//
// The rule is about WHICH FUNCTION is called, not about what an argument holds. An earlier version
// traced the argument back to grpctls.ServerOption by name, and a review broke it twice: reassigning
// the variable after the good line, and reusing that name in another function of the same file, both
// shipped a plaintext server green. A name has no scope in an AST walk. This one has nothing to
// resolve — and it closes grpc.WithInsecure(), which a rule about insecure.NewCredentials never saw.
const (
	grpcPkg     = "google.golang.org/grpc"
	grpctlsPath = "internal/grpctls"
)

// forbidden are the constructors that decide a transport's security, and the reason each one is.
var forbidden = map[string]string{
	"NewServer": "grpctls.NewServer — a server built here carries no credentials and serves in plaintext",
	"NewClient": "grpctls.NewClient or grpctls.Dialer — a client built here presents no identity and verifies no peer",
}

// scanRoots are the trees holding production code. test/ is tooling run with go run, never shipped.
var scanRoots = []string{"../../cmd", "../../internal"}

// minScanned is the floor a broken path falls through. Without it, a root that stopped resolving would
// scan nothing and report a clean repository.
const minScanned = 200

func TestOnlyGRPCTLSBuildsAGRPCServerOrClient(t *testing.T) {
	t.Parallel()

	var offenders []string
	sawGRPC := false
	scanned := walkProduction(t, func(path string, file *ast.File, fset *token.FileSet) {
		imports := importNames(file)
		if !hasPath(imports, grpcPkg) {
			return
		}
		sawGRPC = true
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			why, forbid := forbidden[sel.Sel.Name]
			if !forbid {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && imports[id.Name] == grpcPkg {
				offenders = append(offenders, position(path, fset, call.Pos())+" → "+why)
			}
			return true
		})
	})

	if scanned < minScanned {
		t.Fatalf("scanned only %d production files, expected at least %d: the roots stopped resolving", scanned, minScanned)
	}
	// The import path is matched, not the identifier, so an alias cannot slip past — but a path that
	// stopped existing (a /v2 suffix, a rename) would match nothing and leave this guard green and blind.
	if !sawGRPC {
		t.Fatalf("no production file imports %s: the guard is watching a path that no longer exists", grpcPkg)
	}
	for _, o := range offenders {
		t.Errorf("%s builds a gRPC transport directly — use %s (step-300b)", strings.SplitN(o, " → ", 2)[0], strings.SplitN(o, " → ", 2)[1])
	}
}

func hasPath(imports map[string]string, path string) bool {
	for _, p := range imports {
		if p == path {
			return true
		}
	}
	return false
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
			// This package, and only it: a Dir match, not a Contains, so a helper dropped at
			// cmd/<svc>/internal/grpctls/ does not inherit the exemption.
			case filepath.ToSlash(filepath.Dir(path)) == "../.."+"/"+grpctlsPath:
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
