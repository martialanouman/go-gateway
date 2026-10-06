package supervisor_test

import (
	"encoding/json"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

const supervisorPath = "github.com/martialanouman/go-gateway/internal/platform/supervisor"

// TestEverySupervisedCommandRunsEveryComponentItBuilds: a component that newXxxApp builds but run() never
// hands to the supervisor is invisible — its own tests start it themselves, the service boots, /readyz is
// green, and a reload watcher silently stops reloading. A component is any field of a service's *App struct
// whose type has a Run(context.Context, …) method; it must appear in an argument of a supervisor Add.
//
// It type-checks the mains (the name of a field says nothing about what it is) against the export data `go
// list -export` leaves in the build cache, so it needs no dependency beyond the toolchain.
func TestEverySupervisedCommandRunsEveryComponentItBuilds(t *testing.T) {
	pkgs, exports := listCommands(t)

	var apps int
	var missing []string
	for _, p := range pkgs {
		if p.Name != "main" || !slices.Contains(p.Imports, supervisorPath) {
			continue
		}
		info, pkg := typeCheck(t, p, exports)
		for _, app := range appStructs(pkg) {
			apps++
			added := fieldsAddedToSupervisor(info, app)
			for f := range app.Underlying().(*types.Struct).Fields() {
				if isComponent(f.Type()) && !added[f] {
					missing = append(missing, filepath.Base(p.Dir)+": "+app.Obj().Name()+"."+f.Name())
				}
			}
		}
	}

	if apps < minSupervisedCommands {
		t.Fatalf("found %d service apps under cmd/, want at least %d — the scan is not reading the tree", apps, minSupervisedCommands)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("components built but never handed to the supervisor (g.Add): %s", strings.Join(missing, ", "))
	}
}

type listedPackage struct {
	Dir, ImportPath, Name, Export string
	GoFiles, Imports              []string
}

// listCommands lists every command and, through -deps, the export data file of everything they import.
func listCommands(t *testing.T) ([]listedPackage, map[string]string) {
	t.Helper()
	cmd := exec.Command("go", "list", "-export", "-deps", "-json", "../../../cmd/...")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	var pkgs []listedPackage
	for dec := json.NewDecoder(strings.NewReader(string(out))); ; {
		var p listedPackage
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode go list: %v", err)
		}
		pkgs = append(pkgs, p)
	}
	exports := map[string]string{}
	for _, p := range pkgs {
		exports[p.ImportPath] = p.Export
	}
	return pkgs, exports
}

func typeCheck(t *testing.T, p listedPackage, exports map[string]string) (*types.Info, *types.Package) {
	t.Helper()
	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(p.GoFiles))
	for _, name := range p.GoFiles {
		f, err := parser.ParseFile(fset, filepath.Join(p.Dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, f)
	}
	lookup := func(path string) (io.ReadCloser, error) { return os.Open(exports[path]) }
	conf := types.Config{Importer: importer.ForCompiler(fset, "gc", lookup)}
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}, Types: map[ast.Expr]types.TypeAndValue{}}
	pkg, err := conf.Check(p.ImportPath, fset, files, info)
	if err != nil {
		t.Fatalf("type-check %s: %v", p.ImportPath, err)
	}
	return info, pkg
}

func appStructs(pkg *types.Package) []*types.Named {
	var apps []*types.Named
	for _, name := range pkg.Scope().Names() {
		tn, ok := pkg.Scope().Lookup(name).(*types.TypeName)
		if !ok || !strings.HasSuffix(name, "App") {
			continue
		}
		if named, ok := tn.Type().(*types.Named); ok {
			if _, ok := named.Underlying().(*types.Struct); ok {
				apps = append(apps, named)
			}
		}
	}
	return apps
}

// isComponent reports whether a value of type t (or a pointer to it) has a Run(context.Context, …) method.
func isComponent(t types.Type) bool {
	if _, isPtr := t.(*types.Pointer); !isPtr {
		t = types.NewPointer(t)
	}
	sel := types.NewMethodSet(t).Lookup(nil, "Run")
	if sel == nil {
		return false
	}
	params := sel.Type().(*types.Signature).Params()
	return params.Len() > 0 && types.TypeString(params.At(0).Type(), nil) == "context.Context"
}

// fieldsAddedToSupervisor collects the app's fields referenced anywhere inside the arguments of a
// (*supervisor.Group).Add or (*supervisor.Ordered).Add call — closures included.
func fieldsAddedToSupervisor(info *types.Info, app *types.Named) map[*types.Var]bool {
	added := map[*types.Var]bool{}
	for expr, tv := range info.Types {
		call, ok := expr.(*ast.CallExpr)
		if !ok || tv.IsType() {
			continue
		}
		fun, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || fun.Sel.Name != "Add" {
			continue
		}
		s, ok := info.Selections[fun]
		if !ok || !isSupervisor(s.Recv()) {
			continue
		}
		for _, arg := range call.Args {
			ast.Inspect(arg, func(n ast.Node) bool {
				if fs, ok := n.(*ast.SelectorExpr); ok {
					if fsel, ok := info.Selections[fs]; ok && fsel.Kind() == types.FieldVal && sameNamed(fsel.Recv(), app) {
						added[fsel.Obj().(*types.Var)] = true
					}
				}
				return true
			})
		}
	}
	return added
}

func isSupervisor(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == supervisorPath &&
		(n.Obj().Name() == "Group" || n.Obj().Name() == "Ordered")
}

func sameNamed(t types.Type, app *types.Named) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	return types.Identical(t, app)
}
