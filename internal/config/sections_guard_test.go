package config_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/config"
)

// The floors below are what keeps this guard from passing on nothing. A wrong relative path, a renamed
// directory or a parser that stops understanding the sources would otherwise leave every set empty and
// every comparison satisfied — a green test that guards air (the same trap declaredTopicConstants avoids
// in internal/storage/kafkaprovision). Both are far under today's counts (18 command packages, 49 reads),
// so they fire on a broken scan, not on ordinary churn.
const (
	minScannedCommands = 10
	minSectionReads    = 20
)

// TestEveryCommandDeclaresTheSectionsItReads is the guard step-193d exists for: config.Load validates
// only the sections a binary DECLARES, so a binary that reads cfg.Foo without declaring SectionFoo runs
// on values nobody checked. Nothing saw that asymmetry before — billing-svc had read cfg.Billing.Reaper*
// unvalidated since step-190, and smpp-server-svc had read cfg.GRPC.Port unvalidated since step-048,
// through three wiring steps that each read those files.
//
// It is deliberately one-way. The reverse (declaring a section nobody reads) costs an operator a
// pointless variable, not an unchecked value, and guarding it would need exemptions: config-sync
// declares SectionOTel and never names cfg.OTel, because it hands the whole cfg to
// observability.InitTracing. A guard that opens on a list of exceptions is a guard people stop reading.
func TestEveryCommandDeclaresTheSectionsItReads(t *testing.T) {
	t.Parallel()

	sections := sectionFields(t)
	commands := commandPackages(t)

	if len(commands) < minScannedCommands {
		t.Fatalf("scanned %d command packages under ../../cmd, want at least %d — the scan is not reading the tree",
			len(commands), minScannedCommands)
	}

	reads := 0
	for _, cmd := range commands {
		declared, read := sectionsOf(t, cmd.dir, sections)
		reads += len(read)

		// No section constant at all means config.Load(serviceName) — which validates SectionAll, so
		// every read is covered. Guarding it would be a false positive on the safest possible call.
		if len(declared) == 0 {
			continue
		}
		for _, section := range read {
			if !slices.Contains(declared, section) {
				t.Errorf("cmd/%s reads cfg.%s but never names config.Section%s: config.Load validates only "+
					"declared sections, so nothing checks what an operator puts in those variables",
					cmd.name, section, section)
			}
		}
	}

	if reads < minSectionReads {
		t.Fatalf("found %d cfg.<section> reads across cmd/, want at least %d — the parser is not reading the files",
			reads, minSectionReads)
	}
}

// TestSectionConstantsMatchTheConfigStructs keeps the convention the guard above derives its names from:
// one section constant per Config sub-struct, named Section<Field>. It is also the only thing holding
// SectionAll to its own godoc — "It must include every section, or Validate() would quietly stop being a
// full check" — which nothing verified until now.
func TestSectionConstantsMatchTheConfigStructs(t *testing.T) {
	t.Parallel()

	fields := sectionFields(t)
	constants := declaredSectionConstants(t)

	for _, field := range fields {
		if !slices.Contains(constants, "Section"+field) {
			t.Errorf("Config has sub-struct %s but no Section%s constant: the section guard derives its "+
				"names from this convention", field, field)
		}
	}
	for _, name := range constants {
		if field := strings.TrimPrefix(name, "Section"); !slices.Contains(fields, field) {
			t.Errorf("constant %s names no Config sub-struct: a section nothing carries cannot be validated", name)
		}
	}

	// SectionAll is the fallback for a caller that declares nothing, so a section missing from it makes
	// Validate() silently partial.
	for _, field := range fields {
		if config.SectionAll&sectionValue(t, "Section"+field) == 0 {
			t.Errorf("SectionAll omits Section%s: Validate() would quietly stop being a full check", field)
		}
	}
}

// sectionFields returns the names of Config's sub-structs — OTel, Postgres, … — which are exactly the
// configuration groups a binary can declare.
func sectionFields(t *testing.T) []string {
	t.Helper()

	rt := reflect.TypeOf(config.Config{})
	var fields []string
	for i := range rt.NumField() {
		f := rt.Field(i)
		// Same package means a section struct; time.Duration and the scalars are not.
		if f.Type.Kind() == reflect.Struct && f.Type.PkgPath() == rt.PkgPath() {
			fields = append(fields, f.Name)
		}
	}
	if len(fields) < 8 {
		t.Fatalf("reflected %d sub-structs on config.Config, want at least 8 — reflection is not seeing the type", len(fields))
	}
	return fields
}

type commandPackage struct {
	name string
	dir  string
}

// commandPackages lists the cmd/ directories holding Go sources.
func commandPackages(t *testing.T) []commandPackage {
	t.Helper()

	const root = "../../cmd"
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}

	var commands []commandPackage
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		sources, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatalf("glob %s: %v", dir, err)
		}
		if len(sources) > 0 {
			commands = append(commands, commandPackage{name: e.Name(), dir: dir})
		}
	}
	return commands
}

// sectionsOf reads one command's sources and reports the sections it names (config.SectionX, anywhere in
// the package — not only inside the config.Load call, so a service is free to hold its list in a
// variable) and the sections it reads (cfg.X, whether the whole sub-struct is passed along or one field
// is read off it).
//
// Test files are skipped on purpose: they build arbitrary configs, which says nothing about what the
// binary reads at boot.
func sectionsOf(t *testing.T, dir string, sections []string) (declared, read []string) {
	t.Helper()

	sources, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("glob %s: %v", dir, err)
	}

	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), source, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", source, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch ident.Name {
			case "config":
				// config.SectionAll is the declare-everything case, not a group of its own.
				if name := strings.TrimPrefix(sel.Sel.Name, "Section"); name != sel.Sel.Name &&
					slices.Contains(sections, name) && !slices.Contains(declared, name) {
					declared = append(declared, name)
				}
			case "cfg":
				if slices.Contains(sections, sel.Sel.Name) && !slices.Contains(read, sel.Sel.Name) {
					read = append(read, sel.Sel.Name)
				}
			}
			return true
		})
	}
	return declared, read
}

// declaredSectionConstants reads the Section constant names out of the package's own source, so the
// convention cannot be satisfied by making the same mistake twice.
func declaredSectionConstants(t *testing.T) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "config.go", nil, 0)
	if err != nil {
		t.Fatalf("parse config.go: %v", err)
	}

	var names []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 {
				continue
			}
			// SectionAll is the union, not a section.
			if name := value.Names[0].Name; strings.HasPrefix(name, "Section") && name != "SectionAll" {
				names = append(names, name)
			}
		}
	}
	if len(names) < 8 {
		t.Fatalf("parsed %d Section constants from config.go, want at least 8 — the parser is not reading the file", len(names))
	}
	return names
}

// sectionValue resolves a section constant by name. The constants are untyped bit flags with no runtime
// registry, so the mapping is spelled out here; TestSectionConstantsMatchTheConfigStructs is what keeps
// this list from drifting away from the package.
func sectionValue(t *testing.T, name string) config.Section {
	t.Helper()

	byName := map[string]config.Section{
		"SectionOTel":          config.SectionOTel,
		"SectionPostgres":      config.SectionPostgres,
		"SectionKafka":         config.SectionKafka,
		"SectionClickHouse":    config.SectionClickHouse,
		"SectionHTTP":          config.SectionHTTP,
		"SectionRedis":         config.SectionRedis,
		"SectionGRPC":          config.SectionGRPC,
		"SectionSMPP":          config.SectionSMPP,
		"SectionBilling":       config.SectionBilling,
		"SectionContentKey":    config.SectionContentKey,
		"SectionBillingReaper": config.SectionBillingReaper,
	}
	section, ok := byName[name]
	if !ok {
		t.Fatalf("no value known for %s: add it here when the section lands", name)
	}
	return section
}
