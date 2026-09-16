package config_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"testing"
)

// minScannedGoFiles keeps the guard below from passing on nothing: a wrong root or a parser that stops
// reading would scan zero files and find zero suppressions. The module holds several hundred.
const minScannedGoFiles = 100

// suppressionDirective matches gosec's own suppression comment, in any case. The keyword is built by
// concatenation so this file does not trip its own guard.
var suppressionDirective = regexp.MustCompile(`(?i)#` + `nosec`)

// TestNoGosecNativeSuppression keeps one suppression syntax in the tree: //nolint:gosec // reason.
// nolintlint refuses that form without a reason; gosec's native form it cannot see, and golangci-lint
// ignores gosec's own nosec-require-justification setting (measured at v2.12.2, step-290a). Test files
// are scanned too: gosec is excluded there, so a native suppression in one is dead weight no linter reads.
func TestNoGosecNativeSuppression(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	scanned := 0
	err := filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "testdata", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		if ast.IsGenerated(f) {
			return nil
		}
		scanned++
		for _, group := range f.Comments {
			for _, c := range group.List {
				if suppressionDirective.MatchString(c.Text) {
					t.Errorf("%s: gosec's native suppression; write //nolint:gosec // <reason> so nolintlint checks it",
						fset.Position(c.Pos()))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the module: %v", err)
	}
	if scanned < minScannedGoFiles {
		t.Fatalf("scanned %d Go files, want at least %d — the walk is not reading the tree", scanned, minScannedGoFiles)
	}
}
