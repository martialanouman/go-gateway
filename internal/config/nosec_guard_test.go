package config_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// minScannedGoFiles keeps the guard below from passing on too little: a wrong root, or a skip rule that
// grew too wide, would scan part of the tree and find nothing there. The walk reads about 640 files and
// internal/ alone about 570 (cmd/ 52, test/ 22), so the floor fires when cmd/ drops out; test/ alone is
// under the margin, which a floor closer to the count would buy with failures on ordinary deletions.
const minScannedGoFiles = 620

// suppressionDirective matches both of gosec's native suppression comments (the tag and the directive, see
// findNoSecDirective in gosec's analyzer.go), in any case. The keywords are built by concatenation so this
// file does not trip its own guard.
var suppressionDirective = regexp.MustCompile(`(?i)#` + `nosec|//\s*gosec` + `:disable`)

// TestNoGosecNativeSuppression keeps one suppression syntax in the tree: //nolint:gosec // reason.
// nolintlint refuses that form without a reason; gosec's native forms it cannot see, and golangci-lint
// ignores gosec's own nosec-require-justification setting (measured at v2.12.2, step-290a). Test files
// are scanned too: gosec is excluded there, so a native suppression in one is dead weight no linter reads.
// The walk skips what the go tool skips (dot and underscore directories, testdata, vendor), so a stale
// worktree under .claude/ fails nothing ./... would not build.
func TestNoGosecNativeSuppression(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	scanned := 0
	err := filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != "../.." && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
				name == "testdata" || name == "vendor" || name == "node_modules") {
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
