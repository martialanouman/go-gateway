package deploy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"

	"github.com/martialanouman/go-gateway/internal/deploy"
)

// goreleaserPath is the release configuration, relative to this package's directory. A constant for
// the same reason deploy.Dir is one: the inspection below takes it as an argument so it can be
// pointed at a broken fixture instead.
const goreleaserPath = "../../.goreleaser.yaml"

// minImages is the floor for the real tree, counted in REFERENCES rather than distinct names. An
// inspection that reads no image satisfies every assertion it makes, so a wrong path reports a broken
// scan instead of a clean bill of health. deploy/k8s holds thirteen references to twelve images today.
// The broken fixture is deliberately smaller and is not held to it.
const minImages = 12

// wantTag is the tag every image entry must publish. {{ .Tag }} is v1.2.3, WITH the leading v;
// {{ .Version }} is 1.2.3 without it. deploy/k8s carries the :v0.0.0 placeholder and
// scripts/render-manifests.sh substitutes the published tag verbatim, so the v is load-bearing.
const wantTag = "{{ .Tag }}"

// distrolessPrefix is the only base these images may build on: no shell, no package manager, nothing
// to escalate from. content-key-svc holds the KEK and ADR-0011 requires it to carry the narrowest
// surface of the lot — the same base for all of them is what keeps that true without a second rule.
const distrolessPrefix = "gcr.io/distroless/"

// tagPlaceholder is what every image in deploy/k8s must carry. The manifests are a template: the
// version is substituted at deploy time by scripts/render-manifests.sh. A version pinned by hand in
// one file would not be substituted, and that service would silently stay behind while the other
// eleven move on.
const tagPlaceholder = "v0.0.0"

// goreleaserConfig is the slice of .goreleaser.yaml this guard reads. Anything not asserted on is
// absent on purpose — the same discipline the manifest projection follows.
type goreleaserConfig struct {
	Builds    []build    `yaml:"builds"`
	DockersV2 []dockerV2 `yaml:"dockers_v2"`
}

type build struct {
	ID     string `yaml:"id"`
	Binary string `yaml:"binary"`
}

// dockerV2 is one container image entry. dockers_v2 (GoReleaser v2.12+) builds the multi-arch
// manifest in a single buildx invocation, so there is no per-platform entry and no docker_manifests.
type dockerV2 struct {
	ID         string   `yaml:"id"`
	IDs        []string `yaml:"ids"`
	Images     []string `yaml:"images"`
	Tags       []string `yaml:"tags"`
	Dockerfile string   `yaml:"dockerfile"`
	ExtraFiles []string `yaml:"extra_files"`
}

// dockerfileOrDefault is the Dockerfile an entry builds with; GoReleaser defaults to "Dockerfile".
func (d dockerV2) dockerfileOrDefault() string {
	if d.Dockerfile == "" {
		return "Dockerfile"
	}
	return d.Dockerfile
}

// loadGoreleaser decodes the release configuration.
func loadGoreleaser(t *testing.T, path string) goreleaserConfig {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v — the release configuration must exist and parse before anything can be "+
			"said about the images it produces", path, err)
	}
	var cfg goreleaserConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return cfg
}

// deployedImage is one image reference found in the manifests, reduced to what the guard matches on.
type deployedImage struct {
	name   string // the binary the image is built from, e.g. "smpp-server-svc"
	tag    string
	source string
}

// deployedImages lists EVERY image reference the manifest tree pulls, occurrence by occurrence — it
// does not deduplicate. migrate is named twice (both migration Jobs), and the rules divide on exactly
// that: whether an image is built and published is a question about the NAME, asked once, while the
// tag it carries is a question about each REFERENCE. Deduplicating here answered the second question
// with the first occurrence and cleared whatever the others said.
//
// An image outside the release registry is already reported by the manifest guard's own
// image-convention rule, so it is skipped here rather than reported twice.
func deployedImages(t *testing.T, dir string) []deployedImage {
	t.Helper()

	manifests, err := deploy.Load(dir)
	if err != nil {
		t.Fatalf("load %s: %v", dir, err)
	}

	var out []deployedImage
	for _, m := range manifests {
		for _, c := range m.Spec.Template.Spec.Containers {
			if !strings.HasPrefix(c.Image, imagePrefix) {
				continue
			}
			name, tag, _ := strings.Cut(strings.TrimPrefix(c.Image, imagePrefix), ":")
			out = append(out, deployedImage{name: name, tag: tag, source: m.Source})
		}
	}
	return out
}

// dockerfile is the slice of a Dockerfile the guard reads: its base image and its user.
type dockerfile struct {
	path string
	from string
	user string
}

// readDockerfile pulls out the last FROM and the last USER. Last, not first: a multi-stage build ends
// on the stage that ships, and a later USER overrides an earlier one.
func readDockerfile(root, name string) (dockerfile, error) {
	path := filepath.Join(root, name)
	raw, err := os.ReadFile(path)
	if err != nil {
		return dockerfile{path: path}, err
	}
	d := dockerfile{path: path}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "FROM":
			d.from = fields[1]
		case "USER":
			d.user = fields[1]
		}
	}
	return d, nil
}

// isNonRootNumericUser reports whether a USER directive names a numeric, non-zero uid. Numeric is not
// a style preference: the kubelet refuses to start a pod with runAsNonRoot whose image carries a user
// it cannot resolve to a number.
func isNonRootNumericUser(user string) bool {
	uid, _, _ := strings.Cut(user, ":")
	if uid == "" {
		return false
	}
	for _, r := range uid {
		if r < '0' || r > '9' {
			return false
		}
	}
	return strings.Trim(uid, "0") != ""
}

// inspectImages reads the manifest tree, the release configuration and the Dockerfiles together, and
// returns everything that stops a deployed image from existing or from being safe to run. It takes its
// paths as arguments for the same reason inspect does: so the guard can be pointed at a broken fixture
// and made to prove it still bites.
func inspectImages(t *testing.T, manifestDir, releasePath string) []violation {
	t.Helper()

	images := deployedImages(t, manifestDir)
	cfg := loadGoreleaser(t, releasePath)
	root := filepath.Dir(releasePath)

	var out []violation
	add := func(rule, format string, args ...any) {
		out = append(out, violation{rule: rule, msg: fmt.Sprintf(format, args...)})
	}

	binaries := map[string]bool{}
	for _, b := range cfg.Builds {
		binaries[b.Binary] = true
	}
	entries := map[string]dockerV2{}
	for _, d := range cfg.DockersV2 {
		for _, img := range d.Images {
			entries[strings.TrimPrefix(img, imagePrefix)] = d
		}
	}

	// Rules that speak once per image NAME, not once per reference: migrate is pulled by two Jobs and
	// is one image, so reporting it twice would be noise.
	reported := map[string]bool{}
	for _, img := range images {
		once := !reported[img.name]
		reported[img.name] = true

		// A manifest can only pull what the release builds. Without this a service is deployable,
		// supervised, counted among the ten — and published nowhere, with nothing to say so until a
		// rollout pulls an image that was never pushed.
		if once && !binaries[img.name] {
			add("image-has-build", "%s: pulls %s%s, and no build in %s produces a binary named %q — "+
				"the image can never exist",
				img.source, imagePrefix, img.name, filepath.Base(releasePath), img.name)
		}

		entry, ok := entries[img.name]
		switch {
		case !once:
			// Already answered for this name.
		case !ok:
			add("image-has-docker-entry", "%s: pulls %s%s, and no dockers_v2 entry in %s publishes it — "+
				"the binary ships as an archive and the pod stays in ImagePullBackOff",
				img.source, imagePrefix, img.name, filepath.Base(releasePath))
		case !slices.Equal(entry.Tags, []string{wantTag}):
			add("image-has-docker-entry", "%s: the dockers_v2 entry for %s publishes tags %v, want [%q] — "+
				"deploy/k8s carries a :v0.0.0 placeholder that the rendered tag replaces verbatim, so the "+
				"leading v has to be there",
				filepath.Base(releasePath), img.name, entry.Tags, wantTag)
		}

		// The manifests are a template. A version pinned by hand in one file is not substituted at
		// deploy time, and that service stays behind while the other eleven move on — silently.
		if img.tag != tagPlaceholder {
			add("image-tag-placeholder", "%s: pulls %s%s:%s, want the :%s placeholder — the version is "+
				"substituted at deploy time by scripts/render-manifests.sh, and a hand-pinned tag is not",
				img.source, imagePrefix, img.name, img.tag, tagPlaceholder)
		}
	}

	// The reverse direction. An image nobody deploys is attack surface published for free, and it goes
	// stale with nothing to report it — the same reasoning that keeps fake-smsc out of builds.
	deployed := map[string]bool{}
	for _, img := range images {
		deployed[img.name] = true
	}
	for _, d := range cfg.DockersV2 {
		for _, img := range d.Images {
			name := strings.TrimPrefix(img, imagePrefix)
			if !deployed[name] {
				add("docker-entry-is-deployed", "%s: publishes %s, which no manifest under %s deploys — "+
					"remove the entry, or add the workload that needs it",
					filepath.Base(releasePath), img, manifestDir)
			}
		}
	}

	out = append(out, inspectDockerfiles(cfg, root, releasePath)...)
	return out
}

// inspectDockerfiles holds every Dockerfile the release references to the non-root contract, and the
// migrate image to the files it cannot run without.
func inspectDockerfiles(cfg goreleaserConfig, root, releasePath string) []violation {
	var out []violation
	add := func(rule, format string, args ...any) {
		out = append(out, violation{rule: rule, msg: fmt.Sprintf(format, args...)})
	}

	bases := map[string][]string{}
	for _, d := range cfg.DockersV2 {
		name := d.dockerfileOrDefault()
		df, err := readDockerfile(root, name)
		if err != nil {
			add("image-nonroot", "%s: dockers_v2 entry %q builds with %s, which does not exist",
				filepath.Base(releasePath), d.ID, name)
			continue
		}
		bases[df.from] = append(bases[df.from], name)

		// deploy/k8s carries no securityContext, so the image is the ONLY place non-root is decided.
		if !isNonRootNumericUser(df.user) {
			add("image-nonroot", "%s: USER is %q, want a numeric non-zero uid — deploy/k8s sets no "+
				"securityContext, so nothing else stops this container running as root, and the kubelet "+
				"cannot resolve a non-numeric user under runAsNonRoot",
				df.path, df.user)
		}
		if !strings.HasPrefix(df.from, distrolessPrefix) {
			add("image-nonroot", "%s: FROM %q, want a %s base — a shell in the image is a shell an "+
				"attacker can escalate from, and content-key-svc holds the KEK (ADR-0011)",
				df.path, df.from, distrolessPrefix)
		}

		// cmd/migrate resolves "migrations" and "migrations/clickhouse" RELATIVE TO ITS CWD. Both Jobs
		// invoke it, and a static binary without those files fails on first start. It takes the two
		// halves — GoReleaser must stage the directory into the build context, and the Dockerfile must
		// copy it — so both are checked: either one alone still ships a broken image.
		if slices.Contains(d.IDs, "migrate") || d.ID == "migrate" {
			if !slices.Contains(d.ExtraFiles, "migrations") {
				add("migrations-in-migrate-image", "%s: the migrate entry has extra_files %v, missing "+
					"\"migrations\" — the directory never reaches the build context and both migration Jobs "+
					"fail on first start", filepath.Base(releasePath), d.ExtraFiles)
			}
			if raw, err := os.ReadFile(filepath.Join(root, name)); err == nil &&
				!strings.Contains(string(raw), "COPY migrations") {
				add("migrations-in-migrate-image", "%s: no COPY of migrations — cmd/migrate reads them "+
					"from disk at a path relative to its CWD", filepath.Join(root, name))
			}
		}
	}

	// One base, decided once. Two Dockerfiles drifting onto different bases is a base nobody chose.
	if len(bases) > 1 {
		// Sorted: a map in a failure message reorders itself between runs, and a message that changes
		// shape is a message people stop trusting.
		distinct := make([]string, 0, len(bases))
		for from, files := range bases {
			slices.Sort(files)
			distinct = append(distinct, fmt.Sprintf("%s (%s)", from, strings.Join(files, ", ")))
		}
		slices.Sort(distinct)
		add("image-nonroot", "the Dockerfiles do not share one base: %s — a base that drifts between "+
			"them is a base nobody decided", strings.Join(distinct, " vs "))
	}
	return out
}

// TestEveryDeployedImageIsBuiltAndPublished holds the release configuration to what deploy/k8s pulls.
// step-270 wrote the manifests against images that nothing built; this keeps the two in step from here
// on, in both directions.
func TestEveryDeployedImageIsBuiltAndPublished(t *testing.T) {
	t.Parallel()

	if got := deployedImages(t, deploy.Dir); len(got) < minImages {
		t.Fatalf("found %d images under %s, want at least %d — the scan is not reading the tree",
			len(got), deploy.Dir, minImages)
	}
	for _, v := range inspectImages(t, deploy.Dir, goreleaserPath) {
		t.Errorf("[%s] %s", v.rule, v.msg)
	}
}

// TestTheImageGuardCatchesWhatItClaimsTo runs the same inspection over a deliberately broken tree.
// Without it the assertions above are a green test nobody has ever seen fail — the failure mode this
// repository has hit often enough to write down.
func TestTheImageGuardCatchesWhatItClaimsTo(t *testing.T) {
	t.Parallel()

	got := map[string]bool{}
	for _, v := range inspectImages(t, filepath.Join("testdata", "images", "k8s"),
		filepath.Join("testdata", "images", "goreleaser.yaml")) {
		got[v.rule] = true
	}

	for _, rule := range []string{
		"image-has-build", "image-has-docker-entry", "docker-entry-is-deployed",
		"image-nonroot", "migrations-in-migrate-image", "image-tag-placeholder",
	} {
		if !got[rule] {
			t.Errorf("rule %q reported nothing on testdata/images, so nothing proves it can fail — "+
				"either the fixture stopped violating it or the rule stopped looking", rule)
		}
	}
}

// TestEveryOccurrenceOfAnImageIsChecked pins a hole the first version of this guard shipped with.
// deployedImages deduplicated by image name — right for the rules that speak once per image, wrong for
// the ones that speak per occurrence. migrate is the only name deploy/k8s references twice (both
// migration Jobs), so a version pinned by hand in the SECOND of them was cleared by the first, and the
// one rule that exists to catch exactly that never fired.
func TestEveryOccurrenceOfAnImageIsChecked(t *testing.T) {
	t.Parallel()

	for _, v := range inspectImages(t, filepath.Join("testdata", "images", "k8s"),
		filepath.Join("testdata", "images", "goreleaser.yaml")) {
		if v.rule == "image-tag-placeholder" && strings.Contains(v.msg, "migrate-postgres") {
			return
		}
	}
	t.Error("the hand-pinned tag in the second migrate Job went unreported — deployedImages keeps only " +
		"the first occurrence of an image name, so the rule never sees the one that is wrong")
}
