package deploy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/deploy"
)

// The floors. A guard that reads an empty tree satisfies every assertion it makes, so each scan has a
// minimum below which it reports a broken scan instead of a clean bill of health. Ten services and
// thirty variables are what the repo has today; both fire on a wrong path, not on ordinary churn.
const (
	minServices = 10
	minEnvVars  = 30
)

// opsPort is the ops port every service serves /metrics, /healthz and /readyz on (plan §1.4). It is
// duplicated from config's envDefault rather than read from it, because the guard's whole job is to
// notice a manifest drifting from the contract — reading the same source as the thing under test
// would make a change to either invisible.
const opsPort = 9090

// readinessProbeTimeout is the server-side budget internal/observability bounds its readiness probes
// with (readinessTimeout). A kubelet probe that gives up at or before it never sees the verdict the
// server was about to produce, and reports a false 503.
const readinessProbeTimeout = 3 * time.Second

// imagePrefix is the registry path the release publishes to; step-270b builds the images themselves.
const imagePrefix = "ghcr.io/martialanouman/go-gateway/"

// secretVars must never carry a literal value in a manifest. Each one is a credential or a key: a
// value here is a secret in git history.
var secretVars = []string{
	"POSTGRES_URL", "REDIS_URL", "CLICKHOUSE_PASSWORD", "HTTP_ADMIN_TOKENS",
	"CONTENT_KMS_MASTER_KEY", "CONNECTOR_PASSWORD", "CONNECTOR_SYSTEM_ID",
}

// envExceptions are the variables read outside internal/config, so reflection over config.Config
// cannot know them. The list is short and each entry names its reader on purpose: a guard that opens
// on a growing list of exceptions is a guard people stop reading (see internal/config's own).
var envExceptions = []string{
	"CONNECTOR_",             // cmd/connector-pool-svc/main.go reads its own block
	"CONTENT_KMS_MASTER_KEY", // cmd/content-key-svc/main.go, via os.Getenv
}

// requiredOverrides are the variables whose shared default is WRONG for a given service. Each one is a
// port collision waiting to happen: HTTP_PORT defaults to 8081, so rest-api-svc without this override
// serves the public API on the admin port; GRPC_PORT defaults to 7000, which is session-manager's.
var requiredOverrides = map[string]map[string]string{
	"rest-api-svc":    {"HTTP_PORT": "8080"},
	"billing-svc":     {"GRPC_PORT": "7001"},
	"content-key-svc": {"GRPC_PORT": "7002"},
}

// usesSupervisor detects a service main, the same way internal/platform/supervisor's drain-hook guard
// does. The duplication is deliberate: two guards that share a helper fail together, and this one must
// keep working on the day someone breaks that one.
var usesSupervisor = regexp.MustCompile(`var \w+ supervisor\.(Group|Ordered)|supervisor\.(Group|Ordered)\{`)

type violation struct {
	rule string
	msg  string
}

// TestManifestsHoldTheDeploymentInvariants is the guard. Everything it checks is a property the code
// already enforces somewhere the compiler or a test can see it — and that a manifest can contradict
// with no symptom until a rolling deploy in production.
func TestManifestsHoldTheDeploymentInvariants(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, deploy.Dir) {
		t.Errorf("[%s] %s", v.rule, v.msg)
	}
}

// TestTheGuardCatchesWhatItClaimsTo runs the same inspection over a deliberately broken tree. Without
// it the assertions above are a green test nobody has ever seen fail — the failure mode this repo has
// hit often enough to write down: a fixture that does not exercise the condition it names.
func TestTheGuardCatchesWhatItClaimsTo(t *testing.T) {
	t.Parallel()

	got := map[string]bool{}
	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		got[v.rule] = true
	}

	for _, rule := range []string{
		"deployment-per-service", "probe-endpoints", "probe-timeout", "grace-period",
		"ops-port-not-exposed", "pdb-per-deployment", "secrets-by-reference",
		"required-override", "known-env-name", "hpa-max-replicas", "image-convention",
	} {
		if !got[rule] {
			t.Errorf("rule %q reported nothing on testdata/broken, so nothing proves it can fail — "+
				"either the fixture stopped violating it or the rule stopped looking", rule)
		}
	}
}

// inspect reads a manifest tree and returns everything wrong with it. It takes the directory as an
// argument so the guard can be pointed at a broken fixture and made to prove it still bites.
func inspect(t *testing.T, dir string) []violation {
	t.Helper()

	manifests, err := deploy.Load(dir)
	if err != nil {
		t.Fatalf("load %s: %v — the manifests must exist and parse before anything else can be said "+
			"about them", dir, err)
	}
	if len(manifests) == 0 {
		t.Fatalf("no manifest found under %s: the scan is not reading the tree", dir)
	}

	env := envSpecs(t)
	var out []violation
	add := func(rule, format string, args ...any) {
		out = append(out, violation{rule: rule, msg: fmt.Sprintf(format, args...)})
	}

	deployments := map[string]deploy.Manifest{}
	configMaps := map[string]map[string]string{}
	for _, m := range manifests {
		switch m.Kind {
		case "Deployment":
			deployments[m.Metadata.Name] = m
		case "ConfigMap":
			configMaps[m.Metadata.Name] = m.Data
		}
	}

	// 1. One Deployment per supervised service, and no Deployment without a binary. Both directions:
	// a service with no manifest never ships, and a manifest with no binary is a pod that crash-loops.
	services := supervisedServices(t)
	for _, svc := range services {
		if _, ok := deployments[svc]; !ok {
			add("deployment-per-service", "cmd/%s runs a supervisor but has no Deployment in %s — add %s.yaml", svc, dir, svc)
		}
	}
	for name := range deployments {
		if !contains(services, name) {
			add("deployment-per-service", "Deployment %q matches no cmd/ service — remove it or fix its name", name)
		}
	}

	for _, m := range manifests {
		switch m.Kind {
		case "Deployment":
			out = append(out, inspectDeployment(m, env, configMaps)...)
		case "Service":
			// 2. The ops port is internal and absent from the OpenAPI contracts (plan §1.4). A Service
			// that lists it is one ingress rule away from exposing /metrics publicly.
			for _, p := range m.Spec.Ports {
				if p.Port == opsPort || p.TargetPort == opsPort {
					add("ops-port-not-exposed", "%s: Service %q exposes the ops port %d — it serves /metrics, /healthz and /readyz and is internal only",
						m.Source, m.Metadata.Name, opsPort)
				}
			}
		case "ConfigMap":
			// A ConfigMap is the wrong home for a credential: anything able to list the namespace reads
			// it, and unlike a Secret nothing marks it as sensitive.
			for name := range m.Data {
				if isSecret(name) {
					add("secrets-by-reference", "%s: ConfigMap %q carries %s — it belongs in a Secret",
						m.Source, m.Metadata.Name, name)
				}
			}
		case "Secret":
			// 3. No Secret ships from this repo. step-300 (TLS) and step-310 (OIDC) provision them.
			if len(m.Data) > 0 || len(m.StringData) > 0 {
				add("secrets-by-reference", "%s: Secret %q carries data — manifests reference secrets, they never contain them", m.Source, m.Metadata.Name)
			}
		case "HorizontalPodAutoscaler":
			out = append(out, inspectHPA(m, env)...)
		}
	}

	// 4. Every Deployment is covered by a PDB. Without one, a node drain can take every pod of a
	// connector at once: zero binds to that SMSC, and every ESME of an smpp-server pod left with a
	// session token held for its full 60 s TTL (step-260).
	for name, d := range deployments {
		if !coveredByPDB(manifests, d) {
			add("pdb-per-deployment", "Deployment %q has no PodDisruptionBudget selecting it — an eviction can take every replica at once", name)
		}
	}

	return out
}

// inspectDeployment holds one workload to the probe, grace-period, secret and environment invariants.
func inspectDeployment(m deploy.Manifest, env map[string]string, configMaps map[string]map[string]string) []violation {
	var out []violation
	add := func(rule, format string, args ...any) {
		out = append(out, violation{rule: rule, msg: fmt.Sprintf(format, args...)})
	}
	svc := m.Metadata.Name

	for _, c := range m.Spec.Template.Spec.Containers {
		// Probes: /healthz is liveness and /readyz is readiness, both on the ops port (plan §1.5).
		// Swapping them turns a dependency outage into a crash loop, which is the exact failure the
		// split exists to prevent.
		checkProbe := func(p *deploy.Probe, kind, want string) {
			if p == nil {
				add("probe-endpoints", "%s: container %q has no %s — the kubelet cannot tell a live pod from a wedged one", m.Source, c.Name, kind)
				return
			}
			if p.HTTPGet == nil || p.HTTPGet.Path != want {
				add("probe-endpoints", "%s: container %q %s must GET %s (plan §1.5)", m.Source, c.Name, kind, want)
				return
			}
			if port, ok := p.ProbePort(); !ok || port != opsPort {
				add("probe-endpoints", "%s: container %q %s must target the ops port %d, got %v", m.Source, c.Name, kind, opsPort, p.HTTPGet.Port)
			}
		}
		checkProbe(c.LivenessProbe, "livenessProbe", "/healthz")
		checkProbe(c.ReadinessProbe, "readinessProbe", "/readyz")

		if p := c.ReadinessProbe; p != nil {
			if got := time.Duration(p.TimeoutSeconds) * time.Second; got <= readinessProbeTimeout {
				add("probe-timeout", "%s: container %q readinessProbe.timeoutSeconds = %d, want more than %s — "+
					"the ops server bounds its own probes at that, so the kubelet would give up before the verdict exists",
					m.Source, c.Name, p.TimeoutSeconds, readinessProbeTimeout)
			}
		}

		if !strings.HasPrefix(c.Image, imagePrefix) {
			add("image-convention", "%s: container %q image %q does not start with %q", m.Source, c.Name, c.Image, imagePrefix)
		}

		out = append(out, inspectEnv(m, c, svc, env, configMaps)...)
	}

	// The grace period must clear the whole drain: DRAIN_DELAY waiting for the load balancer, then the
	// supervisor's drain budget (SHUTDOWN_TIMEOUT), then DrainTracing flushing the exporter — which
	// runs in a defer AFTER the supervisor returns, and takes SHUTDOWN_TIMEOUT again. Under that, the
	// kubelet SIGKILLs mid-drain and the pod dies exactly the way the drain exists to prevent.
	drain := durationEnv(m, env, configMaps, "DRAIN_DELAY")
	shutdown := durationEnv(m, env, configMaps, "SHUTDOWN_TIMEOUT")
	need := drain + 2*shutdown
	grace := m.Spec.Template.Spec.TerminationGracePeriodSeconds
	switch {
	case grace == nil:
		add("grace-period", "%s: Deployment %q sets no terminationGracePeriodSeconds — the 30 s default is under the %s this service needs",
			m.Source, svc, need)
	case time.Duration(*grace)*time.Second < need:
		add("grace-period", "%s: Deployment %q terminationGracePeriodSeconds = %d, want at least %d (DRAIN_DELAY %s + 2 × SHUTDOWN_TIMEOUT %s)",
			m.Source, svc, *grace, int(need.Seconds()), drain, shutdown)
	}

	return out
}

// inspectEnv checks the variables a container sets: known to config, secrets by reference only, and
// the overrides whose shared default is wrong for this service.
func inspectEnv(m deploy.Manifest, c deploy.Container, svc string, env map[string]string, configMaps map[string]map[string]string) []violation {
	var out []violation
	add := func(rule, format string, args ...any) {
		out = append(out, violation{rule: rule, msg: fmt.Sprintf(format, args...)})
	}

	literals := effectiveEnv(c, configMaps)
	for _, e := range c.Env {
		// A name config does not know is read by nobody: caarlos0/env ignores it in silence, so a
		// POSTGRE_URL typo leaves the service on its localhost default with nothing to show for it.
		if _, known := env[e.Name]; !known && !excepted(e.Name) {
			add("known-env-name", "%s: container %q sets %s, which no config section declares — a variable config does not read is silently ignored",
				m.Source, c.Name, e.Name)
		}
		if e.Value != "" && isSecret(e.Name) {
			add("secrets-by-reference", "%s: container %q sets %s to a literal value — it must come from a secretKeyRef",
				m.Source, c.Name, e.Name)
		}
	}

	for name, want := range requiredOverrides[svc] {
		got, literal := literals[name]
		if !literal || got != want {
			add("required-override", "%s: %s must set %s=%s — the shared default (%s) belongs to another service",
				m.Source, svc, name, want, env[name])
		}
	}
	if got, literal := literals["ENVIRONMENT"]; !literal || got != "production" {
		add("required-override", "%s: %s must set ENVIRONMENT=production — that is what refuses loopback addresses, an insecure OTLP exporter and the default ClickHouse password",
			m.Source, svc)
	}

	return out
}

// inspectHPA holds an autoscaler to the ceiling step-201d measured: the router opens one goroutine per
// partition ASSIGNED TO ITS POD, so an HPA that reaches the partition count leaves every pod with a
// single lane. The fan-out disappears at the exact moment the load calls for it, and the HPA keeps
// growing without buying anything (ADR-0014).
func inspectHPA(m deploy.Manifest, env map[string]string) []violation {
	partitions, err := strconv.Atoi(env["KAFKA_TOPIC_PARTITIONS"])
	if err != nil || partitions == 0 {
		return nil
	}
	for _, metric := range m.Spec.Metrics {
		queue := metric.External.Metric.Selector.MatchLabels["queue"]
		// mt.routed is sized per connector by KAFKA_TOPIC_PARTITIONS_OVERRIDES, not by the shared
		// default, so this rule cannot speak for connector-pool-svc.
		if queue == "" || queue == "mt.routed" {
			continue
		}
		if m.Spec.MaxReplicas >= partitions {
			return []violation{{
				rule: "hpa-max-replicas",
				msg: fmt.Sprintf("%s: HPA %q scales to %d pods on a %d-partition topic (%s) — at one pod per "+
					"partition each pod is down to a single lane and the fan-out is gone. Keep maxReplicas under the partition count",
					m.Source, m.Metadata.Name, m.Spec.MaxReplicas, partitions, queue),
			}}
		}
	}
	return nil
}

// coveredByPDB reports whether some PodDisruptionBudget selects this Deployment's pods.
func coveredByPDB(manifests []deploy.Manifest, d deploy.Manifest) bool {
	labels := d.Spec.Template.Metadata.Labels
	for _, m := range manifests {
		if m.Kind != "PodDisruptionBudget" || len(m.Spec.Selector.MatchLabels) == 0 {
			continue
		}
		matched := true
		for k, v := range m.Spec.Selector.MatchLabels {
			if labels[k] != v {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// effectiveEnv is what the container actually sees, in Kubernetes' own precedence: envFrom first, then
// env, which wins. Only literal values appear — a variable sourced from a Secret has a value at
// runtime, but not one this tree can be read for, and treating an absent one as satisfying an override
// would let a typo pass.
//
// Resolving envFrom is not a refinement: without it a ConfigMap holding ENVIRONMENT would satisfy no
// rule and every service would look non-compliant, which is the fastest way to make a guard get
// deleted.
func effectiveEnv(c deploy.Container, configMaps map[string]map[string]string) map[string]string {
	out := map[string]string{}
	for _, from := range c.EnvFrom {
		if from.ConfigMapRef == nil {
			continue
		}
		for k, v := range configMaps[from.ConfigMapRef.Name] {
			out[k] = v
		}
	}
	for _, e := range c.Env {
		if e.ValueFrom != nil {
			delete(out, e.Name)
			continue
		}
		out[e.Name] = e.Value
	}
	return out
}

// durationEnv resolves a duration the manifest may override, falling back to the default config would
// apply. Reading the fallback from config rather than hardcoding it means changing a default in Go
// re-derives what the manifests must clear, instead of leaving this guard asserting a stale number.
func durationEnv(m deploy.Manifest, env map[string]string, configMaps map[string]map[string]string, name string) time.Duration {
	raw := env[name]
	for _, c := range m.Spec.Template.Spec.Containers {
		if v, ok := effectiveEnv(c, configMaps)[name]; ok && v != "" {
			raw = v
		}
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0
	}
	return d
}

// envSpecs reflects over config.Config to build every variable name the services actually read, with
// its default. Same approach as internal/config's own section guard: the struct tags are the source of
// truth, so a renamed variable moves this set instead of leaving it pinned to a stale copy.
func envSpecs(t *testing.T) map[string]string {
	t.Helper()

	out := map[string]string{}
	var walk func(reflect.Type, string)
	walk = func(rt reflect.Type, prefix string) {
		for i := range rt.NumField() {
			f := rt.Field(i)
			if p, ok := f.Tag.Lookup("envPrefix"); ok {
				walk(f.Type, prefix+p)
				continue
			}
			name, ok := f.Tag.Lookup("env")
			if !ok || name == "-" {
				continue
			}
			out[prefix+name] = f.Tag.Get("envDefault")
		}
	}
	walk(reflect.TypeOf(config.Config{}), "")

	if len(out) < minEnvVars {
		t.Fatalf("reflection found %d config variables, want at least %d: it is not reading config.Config",
			len(out), minEnvVars)
	}
	return out
}

// supervisedServices lists the deployable services: those whose main runs a supervisor. Tools and
// Jobs (migrate, kafka-provision, mt-replay) run to completion and are deliberately excluded — they
// have no ops port and nothing to keep alive.
func supervisedServices(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join("..", "..", "cmd"))
	if err != nil {
		t.Fatalf("read cmd/: %v", err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		src, err := os.ReadFile(filepath.Join("..", "..", "cmd", e.Name(), "main.go"))
		if err != nil || !usesSupervisor.Match(src) {
			continue
		}
		out = append(out, e.Name())
	}
	if len(out) < minServices {
		t.Fatalf("found %d supervised services in cmd/, want at least %d: the scan is not reading the tree",
			len(out), minServices)
	}
	sort.Strings(out)
	return out
}

func excepted(name string) bool {
	for _, e := range envExceptions {
		if name == e || (strings.HasSuffix(e, "_") && strings.HasPrefix(name, e)) {
			return true
		}
	}
	return false
}

func isSecret(name string) bool { return contains(secretVars, name) }

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
