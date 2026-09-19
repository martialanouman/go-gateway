package deploy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
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
		"required-override", "known-env-name", "hpa-max-replicas", "pool-covers-lanes", "image-convention",
		"no-subpath",
	} {
		if !got[rule] {
			t.Errorf("rule %q reported nothing on testdata/broken, so nothing proves it can fail — "+
				"either the fixture stopped violating it or the rule stopped looking", rule)
		}
	}
}

// TestJobsAreHeldToTheContainerRules pins a hole the first version of this guard shipped with: inspect
// switched on Deployment, Service, Secret, ConfigMap and HPA, so a Job passed untouched. A literal
// CLICKHOUSE_PASSWORD, a misspelt variable and an arbitrary image all went green — in the very file
// step-270 named for the ClickHouse password that production refuses at its default.
//
// Jobs are held to the CONTAINER rules only. Probes and a grace period would be wrong for a process
// that runs to completion: it has no ops port and nothing to drain.
func TestJobsAreHeldToTheContainerRules(t *testing.T) {
	t.Parallel()

	var onJob []violation
	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if strings.Contains(v.msg, "migrate-postgres") || strings.Contains(v.msg, `container "migrate"`) {
			onJob = append(onJob, v)
		}
	}

	got := map[string]bool{}
	for _, v := range onJob {
		got[v.rule] = true
		if v.rule == "probe-endpoints" || v.rule == "grace-period" || v.rule == "probe-timeout" {
			t.Errorf("[%s] reported on a Job: %s — a Job exits, it has neither probes nor a drain", v.rule, v.msg)
		}
	}
	for _, rule := range []string{"secrets-by-reference", "known-env-name", "image-convention", "no-subpath"} {
		if !got[rule] {
			t.Errorf("rule %q reported nothing on the broken fixture's Job — Jobs are escaping it", rule)
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
		if !slices.Contains(services, name) {
			add("deployment-per-service", "Deployment %q matches no cmd/ service — remove it or fix its name", name)
		}
	}

	for _, m := range manifests {
		switch m.Kind {
		case "Deployment":
			out = append(out, inspectDeployment(m, env, configMaps)...)
		case "Job":
			// Container rules only. A Job exits, so it has no ops port to probe and nothing to drain —
			// asking it for probes or a grace period would be asking for the wrong thing.
			for _, c := range m.Spec.Template.Spec.Containers {
				out = append(out, inspectContainerEnv(m, c, env, configMaps)...)
				out = append(out, inspectVolumeMounts(m, c)...)
				out = append(out, inspectImage(m, c)...)
			}
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
			for name := range m.Data {
				// A ConfigMap is the wrong home for a credential: anything able to list the namespace
				// reads it, and unlike a Secret nothing marks it as sensitive.
				if isSecret(name) {
					add("secrets-by-reference", "%s: ConfigMap %q carries %s — it belongs in a Secret",
						m.Source, m.Metadata.Name, name)
				}
				// The same rule as a container's own env, applied where it bites hardest: envFrom hands
				// this map to every service that mounts it, so one typo leaves ALL of them on the
				// development default of a variable an operator believes they set.
				if _, known := env[name]; !known && !excepted(name) {
					add("known-env-name", "%s: ConfigMap %q carries %s, which no config section declares — "+
						"envFrom hands it to every service mounting this map, and a variable config does not read is silently ignored",
						m.Source, m.Metadata.Name, name)
				}
			}
		case "Secret":
			// 3. No Secret ships from this repo. step-300 (TLS) and step-310 (OIDC) provision them.
			if len(m.Data) > 0 || len(m.StringData) > 0 {
				add("secrets-by-reference", "%s: Secret %q carries data — manifests reference secrets, they never contain them", m.Source, m.Metadata.Name)
			}
		case "HorizontalPodAutoscaler":
			out = append(out, inspectHPA(m, env, deployments, configMaps)...)
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

		out = append(out, inspectImage(m, c)...)
		out = append(out, inspectContainerEnv(m, c, env, configMaps)...)
		out = append(out, inspectVolumeMounts(m, c)...)
		out = append(out, inspectRequiredOverrides(m, c, svc, env, configMaps)...)
	}

	// The grace period must clear the whole drain, in sequence: DRAIN_DELAY waiting for the load
	// balancer, then the supervisor's DRAIN_BUDGET, then DrainTracing flushing the span exporter —
	// which runs in a defer AFTER the supervisor returns and takes SHUTDOWN_TIMEOUT. Under that, the
	// kubelet SIGKILLs mid-drain and the pod dies exactly the way the drain exists to prevent.
	drain := durationEnv(m, env, configMaps, "DRAIN_DELAY")
	budget := durationEnv(m, env, configMaps, "DRAIN_BUDGET")
	shutdown := durationEnv(m, env, configMaps, "SHUTDOWN_TIMEOUT")
	need := drain + budget + shutdown
	grace := m.Spec.Template.Spec.TerminationGracePeriodSeconds
	switch {
	case grace == nil:
		add("grace-period", "%s: Deployment %q sets no terminationGracePeriodSeconds — the 30 s default is under the %s this service needs",
			m.Source, svc, need)
	case time.Duration(*grace)*time.Second < need:
		add("grace-period", "%s: Deployment %q terminationGracePeriodSeconds = %d, want at least %d (DRAIN_DELAY %s + DRAIN_BUDGET %s + SHUTDOWN_TIMEOUT %s)",
			m.Source, svc, *grace, int(need.Seconds()), drain, budget, shutdown)
	}

	return out
}

// inspectImage holds a container to the release registry, so a copy-pasted manifest cannot ship the
// neighbour's binary — or something off the internet.
func inspectImage(m deploy.Manifest, c deploy.Container) []violation {
	if strings.HasPrefix(c.Image, imagePrefix) {
		return nil
	}
	return []violation{{
		rule: "image-convention",
		msg: fmt.Sprintf("%s: container %q image %q does not start with %q",
			m.Source, c.Name, c.Image, imagePrefix),
	}}
}

// inspectVolumeMounts refuses a subPath mount, on every container the tree holds. The kubelet updates a
// Secret volume by swapping a symlink; a subPath mount resolves the path once and never follows it, so
// a rotated certificate stops reaching the process with nothing logged — weeks before the handshakes
// start expiring.
func inspectVolumeMounts(m deploy.Manifest, c deploy.Container) []violation {
	var out []violation
	for _, v := range c.VolumeMounts {
		// subPathExpr is the same mechanism with variable expansion.
		field, value := "subPath", v.SubPath
		if value == "" && v.SubPathExpr != "" {
			field, value = "subPathExpr", v.SubPathExpr
		}
		if value == "" {
			continue
		}
		out = append(out, violation{
			rule: "no-subpath",
			msg: fmt.Sprintf("%s: container %q mounts %q with %s %q — a subPath mount never receives a Secret update, "+
				"so a rotated certificate would silently stop reaching the process until the next restart",
				m.Source, c.Name, v.Name, field, value),
		})
	}
	return out
}

// inspectContainerEnv checks the variables a container sets: known to config, and secrets by
// reference only. It applies to every container the tree holds, Jobs included.
func inspectContainerEnv(m deploy.Manifest, c deploy.Container, env map[string]string, configMaps map[string]map[string]string) []violation {
	var out []violation
	add := func(rule, format string, args ...any) {
		out = append(out, violation{rule: rule, msg: fmt.Sprintf(format, args...)})
	}

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
	for name, v := range effectiveEnv(c, configMaps) {
		if v != "" && isSecret(name) {
			add("secrets-by-reference", "%s: container %q receives %s as a literal value — it must come from a secretKeyRef",
				m.Source, c.Name, name)
		}
	}
	return out
}

// inspectRequiredOverrides checks the variables whose shared default belongs to another service, plus
// the production switch. Deployments only: a Job takes them from the same ConfigMap.
func inspectRequiredOverrides(m deploy.Manifest, c deploy.Container, svc string, env map[string]string, configMaps map[string]map[string]string) []violation {
	var out []violation
	add := func(rule, format string, args ...any) {
		out = append(out, violation{rule: rule, msg: fmt.Sprintf(format, args...)})
	}

	literals := effectiveEnv(c, configMaps)
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
func inspectHPA(m deploy.Manifest, env map[string]string, deployments map[string]deploy.Manifest,
	configMaps map[string]map[string]string,
) []violation {
	var queue string
	for _, metric := range m.Spec.Metrics {
		// mt.routed is sized per connector by KAFKA_TOPIC_PARTITIONS_OVERRIDES, not by the shared
		// default, so this rule cannot speak for connector-pool-svc. A CPU-only HPA has no queue at
		// all and is not bounded by partitions either.
		if q := metric.External.Metric.Selector.MatchLabels["queue"]; q != "" && q != "mt.routed" {
			queue = q
			break
		}
	}
	if queue == "" {
		return nil
	}

	fail := func(format string, args ...any) []violation {
		return []violation{{rule: "hpa-max-replicas", msg: fmt.Sprintf(format, args...)}}
	}

	// The count must come from what is DEPLOYED, not from config's envDefault: the two agree today,
	// and a ConfigMap lowered to 6 while Go still says 12 would leave this rule clearing a ceiling
	// that collapses the fan-out — the exact failure it exists to catch (ADR-0014).
	target, ok := deployments[m.Spec.ScaleTargetRef.Name]
	if !ok {
		return fail("%s: HPA %q scales Deployment %q, which this tree does not define — its partition "+
			"count cannot be read, so nothing bounds maxReplicas %d",
			m.Source, m.Metadata.Name, m.Spec.ScaleTargetRef.Name, m.Spec.MaxReplicas)
	}
	raw := deployedEnv(target, env, configMaps, "KAFKA_TOPIC_PARTITIONS")
	partitions, err := strconv.Atoi(raw)
	if err != nil || partitions == 0 {
		return fail("%s: HPA %q scales %q, whose KAFKA_TOPIC_PARTITIONS reads %q — an unparseable "+
			"partition count leaves maxReplicas %d unbounded",
			m.Source, m.Metadata.Name, target.Metadata.Name, raw, m.Spec.MaxReplicas)
	}
	// The ceiling and the floor are independent defects of the same coupling, so they ACCUMULATE. An
	// early return here would hide the pool violation behind the replica one and hand an operator half
	// the story — then the other half on the next run, after they fixed the first.
	var out []violation
	if m.Spec.MaxReplicas >= partitions {
		out = append(out, fail("%s: HPA %q scales to %d pods on a %d-partition topic (%s) — at one pod per "+
			"partition each pod is down to a single lane and the fan-out is gone. Keep maxReplicas under the partition count",
			m.Source, m.Metadata.Name, m.Spec.MaxReplicas, partitions, queue)...)
	}
	return append(out, inspectPoolCoversLanes(m, target, env, configMaps, partitions, queue)...)
}

// inspectPoolCoversLanes holds the OTHER half of the maxReplicas coupling: the floor.
//
// maxReplicas is bounded above so a pod keeps more than one lane; minReplicas is what decides how many
// lanes a pod may be handed at once, and that is the number the Postgres pool has to cover. A consumer
// group hands each member ceil(partitions / members) partitions, so at the HPA's floor one pod owns
// ceil(partitions/minReplicas) of them — and router-svc turns each assigned partition into a
// CONCURRENT lane (handleBatch, internal/router/router.go), each lane processing sequentially and
// therefore holding at most one pgx connection at a time. MaxConns under that count makes Acquire the
// queue in front of the hot path: it waits, then returns a transient error, then the record is
// redelivered and repeats the same lookup on a pool that is still full.
//
// It is stated as ASSIGNMENT rather than as fan-out on purpose, and that is what lets it apply to every
// HPA the rule above already covers. How many partitions a member is handed is true of any consumer;
// whether it turns them into concurrent lanes is a property of the code. mo-dlr-router-svc consumes one
// record at a time and clears the bound with room to spare — the guard is conservative for it, not
// wrong about it. Naming router-svc's Deployment instead would rot the day another service adopts
// RunBatch, which is exactly what this rule replaces: a paragraph in step-280 that named a ratio
// (10/12) the manifests never produced.
func inspectPoolCoversLanes(m, target deploy.Manifest, env map[string]string,
	configMaps map[string]map[string]string, partitions int, queue string,
) []violation {
	fail := func(format string, args ...any) []violation {
		return []violation{{rule: "pool-covers-lanes", msg: fmt.Sprintf(format, args...)}}
	}

	// A missing minReplicas means one, which is what the HPA API defaults to — and it is the worst case
	// rather than an unknown: one pod owns every partition.
	minReplicas := 1
	if m.Spec.MinReplicas != nil {
		minReplicas = *m.Spec.MinReplicas
	}
	if minReplicas < 1 {
		return fail("%s: HPA %q declares minReplicas %d — a floor below one pod leaves the lanes per pod "+
			"unbounded, so nothing sizes the Postgres pool", m.Source, m.Metadata.Name, minReplicas)
	}
	lanes := lanesPerPod(partitions, minReplicas)

	// Read from what is DEPLOYED, never from config's envDefault: the two agree today at 10, and a
	// ConfigMap lowered while Go still says 10 would leave this rule clearing a pool that cannot cover
	// the lanes — the same coincidence TestHPACeilingUsesTheDeployedPartitionCount exists to deny.
	raw := deployedEnv(target, env, configMaps, "POSTGRES_MAX_CONNS")
	maxConns, err := strconv.Atoi(raw)
	if err != nil || maxConns == 0 {
		return fail("%s: HPA %q scales %q, whose POSTGRES_MAX_CONNS reads %q — an unparseable pool size "+
			"leaves the %d lanes a pod can own unbounded", m.Source, m.Metadata.Name, target.Metadata.Name, raw, lanes)
	}
	if maxConns < lanes {
		return fail("%s: HPA %q floors at %d pods on a %d-partition topic (%s), so one pod can be assigned "+
			"%d partitions against POSTGRES_MAX_CONNS %d. MaxConns bounds a CONCURRENCY, not a throughput: "+
			"a pod that owns more lanes than connections makes Acquire the queue in front of the hot path, "+
			"and its timeout turns into a redelivery that repeats the lookup on a pool still full. Raise "+
			"MaxConns to at least %d, or raise minReplicas",
			m.Source, m.Metadata.Name, minReplicas, partitions, queue, lanes, maxConns, lanes)
	}
	return nil
}

// deployedEnv is the first literal value the workload's containers give a variable, falling back to
// the default config would apply. It is what the pods actually receive, envFrom resolved — reading
// config's default instead would make this guard agree with itself instead of with the manifests.
func deployedEnv(m deploy.Manifest, env map[string]string, configMaps map[string]map[string]string, name string) string {
	for _, c := range m.Spec.Template.Spec.Containers {
		if v, ok := effectiveEnv(c, configMaps)[name]; ok && v != "" {
			return v
		}
	}
	return env[name]
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
//
// Across containers it takes the LONGEST, not the last one written: the kubelet grants one grace
// period to the whole pod and SIGKILLs everything in it at once, so the budget the grace must cover
// is the slowest container's.
func durationEnv(m deploy.Manifest, env map[string]string, configMaps map[string]map[string]string, name string) time.Duration {
	longest := parseDuration(env[name])
	for _, c := range m.Spec.Template.Spec.Containers {
		if v, ok := effectiveEnv(c, configMaps)[name]; ok && v != "" {
			longest = max(longest, parseDuration(v))
		}
	}
	return longest
}

// parseDuration reads a Go duration, yielding 0 for anything unparseable — a value config itself
// would reject at boot, so the guard leaves it to say so rather than reporting a second time.
func parseDuration(raw string) time.Duration {
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

func isSecret(name string) bool { return slices.Contains(secretVars, name) }

// TestConfigMapKeysAreHeldToTheKnownNameRule pins the same hole Jobs shipped with, on the surface
// that carries it furthest. inspect checked env NAMES on a container's own env: list only, so a key
// reaching the pods through envFrom was read for secrets and nothing else. A typo in the shared
// ConfigMap is therefore handed to all ten services at once and dropped by caarlos0/env in silence —
// every one of them left on the development default of a variable an operator believes they set.
func TestConfigMapKeysAreHeldToTheKnownNameRule(t *testing.T) {
	t.Parallel()

	var onConfigMap []violation
	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if strings.Contains(v.msg, "ConfigMap") {
			onConfigMap = append(onConfigMap, v)
		}
	}

	for _, v := range onConfigMap {
		if v.rule == "known-env-name" && strings.Contains(v.msg, "POSTGRE_MAX_CONNS") {
			return
		}
	}
	t.Errorf("no known-env-name violation reported for the broken ConfigMap's POSTGRE_MAX_CONNS — a "+
		"variable no config section declares reaches the pods through envFrom unchecked. Got: %v", onConfigMap)
}

// TestHPACeilingUsesTheDeployedPartitionCount pins a rule that was right by coincidence. inspectHPA
// read KAFKA_TOPIC_PARTITIONS from config's envDefault — never from what the manifests actually
// deploy — and the two happened to both say 12. Lower the ConfigMap to 6 without touching Go and the
// guard keeps clearing a maxReplicas of 8: six pods on one lane each, two with no partition at all.
// That is precisely the fan-out collapse ADR-0014 has this rule for, waved through by the rule itself.
func TestHPACeilingUsesTheDeployedPartitionCount(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if v.rule == "hpa-max-replicas" && strings.Contains(v.msg, `"rest-api-svc"`) {
			if !strings.Contains(v.msg, "6-partition") {
				t.Errorf("hpa-max-replicas reported %q — it must weigh the ceiling against the 6 partitions "+
					"the Deployment deploys, not config's default", v.msg)
			}
			return
		}
	}
	t.Error("no hpa-max-replicas violation for the rest-api-svc HPA: 8 replicas over 6 deployed " +
		"partitions leaves every pod on a single lane, and the rule read the Go default instead")
}

// lanesPerPod is the most partitions a single group member can be handed: the count divided by the
// members, ROUNDED UP.
//
// The rounding is the whole content of this function. A group of 12 partitions over 5 members splits
// 3+3+2+2+2, so a pod owns three — rounding down would size the pool for two and under-size it on the
// pods that got the extra one. No manifest in this tree divides unevenly, which is exactly why the
// arithmetic is pinned here instead of through a YAML fixture: replacing it with a plain division
// changes nothing the deployed tree can show.
func lanesPerPod(partitions, minReplicas int) int {
	return (partitions + minReplicas - 1) / minReplicas
}

func TestLanesPerPodRoundsUp(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		partitions, minReplicas, want int
	}{
		{12, 4, 3},  // the deployed router-svc: divides evenly
		{12, 1, 12}, // an HPA with no floor: one pod owns everything
		{12, 5, 3},  // 3+3+2+2+2 — the pod that got three is the one the pool must cover
		{6, 4, 2},   // 2+2+1+1
		{12, 12, 1}, // one lane each, the ceiling hpa-max-replicas keeps the tree under
		{12, 16, 1}, // more members than partitions: the extras idle, the busy ones own one
	} {
		if got := lanesPerPod(tc.partitions, tc.minReplicas); got != tc.want {
			t.Errorf("lanesPerPod(%d, %d) = %d, want %d: a member is handed the count rounded UP, and "+
				"rounding down sizes the pool for the pods that were handed fewer",
				tc.partitions, tc.minReplicas, got, tc.want)
		}
	}
}

// TestPoolCoversTheLanesAPodCanBeAssigned is the floor half of the maxReplicas coupling, and it asserts
// the MESSAGE rather than merely that the rule fired: the count it names must be the lanes the HPA's own
// floor produces (ceil(6/2) = 3), not the topic's partition count.
//
// That distinction is the whole reason this guard exists. step-280 carried "the lever is the ratio
// MaxConns / lanes per pod, today 10/12" — a ratio the manifests never produced, because 12 is the
// partition count and a pod at four replicas owns three. A rule that reported the partition count here
// would re-state the same error in code.
func TestPoolCoversTheLanesAPodCanBeAssigned(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if v.rule != "pool-covers-lanes" {
			continue
		}
		if !strings.Contains(v.msg, "assigned 3 partitions") {
			t.Errorf("pool-covers-lanes reported %q — it must weigh the pool against the 3 partitions the "+
				"HPA's floor of 2 pods hands one pod on a 6-partition topic, not against the topic's own "+
				"count", v.msg)
		}
		if !strings.Contains(v.msg, "POSTGRES_MAX_CONNS 2") {
			t.Errorf("pool-covers-lanes reported %q — it must read the pool size the manifests DEPLOY (2), "+
				"not config's default of 10", v.msg)
		}
		return
	}
	t.Error("no pool-covers-lanes violation for the rest-api-svc HPA: 2 connections for the 3 partitions " +
		"its floor assigns one pod is Acquire in front of the hot path, and the rule did not look")
}

// TestAnHPAWithoutAMinimumIsHeldToOnePod pins the branch nothing else reaches. Every HPA in deploy/k8s
// declares minReplicas, so the nil case is invisible to the deployed tree — and an unread default is one
// edit from becoming the partition count, which would clear the guard on exactly the manifest that most
// needs it: an HPA whose author forgot the floor, where one pod owns all twelve lanes.
func TestAnHPAWithoutAMinimumIsHeldToOnePod(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if v.rule != "pool-covers-lanes" || !strings.Contains(v.msg, `"not-a-service"`) {
			continue
		}
		if !strings.Contains(v.msg, "floors at 1 pods") || !strings.Contains(v.msg, "assigned 12 partitions") {
			t.Errorf("pool-covers-lanes reported %q — an absent minReplicas is one pod under the Kubernetes "+
				"API, so that pod owns all 12 partitions", v.msg)
		}
		return
	}
	t.Error("no pool-covers-lanes violation for the not-a-service HPA, which declares no minReplicas: one " +
		"pod would own every partition against a pool of 10")
}

// TestTheDeployedTreeCoversItsLanes is the same invariant on the REAL manifests, stated as arithmetic
// rather than as a passing suite: router-svc floors at 4 pods over 12 partitions, so a pod owns 3 lanes
// against MaxConns 10.
//
// TestManifestsHoldTheDeploymentInvariants already fails on a violation here. This says out loud what is
// being held, because the number it pins is the one step-280 got wrong, and a green suite does not tell
// a reader which ratio it agreed with.
func TestTheDeployedTreeCoversItsLanes(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, deploy.Dir) {
		if v.rule == "pool-covers-lanes" {
			t.Errorf("the deployed tree violates pool-covers-lanes: %s", v.msg)
		}
	}
}

// TestHPAWithNoResolvableTargetIsReported: an HPA whose scaleTargetRef names no Deployment in the tree
// cannot have its partition count read at all. Returning no violation there would swap a coincidence
// for a blind spot — the ceiling would go unchecked and the tree would still look clean.
func TestHPAWithNoResolvableTargetIsReported(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if v.rule == "hpa-max-replicas" && strings.Contains(v.msg, "orphan-hpa") {
			return
		}
	}
	t.Error("HPA \"orphan-hpa\" targets a Deployment that does not exist and nothing was reported — " +
		"an unreadable ceiling is a finding, not a pass")
}

// TestGracePeriodCoversTheSlowestContainer: durationEnv let each container overwrite the last, so a
// pod's drain budget was whichever container happened to be written last in the YAML. The grace
// period has to cover the container that takes LONGEST to go — the kubelet SIGKILLs the whole pod on
// one deadline, not one per container. Not a failure any manifest here exhibits (one container each);
// it is the invariant the function's name already promises and did not keep.
func TestGracePeriodCoversTheSlowestContainer(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if v.rule == "grace-period" && strings.Contains(v.msg, "not-a-service") {
			if !strings.Contains(v.msg, "DRAIN_BUDGET 5m0s") {
				t.Errorf("grace-period reported %q — it must weigh the grace against the slowest "+
					"container's 300s budget, not the last one written", v.msg)
			}
			return
		}
	}
	t.Error("no grace-period violation for not-a-service: 90 s cannot cover a container asking for a " +
		"300 s drain budget, and the rule read the last container instead of the slowest")
}

// TestLoadReadsYmlAsWellAsYaml: Load filtered on ".yaml", so a manifest saved as ".yml" was skipped
// in silence. Kubernetes applies both; a guard that reads one of them reports a clean tree while an
// unchecked Deployment ships beside it.
func TestLoadReadsYmlAsWellAsYaml(t *testing.T) {
	t.Parallel()

	for _, v := range inspect(t, filepath.Join("testdata", "broken")) {
		if strings.Contains(v.msg, "stray-yml-svc") {
			return
		}
	}
	t.Error("nothing reported for the Deployment in stray.yml — Load skipped the file for its " +
		"extension, so a .yml manifest escapes every rule")
}
