// Package deploy reads the Kubernetes manifests under deploy/k8s so a Go test can hold them to the
// invariants this repository already enforces in code.
//
// It exists because the manifests are the one place where those invariants stop being checkable by
// the compiler: the ops port is "internal, never publicly exposed" (plan §1.4), /healthz is liveness
// and /readyz is readiness (plan §1.5), the pod's grace period must clear the drain budget — and a
// manifest that contradicts any of them fails in production, silently, on a rolling deploy.
//
// It is deliberately NOT a Kubernetes client: the projection below decodes only the fields the guard
// asserts on, with the YAML parser the repo already depends on. Pulling k8s.io/api into the module to
// read nine files would buy stronger typing at the price of a dependency tree larger than the service
// it deploys. kubeconform (make manifests) covers the schema; this covers what only this repo knows.
package deploy

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	yaml "go.yaml.in/yaml/v3"
)

// Dir is the manifest tree, relative to this package's directory.
const Dir = "../../deploy/k8s"

// Manifest is the slice of a Kubernetes object this guard reads. Anything not asserted on is absent
// on purpose — an unread field is one more thing to keep in step with no test to notice when it drifts.
type Manifest struct {
	APIVersion string            `yaml:"apiVersion"`
	Kind       string            `yaml:"kind"`
	Metadata   Metadata          `yaml:"metadata"`
	Spec       Spec              `yaml:"spec"`
	Data       map[string]string `yaml:"data"`
	StringData map[string]string `yaml:"stringData"`

	// Source is the file the object was decoded from, so a failure names something an operator can open.
	Source string `yaml:"-"`
}

// Metadata carries the identity fields the guard matches objects on.
type Metadata struct {
	Name        string            `yaml:"name"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

// Spec merges the fields of every kind the tree holds. A Service's spec.selector is a bare label map
// while a Deployment's is {matchLabels: …}; both decode into Selector, the first leaving MatchLabels
// nil, which is why the guard never reads a Service's selector.
type Spec struct {
	Replicas *int        `yaml:"replicas"`
	Selector Selector    `yaml:"selector"`
	Template PodTemplate `yaml:"template"`

	ClusterIP string        `yaml:"clusterIP"`
	Type      string        `yaml:"type"`
	Ports     []ServicePort `yaml:"ports"`

	MaxUnavailable any `yaml:"maxUnavailable"`
	MinAvailable   any `yaml:"minAvailable"`

	ScaleTargetRef ScaleTargetRef `yaml:"scaleTargetRef"`
	MinReplicas    *int           `yaml:"minReplicas"`
	MaxReplicas    int            `yaml:"maxReplicas"`
	Metrics        []HPAMetric    `yaml:"metrics"`
}

// Selector is a Deployment's or a PDB's label selector.
type Selector struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

// ServicePort is one entry of a Service's spec.ports.
type ServicePort struct {
	Name       string `yaml:"name"`
	Port       int    `yaml:"port"`
	TargetPort any    `yaml:"targetPort"`
	Protocol   string `yaml:"protocol"`
}

// ScaleTargetRef names the workload an HPA scales.
type ScaleTargetRef struct {
	Kind string `yaml:"kind"`
	Name string `yaml:"name"`
}

// HPAMetric is one entry of an HPA's spec.metrics; only the name is read, to tell a CPU target from
// the Kafka lag gauge.
type HPAMetric struct {
	Type     string `yaml:"type"`
	Resource struct {
		Name string `yaml:"name"`
	} `yaml:"resource"`
	External struct {
		Metric struct {
			Name     string            `yaml:"name"`
			Selector Selector          `yaml:"selector"`
			Labels   map[string]string `yaml:"matchLabels"`
		} `yaml:"metric"`
	} `yaml:"external"`
}

// PodTemplate is the pod template of a Deployment or a Job.
type PodTemplate struct {
	Metadata Metadata `yaml:"metadata"`
	Spec     PodSpec  `yaml:"spec"`
}

// PodSpec holds the pod-level fields the guard asserts on.
type PodSpec struct {
	TerminationGracePeriodSeconds *int        `yaml:"terminationGracePeriodSeconds"`
	RestartPolicy                 string      `yaml:"restartPolicy"`
	Containers                    []Container `yaml:"containers"`
}

// Container is one entry of a pod's containers.
type Container struct {
	Name           string          `yaml:"name"`
	Image          string          `yaml:"image"`
	Args           []string        `yaml:"args"`
	Ports          []ContainerPort `yaml:"ports"`
	Env            []EnvVar        `yaml:"env"`
	EnvFrom        []EnvFromSource `yaml:"envFrom"`
	LivenessProbe  *Probe          `yaml:"livenessProbe"`
	ReadinessProbe *Probe          `yaml:"readinessProbe"`
}

// ContainerPort is one entry of a container's ports.
type ContainerPort struct {
	Name          string `yaml:"name"`
	ContainerPort int    `yaml:"containerPort"`
	Protocol      string `yaml:"protocol"`
}

// EnvVar is one environment variable of a container. Value and ValueFrom are mutually exclusive, and
// which one carries a given variable is exactly what the secrets invariant checks.
type EnvVar struct {
	Name      string        `yaml:"name"`
	Value     string        `yaml:"value"`
	ValueFrom *EnvVarSource `yaml:"valueFrom"`
}

// EnvVarSource is the indirect source of an environment variable.
type EnvVarSource struct {
	SecretKeyRef    *KeyRef `yaml:"secretKeyRef"`
	ConfigMapKeyRef *KeyRef `yaml:"configMapKeyRef"`
	FieldRef        *struct {
		FieldPath string `yaml:"fieldPath"`
	} `yaml:"fieldRef"`
}

// KeyRef points at one key of a Secret or a ConfigMap.
type KeyRef struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

// EnvFromSource pulls every key of a ConfigMap or Secret into the environment.
type EnvFromSource struct {
	ConfigMapRef *KeyRef `yaml:"configMapRef"`
	SecretRef    *KeyRef `yaml:"secretRef"`
}

// Probe is a container probe. Only httpGet is modelled: every probe in this tree targets the ops
// server, and an exec or tcpSocket probe would be the thing the guard must reject, not decode.
type Probe struct {
	HTTPGet             *HTTPGet `yaml:"httpGet"`
	InitialDelaySeconds int      `yaml:"initialDelaySeconds"`
	PeriodSeconds       int      `yaml:"periodSeconds"`
	TimeoutSeconds      int      `yaml:"timeoutSeconds"`
	FailureThreshold    int      `yaml:"failureThreshold"`
	SuccessThreshold    int      `yaml:"successThreshold"`
}

// HTTPGet is an HTTP probe target. Port is any because Kubernetes accepts a number or a port name.
type HTTPGet struct {
	Path string `yaml:"path"`
	Port any    `yaml:"port"`
}

// Load decodes every object under dir, walking subdirectories and reading multi-document files. It
// returns an error rather than an empty slice when the tree is missing: a guard that quietly reads
// nothing is a guard that passes on nothing.
func Load(dir string) ([]Manifest, error) {
	var out []Manifest
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Both extensions: Kubernetes applies either, so a guard that reads one of them reports a
		// clean tree while an unchecked Deployment ships beside it.
		if d.IsDir() || !isManifest(path) {
			return nil
		}
		docs, err := decodeFile(path)
		if err != nil {
			return err
		}
		out = append(out, docs...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load manifests from %s: %w", dir, err)
	}
	return out, nil
}

// decodeFile reads one file, which may hold several objects separated by ---. yaml.Unmarshal reads
// only the first document of a stream, so a decoder loop is not a style choice here: with Unmarshal
// every object after the first Deployment would silently vanish from the guard's view.
func decodeFile(path string) ([]Manifest, error) {
	//nolint:gosec // G304: path comes from WalkDir over the caller's own manifest directory, never from input.
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var out []Manifest
	dec := yaml.NewDecoder(f)
	for {
		var m Manifest
		switch err := dec.Decode(&m); {
		case errors.Is(err, io.EOF):
			return out, nil
		case err != nil:
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if m.Kind == "" { // an empty document, e.g. a trailing ---
			continue
		}
		m.Source = filepath.ToSlash(path)
		out = append(out, m)
	}
}

// ProbePort returns the probe's port as an int, and whether it was expressed as one. A port name
// resolves through the container's ports, which this guard does not follow — it requires the number.
func (p Probe) ProbePort() (int, bool) {
	if p.HTTPGet == nil {
		return 0, false
	}
	n, ok := p.HTTPGet.Port.(int)
	return n, ok
}

// ServiceName is the service a manifest belongs to, taken from the app label the whole tree carries.
func (m Manifest) ServiceName() string {
	if v := m.Metadata.Labels["app"]; v != "" {
		return v
	}
	base := filepath.Base(m.Source)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

// isManifest reports whether the file is a YAML manifest, by either spelling of the extension.
func isManifest(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".yaml" || ext == ".yml"
}
