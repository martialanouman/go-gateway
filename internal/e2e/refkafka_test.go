package e2e_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
)

// refDialTimeout is the ONE Kafka setting the reference run deliberately departs from production on.
// It bounds the dial, not a fetch, and the run brings its broker up in a container beside nine services
// and an injector on one host — 3s is a production figure for a warm broker on its own node. Every other
// field must be the production default, which is what TestRefKafkaCarriesProductionDefaults enforces.
const refDialTimeout = 5 * time.Second

// refKafkaConfig is the Kafka configuration the reference run drives its clients with.
//
// It is DERIVED from the production defaults rather than written as a literal, and that is the whole
// point of the function. A struct literal here silently skips every envDefault in internal/config: the
// fetch fields land at zero, consumerOpts() applies an option only when a field is > 0, and franz-go
// falls back to its own defaults. The run then measures a client no pod will ever run — most sharply
// FetchMaxPartitionBytes, ADR-0012's duplication bound, which franz-go defaults to 1MiB against the
// 56KiB the ADR commits to: an ~18x larger poll batch, and every conclusion about batching or fan-out
// drawn on top of it (step-201d, D3).
//
// It is the same shape of defect the ClickHouse pool had here (chtest leaves the pool at zero, which the
// driver reads as unset), which is why the pin below exists rather than a comment.
func refKafkaConfig(brokers []string) config.Kafka {
	cfg := config.Defaults().Kafka
	cfg.Brokers = brokers
	cfg.Timeout = refDialTimeout
	return cfg
}

// TestRefKafkaCarriesProductionDefaults pins the reference run's Kafka client on the configuration a pod
// boots with. It is the guard the ClickHouse pool never had: that one was found by reading the driver's
// source after two milestones of runs, and the same class of gap then repeated on Kafka.
//
// It runs in the ORDINARY suite, not behind the loadref tag. The defect it catches is a two-word struct
// literal that compiles, passes every test, and quietly changes what the measurement means — so the guard
// has to be somewhere a developer sees it without starting a broker.
//
// The one legitimate departure is named in refDialTimeout and asserted as such. Everything else must
// match, and a NEW field added to config.Kafka fails here until the run is told what to do with it: the
// reflection walk is what makes this a pin rather than a snapshot of the fields someone thought of.
func TestRefKafkaCarriesProductionDefaults(t *testing.T) {
	brokers := []string{"broker-a:9092", "broker-b:9092"}
	got := refKafkaConfig(brokers)
	want := config.Defaults().Kafka

	// The two fields the run owns: the brokers testcontainers hands it, and the dial timeout above.
	want.Brokers = brokers
	want.Timeout = refDialTimeout

	if diff := diffStructFields(got, want); len(diff) > 0 {
		for _, d := range diff {
			t.Errorf("reference run's Kafka config drifted from production: %s", d)
		}
		t.Log("build it from config.Defaults().Kafka rather than a struct literal — a literal leaves every " +
			"unset field at zero, and franz-go then applies its own default instead (step-201d, D3)")
	}
}

// diffStructFields reports every exported field where got and want disagree, named. It walks by
// reflection on purpose: a hand-written list of comparisons silently ignores the next field somebody adds
// to config.Kafka, which is the very way the fetch levers went unnoticed here in the first place.
func diffStructFields[T any](got, want T) []string {
	gv, wv := reflect.ValueOf(got), reflect.ValueOf(want)
	typ := gv.Type()
	var out []string
	for i := range typ.NumField() {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		if g, w := gv.Field(i).Interface(), wv.Field(i).Interface(); !reflect.DeepEqual(g, w) {
			out = append(out, fmt.Sprintf("%s = %v, want %v", f.Name, g, w))
		}
	}
	return out
}
