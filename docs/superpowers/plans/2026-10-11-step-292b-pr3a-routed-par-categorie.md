# step-292b PR3a — `mt.routed.<catégorie>` : plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mt.routed` devient trois topics par catégorie de trafic, lus au pool par trois consommateurs par connecteur.

**Architecture:** `pipeline.EncodeRouted` est le seul producteur de `mt.routed` : il choisit le topic sur la
catégorie du record, et le routeur, le drainer, le reroute, `mt-replay` et l'auto-réponse STOP suivent sans
autre changement. Au pool, un `FanIn` fait passer trois consommateurs pour un seul `Consumer` : `runOnce`
ne change pas.

**Tech Stack:** Go, franz-go (`internal/storage/kafka`), Redpanda (testcontainers), manifestes k8s + kustomize.

**Spec:** `tasks-todo/step-292b.md` (`## Design arrêté — PR3a`), ADR-0021 §1, §4.

## Global Constraints

- Topics : `mt.routed.otp`, `mt.routed.transactional`, `mt.routed.marketing` ; clé `message_id` inchangée.
- Catégorie vide → `marketing` ; catégorie inconnue → erreur d'encodage.
- Groupe du pool : `connector-pool-svc-<CONNECTOR_ID>-<catégorie>`, `FromLatest`.
- Largeurs : `KAFKA_TOPIC_PARTITIONS_OVERRIDES` = `mt.routed.otp=4,mt.routed.transactional=4` ; le marketing
  garde `KAFKA_TOPIC_PARTITIONS` (12).
- Aucune priorité entre catégories dans cette PR (PR4).
- Tests d'intégration : exporter `DOCKER_HOST` et `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` (mémoire
  testcontainers-orbstack-socket) ; jamais désactiver Ryuk.
- Chaque tâche : rouge lu → vert → **une mutation vue tomber** (cp avant, `cmp` après) → commit.
- Code sans commentaire évident ; une ligne de doc par symbole exporté (`revive`).

---

### Task 1 : trois topics, choisis par `EncodeRouted`

**Files:**
- Modify: `internal/storage/kafka/topics.go` (constantes + `MTRoutedTopics`)
- Modify: `internal/pipeline/wire.go:127-170` (choix du topic)
- Modify: `internal/storage/kafkaprovision/plan.go:31-45`, `internal/testutil/kafkatest/kafkatest.go:33-35`
- Modify: `cmd/connector-pool-svc/wiring.go:290` (temporaire : un consommateur, trois topics ; Task 2 le scinde)
- Test: `internal/pipeline/wire_test.go`
- Fix compile/assertions : `internal/router/router_test.go:172`, `internal/modlrrouter/stop_test.go:165,281`,
  `internal/connectorpool/{deadletter,drainer,reroute,rewrite,submit_internal,sim_harness}_test.go`,
  `internal/e2e/{e2e,reference}_test.go`, `internal/storage/kafka/kafka_integration_test.go`,
  `internal/storage/kafkaprovision/{plan,provision,provision_integration}_test.go`

**Interfaces:**
- Produces: `kafka.TopicMTRoutedOTP`, `kafka.TopicMTRoutedTransactional`, `kafka.TopicMTRoutedMarketing`
  (`string`), `kafka.MTRoutedTopics() []string` (ordre OTP, transactionnel, marketing).

- [ ] **Step 1 : test rouge** — dans `wire_test.go` :

```go
func TestEncodeRoutedPicksTheCategoryTopic(t *testing.T) {
	for cat, want := range map[cp.TrafficCategory]string{
		cp.TrafficOTP:           kafka.TopicMTRoutedOTP,
		cp.TrafficTransactional: kafka.TopicMTRoutedTransactional,
		cp.TrafficMarketing:     kafka.TopicMTRoutedMarketing,
		"":                      kafka.TopicMTRoutedMarketing,
	} {
		rec, err := pipeline.EncodeRouted(pipeline.RoutedMT{MessageID: uuid.New(), TrafficCategory: cat, Body: msg.NewBodyString("x")})
		if err != nil {
			t.Fatalf("%q: encode: %v", cat, err)
		}
		if rec.Topic != want {
			t.Errorf("category %q went to %q, want %q", cat, rec.Topic, want)
		}
	}
}

func TestEncodeRoutedRefusesAnUnknownCategory(t *testing.T) {
	_, err := pipeline.EncodeRouted(pipeline.RoutedMT{MessageID: uuid.New(), TrafficCategory: "promo", Body: msg.NewBodyString("x")})
	if err == nil {
		t.Fatal("an unknown category was encoded onto some topic")
	}
}
```

  Dans `TestRoutedRoundTrip`, remplacer `kafka.TopicMTRouted` par `kafka.TopicMTRoutedMarketing` (le
  record n'a pas de catégorie).

- [ ] **Step 2 :** `go test ./internal/pipeline/ -run 'EncodeRouted|RoutedRoundTrip'` → échec de compilation
  (`undefined: kafka.TopicMTRoutedOTP`). Lire le message.

- [ ] **Step 3 : implémentation.** `topics.go` : remplacer `TopicMTRouted` par

```go
	// TopicMTRoutedOTP, TopicMTRoutedTransactional and TopicMTRoutedMarketing carry routed messages, one
	// topic per traffic category (ADR-0021), so a marketing backlog never sits in front of an OTP.
	// Partition key = the logical message id, so every UDH segment of a message lands on the same
	// partition, in order (§7.3).
	TopicMTRoutedOTP           = "mt.routed.otp"
	TopicMTRoutedTransactional = "mt.routed.transactional"
	TopicMTRoutedMarketing     = "mt.routed.marketing"
```

  et, hors du bloc :

```go
// MTRoutedTopics lists the routed topics, most urgent category first.
func MTRoutedTopics() []string {
	return []string{TopicMTRoutedOTP, TopicMTRoutedTransactional, TopicMTRoutedMarketing}
}
```

  `wire.go` :

```go
var routedTopics = map[cp.TrafficCategory]string{
	cp.TrafficOTP:           kafka.TopicMTRoutedOTP,
	cp.TrafficTransactional: kafka.TopicMTRoutedTransactional,
	cp.TrafficMarketing:     kafka.TopicMTRoutedMarketing,
}
```

  En tête de `EncodeRouted` : `topic, ok := routedTopics[routedCategory(string(env.TrafficCategory))]` ;
  `!ok` → `fmt.Errorf("pipeline: mt.routed: unknown traffic category %q", env.TrafficCategory)` ; le record
  porte `Topic: topic`.

  `kafkaprovision.Topics()` et `kafkatest.topics` : `kafka.TopicMTRouted` → `kafka.MTRoutedTopics()...`
  (`append` ; garder `Topics()` triée si son contrat le dit).
  `wiring.go:290` : `kafka.NewConsumerFromLatest(cfg.Kafka, group, kafka.MTRoutedTopics()...)`.

- [ ] **Step 4 :** `go build ./... && go vet ./...`, puis corriger chaque test qui cite `TopicMTRouted` :
  - assertions de topic produit (router, stop, reroute, drainer, deadletter, rewrite) : le topic de la
    catégorie du record de la fixture (`TopicMTRoutedMarketing` si elle n'en pose pas) ;
  - consommateurs de harnais (`sim_harness`, `e2e`, `reference`) : `kafka.MTRoutedTopics()...` ;
    dans `reference_test.go`, la carte `consumers` est indexée par topic pour lire un lag : lire comment
    `:694` s'en sert et sommer le lag des trois topics sous une clé, sans perdre l'un d'eux ;
  - `kafka_integration_test.go` et `kafkaprovision/*_test.go` : `TopicMTRoutedMarketing` (un topic
    quelconque de l'enregistrement — vérifier que le test n'en attendait pas une largeur précise).
  `config_test.go` ne parse que des chaînes : inchangé.

- [ ] **Step 5 :** `go test ./internal/pipeline/ ./internal/router/ ./internal/modlrrouter/ ./internal/connectorpool/ ./internal/storage/kafkaprovision/` → vert.

- [ ] **Step 6 : mutation.** `cp internal/pipeline/wire.go $SCRATCH/wire.go.orig` ; remplacer
  `routedCategory(string(env.TrafficCategory))` par `cp.TrafficMarketing` → `TestEncodeRoutedPicksTheCategoryTopic`
  doit tomber ; restaurer, `cmp`. Seconde mutation : `ok` forcé à `true` avec `topic` vide → le test de
  refus doit tomber ; restaurer, `cmp`.

- [ ] **Step 7 : intégration** (Kafka réel) : `go test ./internal/storage/... ./internal/connectorpool/ ./internal/e2e/ -run . -count=1` avec les deux variables Docker → vert.

- [ ] **Step 8 : commit** `feat(step-292b): mt.routed par catégorie, choisi par EncodeRouted (PR3a)`.

---

### Task 2 : trois consommateurs par connecteur au pool

**Files:**
- Create: `internal/connectorpool/fanin.go`
- Test: `internal/connectorpool/fanin_test.go`
- Modify: `cmd/connector-pool-svc/wiring.go` (`stores.consumers`, `openStores`, `close`, readiness, `Deps.Consumer`)
- Modify: `cmd/connector-pool-svc/main.go:160-207` (`pollQueueDepth` sur les trois)
- Test: `cmd/connector-pool-svc/wiring_test.go` (groupes)

**Interfaces:**
- Consumes: `kafka.MTRoutedTopics()` (Task 1).
- Produces: `connectorpool.FanIn(consumers ...Consumer) Consumer` ; `routedGroup(connectorID uuid.UUID, topic string) string` dans `cmd/connector-pool-svc`.

- [ ] **Step 1 : tests rouges** — `fanin_test.go` :

```go
type oneRecordThenBlock struct{ rec kafka.Record }

func (c oneRecordThenBlock) RunBatch(ctx context.Context, h kafka.BatchHandler) error {
	h(ctx, []kafka.Record{c.rec})
	<-ctx.Done()
	return nil
}

type failing struct{ err error }

func (f failing) RunBatch(context.Context, kafka.BatchHandler) error { return f.err }

func TestFanInFeedsOneHandlerFromEveryConsumer(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]bool{}
	ctx, cancel := context.WithCancel(t.Context())
	fan := connectorpool.FanIn(oneRecordThenBlock{kafka.Record{Topic: "a"}}, oneRecordThenBlock{kafka.Record{Topic: "b"}})
	done := make(chan error, 1)
	go func() {
		done <- fan.RunBatch(ctx, func(_ context.Context, recs []kafka.Record) []error {
			mu.Lock()
			defer mu.Unlock()
			seen[recs[0].Topic] = true
			if len(seen) == 2 {
				cancel()
			}
			return make([]error, len(recs))
		})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("FanIn never returned; handler saw %v", seen)
	}
	if !seen["a"] || !seen["b"] {
		t.Errorf("handler saw %v, want both consumers' records", seen)
	}
}

func TestFanInStopsEveryConsumerOnTheFirstError(t *testing.T) {
	boom := errors.New("boom")
	fan := connectorpool.FanIn(blockingConsumer{}, failing{boom}, blockingConsumer{})
	done := make(chan error, 1)
	go func() { done <- fan.RunBatch(t.Context(), func(context.Context, []kafka.Record) []error { return nil }) }()
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Errorf("FanIn returned %v, want %v", err, boom)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a failed consumer left the others running: FanIn never returned")
	}
}
```

  (`blockingConsumer` existe dans `connectorpool_test.go` ; `fanin_test.go` est en `package connectorpool_test`
  si `connectorpool_test.go` l'est — vérifier et suivre.)

  `wiring_test.go` :

```go
func TestRoutedGroupIsPerConnectorAndCategory(t *testing.T) {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	if got := routedGroup(id, kafka.TopicMTRoutedOTP); got != "connector-pool-svc-"+id.String()+"-otp" {
		t.Errorf("group = %q", got)
	}
}
```

- [ ] **Step 2 :** `go test ./internal/connectorpool/ -run FanIn` et `go test ./cmd/connector-pool-svc/ -run RoutedGroup` → `undefined: FanIn` / `routedGroup`. Lire.

- [ ] **Step 3 : implémentation** — `fanin.go` :

```go
// FanIn runs every consumer with the same handler, so the pool reads one topic per traffic category
// (ADR-0021 §4) through a single Consumer. The first consumer to return stops the others.
func FanIn(consumers ...Consumer) Consumer { return fanIn(consumers) }

type fanIn []Consumer

func (f fanIn) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(f))
	for _, c := range f {
		go func() { errs <- c.RunBatch(ctx, handle) }()
	}
	first := <-errs
	cancel()
	for range len(f) - 1 {
		if err := <-errs; first == nil {
			first = err
		}
	}
	return first
}
```

  `wiring.go` :
  - `stores.consumer` → `consumers []*kafka.Consumer` ; `openStores` boucle sur `kafka.MTRoutedTopics()`,
    `NewConsumerFromLatest(cfg.Kafka, routedGroup(connectorID, topic), topic)` ; `close` les ferme tous ;
  - `routedGroup(id, topic) = serviceName + "-" + id.String() + "-" + strings.TrimPrefix(topic, "mt.routed.")` ;
  - `Deps.Consumer: connectorpool.FanIn(...)` sur les trois ;
  - readiness : une `ReadyCheck("kafka-"+catégorie, …)` par consommateur ;
  - `app.consumer` → `app.consumers`.
  Le commentaire *MIGRATION HAZARD* reste : il dit vrai du renommage de groupe, et ce renommage en est un.
  `main.go` : `pollQueueDepth` prend `[]*kafka.Consumer` et publie le lag de chacun (le label `queue` est
  déjà le topic).

- [ ] **Step 4 :** `go test ./internal/connectorpool/ ./cmd/connector-pool-svc/` → vert.

- [ ] **Step 5 : mutations.** (a) `fanin.go` : retirer le `cancel()` après `first := <-errs` → `TestFanInStopsEveryConsumerOnTheFirstError`
  doit tomber (délai). (b) ne lancer que `f[0]` → `TestFanInFeedsOneHandlerFromEveryConsumer` doit tomber.
  (c) **le câblage** (mémoire mutate-at-the-layer-the-bug-lives) : `openStores` n'ouvre que le premier topic →
  un test doit tomber ; si aucun ne tombe, ajouter à `wiring_test.go` un test qui construit les stores contre
  le Redpanda de test et compte les groupes, puis rejouer. Restaurer chaque fois, `cmp`.

- [ ] **Step 6 : intégration** : `go test ./cmd/connector-pool-svc/ ./internal/connectorpool/ ./internal/e2e/ -count=1` (Docker) → vert.

- [ ] **Step 7 : commit** `feat(step-292b): trois consommateurs par connecteur au pool (PR3a)`.

---

### Task 3 : déploiement et documents

**Files:**
- Modify: `deploy/k8s/configmap.yaml` (`KAFKA_TOPIC_PARTITIONS_OVERRIDES`)
- Modify: `deploy/k8s/connector-pool-svc.yaml` (HPA `matchExpressions`, commentaires `:7-9`, `:135-137`)
- Modify: `deploy/k8s/jobs/kafka-provision.yaml:5-6` (commentaire)
- Modify: `internal/deploy/manifests_test.go:452-458` (la clause `q != "mt.routed"` devient morte)
- Regenerate: `deploy/test/rendered/` via `deploy/test/check.sh`
- Docs: `docs/specification-technique-passerelle-sms.md:434`, `deploy/README.md:131-160`

- [ ] **Step 1 : rouge.** Ajouter à `manifests_test.go` (à côté des gardes existantes) un test qui lit le
  ConfigMap rendu et exige `mt.routed.otp=4` et `mt.routed.transactional=4` dans
  `KAFKA_TOPIC_PARTITIONS_OVERRIDES`, et que l'HPA `connector-pool-svc` sélectionne exactement
  `kafka.MTRoutedTopics()` (`matchExpressions`, opérateur `In`). Si le type `deploy.Manifest` ne parse pas
  `matchExpressions`, l'y ajouter. `go test ./internal/deploy/` → rouge, lire.

- [ ] **Step 2 : manifestes.**

```yaml
  KAFKA_TOPIC_PARTITIONS_OVERRIDES: "mt.routed.otp=4,mt.routed.transactional=4"
```

```yaml
        metric:
          name: queue_depth_records
          selector:
            matchExpressions:
              - key: queue
                operator: In
                values: [mt.routed.otp, mt.routed.transactional, mt.routed.marketing]
```

  Commentaire HPA : l'HPA additionne les séries retenues (doc Kubernetes). Retirer la clause morte
  `&& q != "mt.routed"` de la garde et réécrire son commentaire en une ligne : le pool sélectionne par
  `matchExpressions`, la règle de partitions ne le couvre pas.

- [ ] **Step 3 :** `go test ./internal/deploy/` → vert ; `deploy/test/check.sh` → vert, `git diff deploy/test/rendered` ne montre que ces changements.

- [ ] **Step 4 : mutation.** Retirer `mt.routed.transactional` de la liste `values` → le test tombe ; restaurer, `cmp`.

- [ ] **Step 5 : documents.** Spec §434 : les trois topics à la place de `mt.routed`, même phrase sur la
  clé. `deploy/README.md` : la jauge par topic, les trois groupes par connecteur, l'étape manuelle (supprimer
  l'ancien `mt.routed` et les groupes `connector-pool-svc-<id>` au déploiement de PR3b).

- [ ] **Step 6 :** `make lint` → vert. Commit `feat(step-292b): HPA et largeurs des topics mt.routed par catégorie (PR3a)`.

---

### Après les tâches (déroulé du dépôt)

- [ ] Revue en lecture seule (sub-agents `Plan`, axes disjoints : mécanisme et concurrence · tests et
  mutations · déploiement et docs · **code en trop**). Reboucler tant qu'un bloquant reste.
- [ ] Coupe.
- [ ] `make check` (Docker + `make smsc-sim`).
- [ ] Fiche : PR3a cochée dans le découpage, statut inchangé (PR3b, PR4 restent). Pas de `git mv`.
- [ ] PR `feat(step-292b): mt.routed par catégorie, trois consommateurs au pool (PR3a)`, corps avec l'étape
  manuelle de déploiement.
