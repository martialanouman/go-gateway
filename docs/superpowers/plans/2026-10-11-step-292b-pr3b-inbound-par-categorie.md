# step-292b PR3b — `mt.inbound.<catégorie>` : plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mt.inbound` devient trois topics par catégorie ; l'ingestion choisit le topic par lecture de la
catégorie du sender ID, le routeur lit les trois par trois consommateurs.

**Architecture:** `pipeline.EncodeInbound` reste le seul producteur et choisit le topic sur un champ
`InboundMT.TrafficCategory` posé par `ingest.Ingestor` (lecture d'un `senderid.Holder`, inconnu → marketing).
`FanIn` passe dans `internal/storage/kafka`, partagé par le routeur et le pool.

**Tech Stack:** Go, franz-go, Redpanda (testcontainers), manifestes k8s + kustomize, PromQL (doc).

**Spec:** `tasks-todo/step-292b.md` (`## Design arrêté — PR3b`), ADR-0021 §1, §2, §4.

## Global Constraints

- Topics : `mt.inbound.otp`, `mt.inbound.transactional`, `mt.inbound.marketing` ; clé `message_id` inchangée.
- Catégorie absente, inconnue ou sender ID non trouvé → `marketing`. L'ingestion **n'autorise rien** : un
  sender inconnu est produit sur `mt.inbound.marketing`, le routeur le rejette (ADR-0021 §2).
- Groupes du routeur : `router-svc-<catégorie>` ; `NewConsumer` (AtStart), comme aujourd'hui.
- Projecteur accepted : un consommateur, groupe `router-svc-accepted-cdr`, trois topics, **AtStart**.
- Largeurs : override `mt.inbound.otp=4,mt.inbound.transactional=4` ajouté à celui de PR3a ; marketing à 12.
- Aucune clé indexée par `(partition, offset)` sans le topic.
- Tests d'intégration : `DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` à chaque commande, `-v`, grep `--- SKIP` ; jamais `TESTCONTAINERS_RYUK_DISABLED`.
- Chaque tâche : rouge lu → vert → mutation vue tomber (cp + cmp, jamais git checkout) → commit.
- Code sans commentaire évident ; une ligne de doc par symbole exporté.

---

### Task 1 : `FanIn` passe dans `internal/storage/kafka`

**Files:** Move `internal/connectorpool/fanin.go` → `internal/storage/kafka/fanin.go`, `fanin_test.go` idem
(adapter les fakes : le paquet kafka n'a pas ceux de connectorpool — écrire les deux fakes minimaux dans le
test) ; Modify `cmd/connector-pool-svc/wiring.go` (`fanIn`), `internal/e2e/{e2e,reference}_test.go`.

**Interfaces — Produces:** `kafka.BatchRunner interface { RunBatch(ctx context.Context, handle BatchHandler) error }`
et `type FanIn []BatchRunner` avec `RunBatch`. `*kafka.Consumer` satisfait `BatchRunner`.

- [ ] Déplacer, renommer l'élément en `BatchRunner`, doc d'une ligne : « FanIn runs every consumer with the
  same handler, so a service reads one topic per traffic category (ADR-0021 §4) through one runner. »
- [ ] Les trois tests de FanIn passent dans `internal/storage/kafka` (unitaires, pas de broker).
- [ ] Mutation : supprimer la boucle de drain → le test de jointure tombe ; restaurer.
- [ ] `go build ./... && go test ./internal/storage/kafka/ -run FanIn ./internal/connectorpool/ ./cmd/connector-pool-svc/` ; `make lint`.
- [ ] Commit `refactor(step-292b): FanIn rejoint internal/storage/kafka (PR3b)`.

---

### Task 2 : trois topics `mt.inbound`, choisis à l'ingestion

**Files:**
- `internal/storage/kafka/topics.go` : `TopicMTInbound` → `TopicMTInboundOTP/Transactional/Marketing` + `MTInboundTopics()`.
- `internal/pipeline/envelope.go` : champ `TrafficCategory cp.TrafficCategory` sur `InboundMT`.
- `internal/pipeline/wire.go` : `EncodeInbound` choisit le topic.
- `internal/ingest/ingest.go` : interface `Categories`, `NewIngestor(producer, admission, categories, logger)`.
- `cmd/rest-api-svc/wiring.go`, `cmd/smpp-server-svc/wiring.go` : `senderid.Holder` chargé au boot, rechargé
  par le watcher existant (la fonction de reload compose celle des limites et celle des sender IDs).
- `cmd/router-svc/wiring.go:254` : temporaire, un consommateur sur `MTInboundTopics()...` (Task 3 scinde) ;
  `:543` projecteur accepted : `kafka.NewConsumer(cfg.Kafka, serviceName+"-accepted-cdr", kafka.MTInboundTopics()...)`
  (AtStart) — réécrire le commentaire au-dessus s'il justifiait FromLatest.
- `internal/storage/kafkaprovision/plan.go`, `internal/testutil/kafkatest/kafkatest.go` ; tous les tests qui
  citent `TopicMTInbound` (liste : `grep -rln TopicMTInbound --include=*_test.go internal cmd`).

**Interfaces:**
- Produces: `kafka.TopicMTInboundOTP` etc., `kafka.MTInboundTopics() []string` (OTP, transactionnel,
  marketing) ; `ingest.Categories interface { Authorize(ctx context.Context, customerID uuid.UUID, from string) (cp.TrafficCategory, error) }`
  (satisfait par `*senderid.Holder`).

- [ ] **Rouges.** `wire_test.go` : table catégorie → topic (`otp`, `transactional`, `marketing`, `""` →
  marketing) ; catégorie inconnue → erreur. `ingest_test.go` : (a) un sender d'une catégorie `otp` connue
  part sur `mt.inbound.otp` ; (b) `Authorize` en erreur (non autorisé) → `mt.inbound.marketing` **et le
  message est accepté** (pas de refus à l'ingestion) ; (c) `categories == nil` → marketing. Lire les rouges.
- [ ] **Implémentation.**

```go
var inboundTopics = map[cp.TrafficCategory]string{
	cp.TrafficOTP:           kafka.TopicMTInboundOTP,
	cp.TrafficTransactional: kafka.TopicMTInboundTransactional,
	cp.TrafficMarketing:     kafka.TopicMTInboundMarketing,
}
```

  `EncodeInbound` : `topic, ok := inboundTopics[routedCategory(string(env.TrafficCategory))]`, `!ok` → erreur
  `pipeline: mt.inbound: unknown traffic category %q`. Le champ n'entre **pas** dans `inboundWire` (le
  routeur recalcule, ADR §2) ; sa doc le dit en une ligne.
  `Ingestor.Accept`, avant l'encodage :

```go
	env.TrafficCategory = cp.TrafficMarketing
	if i.categories != nil {
		if cat, err := i.categories.Authorize(ctx, env.CustomerID, env.From); err == nil {
			env.TrafficCategory = cat
		}
	}
```

- [ ] `go build ./... && go vet ./...` ; corriger les tests appelants (`NewIngestor` gagne un argument,
  `nil` dans les tests qui ne s'en servent pas ; les assertions de topic prennent le topic de la catégorie).
- [ ] **Câblage gardé** : un test de `cmd/rest-api-svc` (et `smpp-server-svc`) qui prouve que l'Ingestor du
  graphe reçoit un `Categories` non nil (suivre le patron des tests de câblage existants, ex.
  `smpp-server-svc` garde déjà `a.ingestor` « so a wiring test can submit through the admission »).
- [ ] **Mutations** : `EncodeInbound` force marketing → table tombe ; `Accept` ignore `categories` → (a)
  tombe ; le câblage passe `nil` → test de câblage tombe. Restaurer, `cmp`.
- [ ] Intégration (Docker) : `./internal/ingest/ ./internal/router/ ./internal/e2e/ ./internal/storage/... ./cmd/rest-api-svc/ ./cmd/smpp-server-svc/ ./cmd/router-svc/` → vert, 0 SKIP.
- [ ] `make lint`. Commit `feat(step-292b): mt.inbound par catégorie, lue à l'ingestion (PR3b)`.

---

### Task 3 : trois consommateurs au routeur

**Files:** `cmd/router-svc/wiring.go` (`stores.consumers`, `openStores`, close, readiness, `Deps.Consumer`),
`cmd/router-svc/main.go` (`pollQueueDepth` : les trois + outcome), `cmd/router-svc/*_test.go`.

- [ ] **Rouge** : dans le test de graphe du routeur, exiger une `*kafka.Consumer` par `MTInboundTopics()`, de
  groupe `router-svc-otp` / `router-svc-transactional` / `router-svc-marketing` (littéraux dans le test,
  via `Consumer.Group()`), et que `pollQueueDepth` reçoit les trois (si un test de queue depth existe,
  l'étendre : `cmd/router-svc/queuedepth_test.go`).
- [ ] **Implémentation** : boucle sur `kafka.MTInboundTopics()`, groupe `serviceName + "-" + strings.TrimPrefix(topic, "mt.inbound.")`,
  `kafka.NewConsumer` ; `Deps.Consumer: kafka.FanIn{...}` ; readiness `kafka-<catégorie>` par consommateur,
  nom tiré du topic **dans la boucle d'ouverture** (pas d'un découpage de nom de groupe).
- [ ] Vérifier `internal/router/` : aucune map ni clé indexée par `(partition, offset)` sans topic
  (`grep -n 'Partition\|Offset' internal/router/*.go`). En trouver une = rouge + correctif dans cette tâche.
- [ ] **Mutations** : un groupe constant pour les trois → test de graphe tombe ; n'ouvrir que le premier topic → tombe.
- [ ] Intégration : `./cmd/router-svc/ ./internal/router/ ./internal/e2e/` (Docker) → vert, 0 SKIP.
- [ ] `make lint`. Commit `feat(step-292b): trois consommateurs au routeur (PR3b)`.

---

### Task 4 : déploiement, garde HPA, documents

**Files:** `deploy/k8s/configmap.yaml`, `deploy/k8s/router-svc.yaml` (HPA + commentaire `:109`),
`internal/deploy/manifests_test.go` (`inspectHPA`, `inspectPoolCoversLanes`), `deploy/README.md`,
`docs/guide-ingenierie-passerelle-sms.md` (§5.1 et §13), `CLAUDE.md` (ligne « Ordre du pipeline »),
`internal/config/config.go:66` (commentaire `RouterLaneWindow`), commentaires `cmd/rest-api-svc/*`,
`cmd/smpp-server-svc/*`, `internal/ingest/*`, `cmd/router-svc/*` qui nomment `mt.inbound` au singulier.

- [ ] **Rouges** (garde) :
  - `inspectHPA` lit `matchExpressions` `queue In [...]` : `maxReplicas` doit rester **strictement sous** la
    largeur du topic le plus large des valeurs (lue dans le ConfigMap déployé : override sinon
    `KAFKA_TOPIC_PARTITIONS`) ; l'exemption nominative `connector-pool-svc` reste.
  - `inspectPoolCoversLanes` : voies par pod = **somme** sur les topics de `lanesPerPod(partitions_t, minReplicas)` ;
    `POSTGRES_MAX_CONNS` ≥ cette somme.
  - Fixtures cassées dans `internal/deploy/testdata/broken/` (patron existant) : un HPA `matchExpressions`
    dont `maxReplicas` = largeur du plus large → violation ; un dont `POSTGRES_MAX_CONNS` couvre le plus large
    mais pas la somme → violation. Lire les rouges.
- [ ] **Manifestes** : override `KAFKA_TOPIC_PARTITIONS_OVERRIDES: "mt.routed.otp=4,mt.routed.transactional=4,mt.inbound.otp=4,mt.inbound.transactional=4"` ;
  HPA du routeur :

```yaml
          selector:
            matchExpressions:
              - key: queue
                operator: In
                values: [mt.inbound.otp, mt.inbound.transactional, mt.inbound.marketing]
```

  Vérifier que le `POSTGRES_MAX_CONNS` déployé du routeur couvre 5 voies au plancher (minReplicas 4) ;
  sinon la garde le dit — le relever dans le ConfigMap/patch, pas abaisser la règle.
- [ ] `go test ./internal/deploy/` vert ; `deploy/test/check.sh` exit 0.
- [ ] **Mutations** : sommer → max dans `inspectPoolCoversLanes` → fixture « somme » tombe ; ignorer
  `matchExpressions` → fixture « plus large » tombe.
- [ ] **Documents** :
  - guide §13, à côté de l'alerte de lag de projection :

```promql
max by (queue) (queue_depth_records{queue=~"mt\\.(inbound|routed)\\.otp"}) > 5000   # for: 2m
absent(queue_depth_records{queue="mt.inbound.otp"})
absent(queue_depth_records{queue="mt.routed.otp"})
```

    une phrase : seuil en records faute de compteur de débit par topic ; 5 000 = seuil de l'HPA du pool,
    step-409 recalibre ; au pool, `absent()` ne part que si tous les pods disparaissent.
  - guide §5.1 : le chemin MT passe par `mt.inbound.<catégorie>` puis `mt.routed.<catégorie>`.
  - `CLAUDE.md`, ligne « Ordre du pipeline » : `auth → débit (sender ID puis compte) → choix du topic
    (lecture de la catégorie, pas une autorisation) → ACK durable Kafka → E.164 → …` (le reste inchangé).
  - `config.go` `RouterLaneWindow` : réserves en vol = somme des voies des trois topics × la fenêtre.
  - `deploy/README.md` : la jauge et les groupes du routeur ; la commande manuelle, sans drain, à lancer
    une fois PR3b déployée : `rpk topic delete mt.inbound mt.routed` puis
    `rpk group delete router-svc connector-pool-svc-<CONNECTOR_ID>`.
  - commentaires « produce mt.inbound » / « consumes mt.inbound » → `mt.inbound.<category>`.
- [ ] `make lint`. Commit `feat(step-292b): HPA du routeur, garde des voies par topic, alerte de lag OTP (PR3b)`.

---

### Après les tâches

- [ ] Revue finale en lecture seule, trois axes (mécanisme et tests · déploiement et docs · code en trop).
- [ ] Un lot de correctifs, une re-revue.
- [ ] `make check` (Docker + simulateur).
- [ ] Fiche : PR3b cochée dans le découpage. PR `feat(step-292b): mt.inbound par catégorie, trois consommateurs au routeur (PR3b)`, corps avec la commande de suppression manuelle.
