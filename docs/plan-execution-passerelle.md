# Plan d'exécution — Implémentation de la passerelle SMS

**Composant :** Passerelle SMS principale (Go)
**Statut :** Plan d'exécution v1.2 (réanalyse : couverture complète de l'API publique, corrélation DLR, modèle CDR versionné, schéma de hachage)
**Méthode :** tranche verticale MVP d'abord (walking skeleton), puis épaississement capacité par capacité.
**Contexte outil :** implémentation assistée par **Claude Code CLI**.

> Chaque jalon (`M0`…`M12`) précise : **Objectif**, **Dépend de**, **Livrables** (fichiers/packages/endpoints/topics concrets), **Nouvelles dépendances**, **Hors périmètre** (ce qui n'est explicitement PAS fait ici) et **Critères d'acceptation** (tests). Pas d'estimation en jours : on pilote à l'acceptation. Les **conventions transverses (§1)** fixent une fois pour toutes les points récurrents (ports, bibliothèques, topics, nommage) — les jalons y renvoient au lieu de les redéfinir.

---

## 0. Comment exécuter ce plan avec Claude Code CLI

### 0.1 La boucle par tâche

Une tâche = **une session Claude Code ciblée = une PR petite et verte**. Pour chaque tâche, donne à l'agent : (1) le **contexte** (réf de spec `§6.x`, contrat `api/openapi-*.yaml` ou `db/schema_passerelle_sms.sql`, package cible) ; (2) le **livrable** (recopié depuis ce plan) ; (3) les **critères d'acceptation** (recopiés — ce sont des tests). Demande d'**écrire les tests en même temps que le code**. Termine par la *definition of done* (§0.4).

### 0.2 `CLAUDE.md` (déjà en place, à la racine)

Claude Code le lit à chaque session : ce qu'on construit, l'ordre du pipeline MT, les 4 invariants, les couplages qui se savent d'avance, la *definition of done*, l'index docs. Le détail par territoire vit dans `.claude/rules/*.md`, chargé automatiquement à la lecture d'un fichier concerné (code Go, tests, `api/`, `db/`+`migrations/`, `internal/platform/errors`, fiches de travail). Garde les deux à jour.

### 0.3 Règle d'or du séquencement + convention STUB

On construit un **squelette qui marche** (`M2`) le plus tôt possible, puis **chaque jalon épaissit une capacité sans casser le flux de bout en bout**. Une étape de pipeline non encore implémentée est un **pass-through explicitement marqué** :

```go
// STUB M5: sender-ID authorization — pass-through until M5. See plan §8.
```

Un STUB **émet quand même son span** et est couvert par le test « ne logge pas le corps ». Il n'est jamais silencieux, jamais un `TODO` anonyme.

### 0.4 Definition of Done (chaque PR)

`gofmt`/`goimports` verts • `golangci-lint` sans alerte • `go test -race ./...` vert • `govulncheck` vert • critères d'acceptation couverts par des tests • aucun invariant violé • godoc sur l'exporté • PR focalisée sur une tâche.

### 0.5 Les 4 invariants (tests bloquants, verts à vie)

**(a)** le corps ne fuit dans aucune sérialisation (log/span/label) — posé à `M0` ; **(b)** un message routé par numéro exact traverse toutes les étapes de conformité — `M7` ; **(c)** facturation idempotente sous double `message_id` — `M9` ; **(d)** `max_sessions` refuse le bind au-delà du quota — `M3`.

### 0.6 Documents de référence (source de vérité)

Contrats : `db/schema_passerelle_sms.sql`, `api/openapi-public.yaml`, `api/openapi-admin.yaml`. Prose sous `docs/` : `specification-technique-passerelle-sms.md`, `guide-ingenierie-passerelle-sms.md` (dont §11 codes d'erreur), `guide-codage-go.md`, `convention-style-go.md`, `glossaire-domaine-sms.md`, `strategie-de-test-passerelle.md`, `adr/`.

---

## 1. Conventions transverses (à fixer AVANT M0)

Ces choix sont fixés une fois ; tous les jalons s'y conforment. Les changer plus tard est une décision d'équipe (ADR).

### 1.1 Module & versions

- **Module Go :** `github.com/martialanouman/go-gateway` (fixé). Il figure dans `go.mod`, le `-local`/`local-prefixes` de goimports/`.golangci.yml`, et tous les chemins d'import internes.
- **Go :** 1.23+ (pour `slog`, `go test -fuzz`, generics matures).

### 1.2 Bibliothèques imposées (dépendances Go)

Aucune autre bibliothèque pour ces rôles sans décision d'équipe. Le détail « ne pas faire » est dans `guide-codage-go.md` §15.

| Rôle | Bibliothèque |
|---|---|
| Routeur HTTP | `github.com/go-chi/chi/v5` |
| Couche OpenAPI | `github.com/danielgtaylor/huma/v2` |
| PostgreSQL | `github.com/jackc/pgx/v5` (+ `pgxpool`) |
| Requêtes typées | `sqlc` (génération) |
| Migrations | `github.com/golang-migrate/migrate/v4` |
| Redis / Dragonfly | `github.com/redis/go-redis/v9` |
| Kafka | `github.com/twmb/franz-go` |
| ClickHouse | `github.com/ClickHouse/clickhouse-go/v2` |
| Scripts de routage | `github.com/dop251/goja` (JS), `github.com/yuin/gopher-lua` (Lua) |
| UUIDv7 | `github.com/google/uuid` (`uuid.NewV7`) |
| E.164 | `github.com/nyaruka/phonenumbers` |
| Filtre de Bloom | `github.com/bits-and-blooms/bloom/v3` |
| gRPC | `google.golang.org/grpc` + `google.golang.org/protobuf` |
| Config (env) | `github.com/caarlos0/env/v11` |
| Hachage secrets | `golang.org/x/crypto` (bcrypt/argon2) |
| WebSocket | `github.com/coder/websocket` |
| Logs | `log/slog` (stdlib) |
| Tracing | `go.opentelemetry.io/otel` (+ exporteur OTLP) |
| Métriques | `github.com/prometheus/client_golang` |
| Tests d'intégration | `github.com/testcontainers/testcontainers-go` |

### 1.3 Outillage à installer (binaires, hors `go.mod`)

- **Go 1.23+** et **Docker + Docker Compose v2** (requis par `docker-compose.yml` et `testcontainers-go`).
- **golangci-lint** (lint), **sqlc** (génération de code SQL), **govulncheck** (`golang.org/x/vuln/cmd/govulncheck`).
- **golang-migrate** CLI *(optionnel)* — sinon les migrations tournent via la bibliothèque depuis le `Makefile`.
- **protoc** (ou **buf**) + `protoc-gen-go` + `protoc-gen-go-grpc` — requis à partir de **M3** (gRPC).
- **k6** ou **vegeta** — requis à **M12** (charge).

Un `make tools` installe les binaires Go (`sqlc`, `govulncheck`, plugins protoc) via `go install`.

### 1.4 Ports (chaque service : un port métier + un port ops commun)

Le **port ops** (9090) sert `/metrics`, `/healthz`, `/readyz` — **interne, jamais exposé publiquement**, absent des contrats OpenAPI. Le port métier porte le trafic du service.

| Service (`cmd/`) | Port métier | Protocole métier | Port ops |
|---|---|---|---|
| `rest-api-svc` | 8080 | HTTP REST **public** | 9090 |
| `admin-api-svc` | 8081 | HTTP REST **interne** | 9090 |
| `smpp-server-svc` | 2775 | SMPP (TCP) | 9090 |
| `connector-pool-svc` | — (client sortant) | SMPP client | 9090 |
| `router-svc` | — (consumer Kafka) | — | 9090 |
| `mo-dlr-router-svc` | — (consumer Kafka) | — | 9090 |
| `session-manager-svc` | 7000 | gRPC | 9090 |
| `billing-svc` | 7001 | gRPC | 9090 |
| `config-sync` | — (pub/sub) | — | 9090 |

### 1.5 Health (tranché — voir aussi guide d'ingénierie)

`/healthz` = **liveness** (le process vit, aucune dépendance vérifiée → échec = redémarrage pod). `/readyz` = **readiness** (les dépendances **vitales pour CE service** sont joignables → échec = retrait du LB, pas de redémarrage). La readiness reflète les **politiques de panne** : `router-svc` avec Redis coupé reste *ready* (fail-closed sur le débit, messages durables dans Kafka) mais devient *not ready* si Kafka est injoignable. Le `GET /health` **public** de `api/openapi-public.yaml` est celui de `rest-api-svc`, sur le port métier 8080, distinct des endpoints ops.

### 1.6 Topics Kafka (liste canonique)

`mt.inbound` (clé = hash compte) · `mt.routed` (clé = `(connector_id, shard_index)`, `shard_index = hash(message_key) % bind_pool_size`) · `mo.inbound` · `dlr.events` · `mt.dead-letter` · `mo.dead-letter` · `mt.reroute-park` · `metrics.stream` (alimente les WS temps réel, `M11`). `message_key` = **ID de message logique** (tous les segments UDH le partagent).

### 1.7 Nommage & emplacements

Services = `cmd/<nom>-svc/main.go`. Code métier sous `internal/` (jamais importable dehors). Protos gRPC sous `api/proto/`, code généré sous `internal/…/pb`. Migrations `migrations/NNNN_description.up.sql`/`.down.sql` (golang-migrate, dérivées de `db/schema_passerelle_sms.sql`). Colonnes SQL, clés JSON, topics : `snake_case` ; identifiants Go : `MixedCaps` (convention de style §2/§3).

### 1.8 Le faux SMSC in-repo (le pair de test des scénarios ordinaires)

Package `internal/testutil/fakesmsc`, lançable en test (embarqué) ou en process (`make fake-smsc`). Parle SMPP via `internal/smpp` (le codec, livré à `M2`). Réponses **scriptables** (`OK`, `Throttled`, `SysErr`, `Delay`), émission de MO/DLR à la demande. Débloque `M2`→`M7`. Le vrai simulateur (`docs/specification-technique-simulateur-smsc.md`) n'est requis qu'à `M8`. Détail : `strategie-de-test-passerelle.md` §2.

### 1.9 Identifiants : schéma de hachage (tranché)

Deux schémas distincts, choisis par fréquence de vérification et besoin de recherche :

- **Mot de passe de bind SMPP** (`password_hash`) : **argon2id** (ou bcrypt). Vérifié rarement (au bind), lent par conception. La ligne est résolue par `system_id`, le hash est ensuite vérifié.
- **Clé API** (`api_key_hash`) : **SHA-256 déterministe** de la clé (format `sgw_<random 32 o>`). Vérifiée **à chaque requête** REST (8 000/s) et cherchée **par hash** (index sur `api_key_hash`) — un hash salé non déterministe rendrait la recherche impossible et la vérification trop lente. L'entropie de la clé (256 bits aléatoires) rend le salage inutile.
- Comparaisons en **temps constant** dans les deux cas (`subtle.ConstantTimeCompare`).

### 1.10 Modèle d'écriture du CDR (tranché)

Pas de mutation ClickHouse par message (infaisable à 8 000/s). Le CDR est un **`ReplacingMergeTree` versionné** : **chaque changement de statut = une nouvelle ligne** portant le même `message_id` et une version croissante ; la lecture prend la dernière version (`argMax`/`FINAL` sur `message_id`). Qui écrit quoi :

1. **`router-svc`** (projecteur `internal/ingest`, PR #108) : ligne `status=accepted`, projetée depuis `mt.inbound` **après** l'ACK durable Kafka (jamais bloquant pour le `202` ; l'ingestion ne l'écrit plus elle-même).
2. **`router-svc`** (projecteur `internal/outcome`, ADR-0012) : ligne `status=enroute` (ou `failed`) depuis l'issue que `connector-pool-svc` publie sur `mt.outcome` après `submit_sm_resp` ; le pool n'écrit directement que les lignes qui précèdent l'envoi (`cancelled`, `rerouted`, dead-letter).
3. **`mo-dlr-router-svc`** : ligne de statut final (`delivered`/`failed`/`expired`) à la corrélation DLR.

Conséquence : `GET /messages/{id}` trouve toujours un message accepté (pas de fenêtre de 404 entre l'ACK et l'envoi).

### 1.11 Corrélation DLR (tranché)

Le SMSC attribue **son propre ID** dans `submit_sm_resp`, et les DLR référencent **cet ID-là**. À l'envoi, `connector-pool-svc` stocke le mapping `dlrmap:{connector_id}:{smsc_msg_id} → message_id` dans Redis (TTL = `validity_period` + marge). À la réception d'un DLR, `mo-dlr-router-svc` résout ce mapping pour retrouver `message_id`/`trace_id`. Un DLR sans mapping (expiré/inconnu) est journalisé et compté, jamais silencieusement jeté.

---

## 2. Le squelette qui marche (M2)

```
POST /messages ──► mt.inbound (Kafka) ──► router-svc (E.164 + route statique, reste STUB)
                                              │
                                              ▼
                                          mt.routed ──► connector-pool-svc ──► faux SMSC (SMPP)
                                                              │
                                                              ▼
                                                          CDR (ClickHouse)  ◄── GET /messages/{id}
```

Dès que ce flux passe un test bout-en-bout, l'architecture est prouvée. Les jalons `M3`+ s'y greffent.

---

## 3. Vue d'ensemble des jalons

| Jalon | Objectif | Débloque |
|---|---|---|
| **M0** | Fondations : dépôt, CI, dépendances docker, migrations, config, observabilité/ops, modèle d'erreur, primitives `platform` | tout |
| **M1** | Plan de contrôle minimal + Admin API (noyau de provisioning) | de quoi configurer un envoi |
| **M2** | **Squelette vertical MT** (REST → codec SMPP → faux SMSC → CDR) | l'architecture prouvée |
| **M3** | Ingress SMPP serveur + sessions (registre gRPC) | double protocole, un pipeline |
| **M4** | Voie retour MO/DLR + webhooks + numéros entrants | bidirectionnel complet |
| **M5** | Conformité : autorisation sender ID, opt-out, anti-spam | envoi conforme |
| **M6** | Encodage/segmentation UDH + gestion du débit | messages longs + protection connecteurs |
| **M7** | Routage avancé : numéros exacts, scripts, stratégies, hot reload | routage production |
| **M8** | Résilience : disjoncteur, fallback, pool de binds, reconnexion | tolérance aux pannes SMSC |
| **M9** | Facturation opt-in : soldes MT/MO, réserve/capture, grand livre | monétisation |
| **M10** | Contenu : stockage chiffré, RGPD, rétention/tiering | conformité données |
| **M11** | Observabilité complète : spans, métriques, temps réel, export | exploitabilité |
| **M12** | Durcissement : charge (NFR), chaos, sécurité, go-live | mise en production |

---

## 4. M0 — Fondations & outillage

**Objectif :** un dépôt qui build/teste/lint, démarre ses dépendances, et fournit les primitives transverses.
**Dépend de :** —

**Livrables**

- `go.mod`/`go.sum` (module §1.1) ; `Makefile` (cibles : `tools`, `up`, `down`, `migrate`, `build`, `test`, `lint`, `vuln`, `run SVC=`, `generate`, `tidy` — la cible `fake-smsc` est ajoutée à `M2` avec le package).
- `docker-compose.yml` : `postgres:18`, `redis` (ou `dragonfly`), `kafka` (Redpanda accepté), `clickhouse`, avec ports et volumes nommés.
- `.golangci.yml` (contenu de `convention-style-go.md` §9, `local-prefixes` = module).
- CI (`.github/workflows/ci.yml` ou équivalent) : `lint`, `go test -race`, `govulncheck`, `build`.
- `migrations/0001_init.up.sql` + `.down.sql` (dérivées de `db/schema_passerelle_sms.sql`) ; runner `internal/storage/postgres/migrate.go` (golang-migrate, driver pgx).
- `internal/config` : structs `Config` + `Load()` (env via `caarlos0/env`), **validation au boot** (`log.Fatal` seul endroit toléré).
- `internal/observability` : init `slog` JSON ; init OTel (exporteur OTLP) ; **serveur ops réutilisable** `NewOpsServer(readiness ...Check)` exposant `/metrics`, `/healthz`, `/readyz` (§1.5).
- `internal/platform/uuidx` (UUIDv7), `internal/platform/e164` (`Normalize`), `internal/platform/msg` (**type `Body` masquant** : `String`/`MarshalJSON`/`LogValue` → `[REDACTED]`, `Reveal()`).
- `internal/platform/errors` : sentinelles + type `Code` + **table de mapping** `code → (httpStatus, smppStatus)` (guide d'ingénierie §11.3). *(La surcharge `huma.NewError` arrive à `M1` avec le premier service HTTP.)*
- `cmd/router-svc/main.go` **squelette canonique** (guide de codage §5) : `signal.NotifyContext`, `Load()` config, init observabilité, démarre le serveur ops, bloque jusqu'à `SIGTERM`. Sert de gabarit à tous les `main`.

**Nouvelles dépendances :** pgx, golang-migrate, google/uuid, nyaruka/phonenumbers, caarlos0/env, otel (+OTLP+sdk), prometheus/client_golang. *(Pas encore de chi/huma/kafka/redis.)*

**Hors périmètre :** aucun traitement de message, aucun endpoint métier, aucune Admin API, aucun accès Kafka/Redis/ClickHouse en écriture métier (seuls les checks readiness pingent les dépendances).

**Critères d'acceptation**

- `docker compose up` démarre les 4 dépendances ; `make migrate` applique le schéma sans erreur.
- `make run SVC=router-svc` démarre ; `GET :9090/healthz` → 200 ; `GET :9090/readyz` → 200 quand les deps vitales répondent, 503 si Kafka coupé.
- **Invariant (a)** vert : un test échoue si sérialiser une struct contenant un `Body` révèle le clair (log JSON `slog` **et** attribut de span via exporteur de test).
- `make lint`, `go test -race ./...`, `make vuln` verts.

---

## 5. M1 — Plan de contrôle minimal + Admin API (noyau)

**Objectif :** provisionner via l'API le minimum pour envoyer : client → compte → identifiants → connecteur → route statique.
**Dépend de :** M0.

**Livrables**

- `internal/storage/postgres` : schémas `sqlc` + repositories pour `customers`, `smpp_accounts`, `credentials`, `smsc_connectors`, `routes`+`route_targets`, `sender_ids`.
- `internal/platform/errors` : surcharge `huma.NewError` produisant le modèle plat `{ code, message, errors[] }` en `application/json`.
- `cmd/admin-api-svc` (chi + huma) implémentant **ce sous-ensemble** de `api/openapi-admin.yaml` (operationId) :
  - `list/create/get/update/delete-customer`, `suspend-customer`
  - `list/create/get/update/delete-smpp-account`, `set-account-channels`, `set-account-session-limits`
  - `list/create/update-credential-status/revoke/rotate-credential` (secret renvoyé **une fois** ; hachage selon **§1.9** : argon2id pour le bind, SHA-256 déterministe indexé pour la clé API)
  - `list/create/get/update/delete-connector`
  - `list/create/get/update/delete-route`
  - `list/create/update/delete-sender-id`
- Auth opérateur (bearer) : **middleware avec un validateur de jeton simple** (interface `TokenVerifier`), scopes câblés mais permissifs — la vraie intégration OIDC/mTLS est à `M12`.
- Test de **contrat** : l'OpenAPI généré par Huma pour ces opérations est comparé à `api/openapi-admin.yaml` (opérations, schémas, `Error`) et échoue à la moindre dérive.

**Nouvelles dépendances :** chi, huma, sqlc (outil), golang.org/x/crypto (hachage).

**Hors périmètre :** aucun envoi de message ; pas de routage dynamique, script, opt-out, facturation, contenu, métriques temps réel ; les endpoints Admin non listés ci-dessus (inbound-numbers, suppressions, billing, etc.) arrivent à leur jalon. Auth opérateur réelle reportée à `M12`.

**Critères d'acceptation**

- Parcours complet : créer `customer` → `smpp_account` → `credential(api_key)` (secret renvoyé **une seule fois**, masqué ensuite) → `smsc_connector` → `route` statique.
- Cardinalité imposée par le schéma : 2ᵉ `smpp_bind` ou `api_key` sur un compte → `409` (`code=conflict`).
- Réponses d'erreur au format plat `{ code, message, errors[] }`.
- Suspendre un client cascade sur ses comptes (statut effectif = min).
- Test de contrat vert.

**Dette connue (issue de la revue M1, à traiter à un jalon ultérieur) :**

- **Null-clearing des champs nullable.** Les `UPDATE` partiels utilisent `COALESCE(narg, col)`, donc un `PATCH { "champ": null }` sur un champ nullable (`overdraft_limit`, `throughput_limit_per_sec`, `match_*`…) est un no-op silencieux : impossible de remettre une valeur à `NULL` via l'Admin API. Le tri-state (absent / null explicite / valeur) est reporté ; **y revenir si aucun jalon ultérieur ne le résout**. Réf : `internal/controlplane/doc.go`.
- **N+1 sur `list-routes` / `get-route`.** Les `route_targets` sont lus par route, hors transaction. Acceptable au débit du plan de contrôle ; à batcher (`WHERE route_id = ANY($1)`) quand le volume l'exigera. Réf : `internal/storage/postgres/routes.go`.

---

## 6. M2 — Squelette vertical MT (REST → faux SMSC → CDR)

**Objectif :** le walking skeleton de bout en bout. **Jalon le plus important.**
**Dépend de :** M0, M1.

**Livrables**

- `internal/smpp` : **codec PDU** SMPP v3.4 (encode/décode `bind_*`, `submit_sm`/`_resp`, `deliver_sm`, `enquire_link`, `unbind`), support TLV/UDH, payload > 254 o. *(Requis ici car la voie sortante parle SMPP au faux SMSC. Le serveur SMPP entrant vient à `M3`.)*
- `internal/testutil/fakesmsc` : faux SMSC (§1.8), réponses scriptables, émission MO/DLR ; cible `make fake-smsc`.
- `internal/storage/kafka` : producteur (`acks=all`, idempotent) + consommateur (franz-go), constantes de topics (§1.6), clé de partition.
- `internal/storage/clickhouse` : sink CDR **versionné selon §1.10** (`ReplacingMergeTree`, une ligne par changement de statut, lecture `argMax`) + lecture de statut (schéma de l'appendice ClickHouse du `.sql`).
- `cmd/rest-api-svc` (chi + huma) : `submit-messages` (`POST /messages`, auth clé API SHA-256 §1.9 → compte → client, contrôle `rest_enabled`, **génération `message_id`/`trace_id` UUIDv7 à l'ingestion**, publication `mt.inbound`, `202` **après** confirmation durable, **puis** ligne CDR `accepted` asynchrone §1.10 — *projetée par `router-svc` depuis PR #108*), `get-message` (`GET /messages/{id}` depuis le CDR), et le `GET /health` public (§1.5). Span racine ouvert à l'ingestion.
- `cmd/router-svc` : consomme `mt.inbound` → **E.164** → **résolution déclarative statique uniquement** → publie `mt.routed`. Étapes 2–5 et 7–8 du pipeline = **STUB marqués** (§0.3). Span par étape.
- `cmd/connector-pool-svc` : `bind_pool_size=1`, consomme `mt.routed`, `submit_sm` → faux SMSC, suit `submit_sm_resp`, écrit le CDR (`status=enroute`) *(écriture déplacée vers le projecteur `mt.outcome` de `router-svc` par step-201c, ADR-0012)*.

**Nouvelles dépendances :** franz-go, clickhouse-go. *(Codec SMPP = interne, sans lib externe.)*

**Hors périmètre :** aucune étape de conformité active (sender ID/opt-out/anti-spam restent STUB) ; pas de segmentation (1 segment supposé), pas de débit, pas de facturation, pas de SMPP **serveur** entrant, pas de MO/DLR, un seul bind sortant, pas de disjoncteur ; les autres endpoints publics (`list-messages`, `cancel-message`, `get-account`, `Idempotency-Key`) arrivent à `M3` (ils requièrent Redis et la parité `cancel_sm`).

**Critères d'acceptation**

- Test bout-en-bout (`testcontainers` + `fakesmsc`) : `POST /messages` → `202` → `GET /messages/{id}` renvoie **immédiatement** `accepted` (pas de fenêtre de 404), puis `enroute` après l'envoi au faux SMSC.
- `trace_id`/`message_id` (UUIDv7) générés à l'ingestion, présents dans le CDR et les en-têtes Kafka.
- Le `202` n'a lieu qu'après écriture durable dans `mt.inbound` (test : faux SMSC coupé → le `202` sort quand même, le message reste en file).
- Un span par étape (STUB compris) émis, vérifiable via l'exporteur de test OTel.
- Round-trip du codec SMPP testé unitairement.

---

## 7. M3 — Ingress SMPP serveur + sessions

**Objectif :** un ESME client peut se binder et soumettre ; SMPP et REST partagent **le même** pipeline.
**Dépend de :** M2 (codec `internal/smpp`, pipeline).

**Livrables**

- `api/proto/session.proto` + code généré : service gRPC `SessionRegistry` (bind/unbind/lookup, `Deliver` vers le pod détenteur).
- `cmd/session-manager-svc` : registre Redis faisant autorité, gRPC :7000, `max_sessions` appliqué **au bind contre le registre inter-pods**, table `account → {pod_id, bind_id}[]`, supervision `enquire_link`.
- `cmd/smpp-server-svc` : écoute SMPP :2775, machine à états de session (`internal/smpp/session`), auth bind (`system_id` → `credentials` `smpp_bind`, mot de passe en temps constant, `allowed_bind_types`), `submit_sm` → `mt.inbound` (**pipeline identique**), `enquire_link`, `unbind`, fenêtre (`window_size`). `query_sm`/`cancel_sm` selon bascules (`ESME_RINVCMDID` si désactivé).
- Anti-brute-force : compteur par `system_id`/IP (Redis TTL), backoff, événement de sécurité auditable.
- Rotation d'identifiant : fenêtre de grâce (`previous_secret_hash`/`grace_expires_at`).
- **Complétion de l'API publique** (`api/openapi-public.yaml`, Redis désormais disponible) : `list-messages` (pagination par curseur sur le CDR), `get-account` (projection lecture seule), et l'en-tête **`Idempotency-Key`** (Redis, fenêtre 24 h : un rejeu avec la même clé renvoie le résultat d'origine ; même clé + corps différent → `409 idempotency_conflict`). Test de contrat public. **Annulation :** `cancel_sm` (SMPP) uniquement — pas de surface REST (annule un message pas encore envoyé, sinon `ESME_RCANCELFAIL` ; voir [ADR-0009](adr/0009-annulation-reservee-smpp.md)).

**Nouvelles dépendances :** grpc, protobuf, go-redis (registre de sessions).

**Hors périmètre :** pas de MO/DLR (voie retour à `M4`) ; pas de reconnexion/disjoncteur côté SMSC (`M8`) ; `query_sm` non soumis à sa limite de débit dédiée (arrive avec le débit, `M6`).

**Critères d'acceptation**

- Un client SMPP (`fakesmsc` en mode ESME ou client de test) bind, soumet, reçoit `submit_sm_resp` ; le message suit le même chemin CDR que REST.
- **Invariant (d)** : bind au-delà de `max_sessions` → `ESME_RBINDFAIL` ; un unbind/expiration `enquire_link` libère le jeton.
- Op désactivée → `ESME_RINVCMDID`.
- **Parité protocole** : le même message soumis en REST et en SMPP produit un traitement identique en aval (test dédié).
- Rotation avec `gracePeriodSec` : ancien secret valide pendant la fenêtre, coupé après.
- `cancel_sm` (SMPP-only, ADR-0009) : annulation d'un message en file → `ESME_ROK` + CDR `cancelled` ; déjà envoyé → `ESME_RCANCELFAIL` ; inconnu → `ESME_RINVMSGID`.
- `Idempotency-Key` : rejeu même clé + même corps → réponse d'origine, un seul message publié ; même clé + corps différent → `409 idempotency_conflict`.

---

## 8. M4 — Voie retour MO/DLR + webhooks

**Objectif :** MO et DLR routés vers le bon compte ; bidirectionnel complet.
**Dépend de :** M3.

**Livrables**

- `connector-pool-svc` : à l'envoi, **stockage du mapping de corrélation DLR** `dlrmap:{connector_id}:{smsc_msg_id} → message_id` (§1.11, Redis TTL) ; réception `deliver_sm`, classification MO vs DLR (`esm_class`), publication `mo.inbound` / `dlr.events`.
- Repos + Admin (`api/openapi-admin.yaml`) : `list/create/update/delete-inbound-number`, `assign-inbound-number`, `list/create/update/delete-inbound-keyword`, `list-unrouted-mo`.
- `cmd/mo-dlr-router-svc` : MO → E.164 → résolution du compte (dédié → son compte ; partagé → mot-clé ; sinon file « non routés ») → remise SMPP `deliver_sm` (via gRPC `SessionRegistry.Deliver` au pod détenteur, round-robin binds vivants) **ou** webhook. DLR → corrélation `message_id` → maj CDR (`delivered_at`, `status`, `latency_ms`) → transmission au compte.
- `internal/…/webhook` : envoi signé **HMAC-SHA256**, retries avec backoff, dead-letter après N tentatives.

**Nouvelles dépendances :** aucune (HMAC/HTTP = stdlib).

**Hors périmètre :** pas de détection STOP ni comptage MO (opt-out à `M5`, facturation MO à `M9`) — le MO est routé et remis, sans conformité ni compteur pour l'instant (STUB marqués).

**Critères d'acceptation**

- `fakesmsc` émet un MO sur un numéro entrant → remis au bon compte (webhook ou bind actif via gRPC).
- Un DLR est corrélé via le mapping §1.11 (`smsc_msg_id → message_id`), met à jour le CDR (nouvelle ligne versionnée §1.10) et est transmis ; un DLR sans mapping est journalisé + compté, jamais jeté en silence.
- MO non résolu → visible dans `list-unrouted-mo`, jamais abandonné silencieusement.
- Webhook : signature HMAC vérifiable, retry avec backoff, dead-letter après épuisement.

---

## 9. M5 — Conformité sur le chemin critique

**Objectif :** activer les étapes STUB de conformité, avant tout coût.
**Dépend de :** M4.

**Livrables**

- `internal/pipeline/senderid` : autorisation (§6.19), politique par compte (`strict`/`allow_unregistered_numeric`/`disabled`), `source_addr` vs `sender_ids` `active` du client.
- `internal/pipeline/optout` : repos `suppressions`/`opt_out_keywords`, **Bloom par portée en mémoire** (rafraîchi à froid pour l'instant, hot reload à `M7`), étape MT **bloquante** (union platform/customer/account/inbound_number), détection STOP côté MO écrivant une suppression + auto-réponse (MT jamais facturé). Admin : `list/create/delete-suppression`, `check-suppression`, `import-suppressions`, `*-opt-out-keyword`.
- `internal/pipeline/antispam` : vélocité (MT + MO entrant), contenu (regex précompilées), doublons (Redis TTL), réputation ; actions `block`/`flag`/`throttle` ; **fail-open avec flag** sur perte Redis pour l'état partagé, règles de contenu statiques maintenues. Admin : `*-antispam-rule`.

**Nouvelles dépendances :** bits-and-blooms/bloom.

**Hors périmètre :** la propriété d'invariant (b) « exact-route saute la résolution mais pas la conformité » n'est pleinement testable qu'à `M7` (les numéros exacts arrivent là) ; le hot reload des Bloom est à `M7` (ici, rechargement à froid au démarrage).

**Critères d'acceptation**

- Sender non autorisé → rejet (`code=sender_id_not_authorized`, `403`/`ESME_RINVSRCADR`), CDR `rejected`.
- Destinataire désabonné bloqué si **l'une** des portées matche (`code=recipient_opted_out`).
- Un STOP crée une suppression scopée sur le numéro entrant ; le MO est **quand même remis** et **jamais facturé**.
- Propriété **pas de faux négatif** du Bloom : property test sur des MSISDN présents.
- Le matching de contenu lit le clair **en mémoire** ; test que rien n'est stocké ni loggé (renforce l'invariant a).

---

## 10. M6 — Encodage/segmentation + gestion du débit

**Objectif :** messages longs corrects, connecteurs protégés.
**Dépend de :** M5.

**Livrables**

- `internal/pipeline/encoding` : détection GSM-7/UCS-2/8-bit (respect `data_coding_default`), calcul `segment_count`, découpe UDH ; réassemblage MO. Fuzz.
- `internal/pipeline/ratelimit` : token-bucket **Lua atomique** (script chargé par `EVALSHA`) par compte/connecteur/route ; précédence `throughput_limit_per_sec` connecteur (plafond dur) ≥ `rate_limits` (validé à l'écriture) ; **fail-closed** sur perte Redis (plafond technique statique local).
- Throttling adaptatif AIMD piloté par `submit_sm_resp` (`ESME_RTHROTTLED`).
- Limite de débit dédiée pour `query_sm` (§6.22).

**Nouvelles dépendances :** aucune.

**Hors périmètre :** pas de disjoncteur ni de fallback (`M8`) ; le débit s'applique par pod/instance sans coordination fine multi-pod au-delà des compteurs Redis.

**Critères d'acceptation**

- Message long segmenté avec le bon `segment_count` ; MO concaténé réassemblé (tests aux frontières 160/153, 70/67).
- Limite de débit appliquée **atomiquement sous concurrence** (`-race`, N goroutines).
- Plafond technique du connecteur jamais dépassé.
- `ESME_RTHROTTLED` fait baisser le débit (AIMD) puis remonter.
- La segmentation précède débit et (futur) crédit.

---

## 11. M7 — Routage avancé

**Objectif :** routage de niveau production (portabilité, scripts, stratégies, hot reload).
**Dépend de :** M6.

**Livrables**

- `internal/routing/exact` : repo `exact_routes`, `exactroute:{msisdn}` Redis + **Bloom en mémoire**, **court-circuit L0** ; Admin `list/create/update/delete-exact-route`, `lookup-exact-route`, `import-exact-routes` (async).
- `internal/routing/script` : runtimes `goja` + `gopher-lua` **poolés**, **plafond d'instructions = garde primaire**, timeout mur, plafond mémoire ; contrat `resolveRoute(message) → routeId | null` ; résolution `account → customer → platform` ; cycle `draft → validate → test → publish` ; Admin `*-routing-script`, `assign/validate/test/publish`, `list-versions`.
- `internal/routing/strategy` : les 6 stratégies + `fallback_route`.
- `internal/routing/snapshot` + `cmd/config-sync` : **instantané immuable + pointeur atomique** (guide de codage §5.1) ; pub/sub `config-sync` ; surcouche mutable séparée pour l'état volatil ; **hot reload des Bloom** (exact + suppressions).

**Nouvelles dépendances :** goja, gopher-lua.

**Hors périmètre :** l'état de disjoncteur consommé par le routeur reste absent jusqu'à `M8` (ici, toutes les cibles sont considérées disponibles).

**Critères d'acceptation**

- **Invariant (b)** : un message routé L0 traverse E.164, sender ID, opt-out, anti-spam, segmentation, débit (test avec spies par étape).
- Scénario de portabilité : un numéro porté est routé par `exact_routes`, pas par préfixe.
- Un script retourne `routeId` valide ou `null` → repli déclaratif ; dépassement du plafond d'instructions → repli + log + métrique.
- Hot reload échange routes et Bloom **sans downtime** (trafic continu pendant un reload).
- Chaque stratégie distribue conformément (`weighted`/`hash_based` déterministes).

---

## 12. M8 — Résilience connecteurs

**Objectif :** tolérer un SMSC qui se dégrade/tombe, monter en débit par connecteur.
**Dépend de :** M7. **Bascule au vrai simulateur SMSC** (injection de pannes).

**Livrables**

- `internal/connector/breaker` : machine à états par connecteur ; agrégation multi-pod par hash `breaker:binds` + **majorité** ; `breaker:state` + pub/sub `breaker:events` ; `router-svc` lit uniquement à la (re)construction de l'instantané.
- `fallback_chain` en en-tête + reroute unilatéral ; draineur borné + `mt.reroute-park`.
- Pool de binds : `bind_pool_size > 1`, partition `mt.routed` par `(connector_id, shard_index)`, segments d'un message sur un seul bind.
- `internal/connector/reconnect` : auto-reconnexion **opt-in** (backoff + jitter), `link_status` vs `breaker_state` **distincts** ; `ESME_RINVPASWD` stoppe la boucle. Admin `rebind-connector`, `get-connector-status`, `set-connector-reconnect-policy`, `set-connector-bind-pool`.
- Dead-letter (`mt.dead-letter`/`mo.dead-letter`) + retraitement.

**Nouvelles dépendances :** aucune (le simulateur SMSC est un projet/binaire externe, pas un module Go).

**Hors périmètre :** rien de nouveau côté facturation/contenu.

**Critères d'acceptation**

- Un connecteur dégradé (simulateur) ouvre le disjoncteur ; le trafic bascule via `fallback_chain` ; l'excédent est parqué puis rejoué.
- Agrégat de disjoncteur correct avec binds sur plusieurs pods (test multi-instances).
- `bind_pool_size=4` augmente le débit ; segments d'un message sur un seul bind (test d'ordre).
- Bind coupé + auto-reconnexion → revient ; sans auto-reconnexion → `link_status=down` + rebind manuel.
- `ESME_RINVPASWD` stoppe l'auto-retry.
- Les tests de résilience précédemment `Skip`és (M2→M7) sont dé-`Skip`és et verts.

---

## 13. M9 — Facturation (opt-in)

**Objectif :** facturation prépayée/postpayée, soldes MT/MO séparés, sans impact quand désactivée.
**Dépend de :** M6 (segmentation → coût). *(Peut suivre M8 en parallèle de la résilience.)*

**Livrables**

- `api/proto/billing.proto` + `cmd/billing-svc` (gRPC :7001) : `balances` (MT/MO séparés), **réserve/capture/libère en Lua** (idempotent par `message_id`), compteur MO, `billing_ledger` (partitionné), propriétaire selon `balance_scope`, découvert, `charge_on` submission/delivery + `refund`.
- Intégration : réserve dans `router-svc` (étape 8), capture/libère dans `connector-pool-svc` ; **saut total quand désactivé** (contrôle booléen en cache, zéro appel réseau).
- Réhydratation du cache Redis depuis le grand livre Postgres (autorité durable) ; fail-closed strict pendant la fenêtre.
- Adaptateur externe (§6.10) : modes `balance_check`/`consume_delegate_async`/`consume_delegate_sync`.
- Admin : `get/update-customer-billing`, `get-customer-balances`, `topup/transfer-balance`, `change-balance-scope`, `get-billing-ledger`, `*-rate-plan`, `*-billing-provider`, `test-billing-provider` ; WS `stream-billing-alerts`.

**Nouvelles dépendances :** aucune (gRPC déjà là).

**Hors périmètre :** pas de tarification temps réel via un fournisseur externe synchrone en prod (l'adaptateur existe, l'intégration réelle est optionnelle) ; le contenu/chiffrement est à `M10`.

**Critères d'acceptation**

- Prépayé MT : réserve → capture au succès, libère à l'échec ; solde insuffisant → `402`/code d'extension, **aucune** entrée de grand livre.
- **Invariant (c)** : double livraison d'un même `message_id` ne facture qu'une fois.
- Le compteur MO ne bloque **jamais** le MT ; s'arrête à `mo_billing_floor` + alerte.
- `change-balance-scope` refusé (`409`) si un solde ≠ 0.
- **Facturation désactivée = zéro appel réseau** (test comptant les I/O du chemin chaud).

---

## 14. M10 — Contenu, chiffrement & RGPD

**Objectif :** stockage de contenu configurable et chiffré, effacement RGPD, rétention.
**Dépend de :** M5 (CDR + policy), M9 (soldes/ledger). Les `content_keys` étaient initialement prévues chez `billing-svc` ; elles vivent dans un service dédié `content-key-svc` (seul détenteur de la KMS) — voir ADR-0011.

**Livrables**

- `internal/content` : politique (`off`/`stored_plaintext`/`stored_encrypted`), **enveloppe KMS + clé par client** (`content_keys`), chiffrement **à l'écriture CDR uniquement**, lecture `content:read` gardée et **auditée**, crypto-shred. Interface `KMS` + implémentation locale de dev.
- Rétention/partitionnement/tiering (§6.14) : partitions quotidiennes, TTL, archive Parquet ; `content_retention_days` découplé.
- Effacement RGPD : `erase-customer-content` (crypto-shred), `gdpr-erase` client (crypto-shred + purge CDR) et MSISDN (suppression ligne à ligne across clients) + attestation (`get-gdpr-erase-job`) ; `rotate-content-key` ; `get-message-content`.

**Nouvelles dépendances :** SDK KMS **spécifique au fournisseur** (à décider : AWS KMS / GCP KMS / Vault) — derrière l'interface `KMS`, donc localisée. L'impl de dev n'ajoute rien.

**Hors périmètre :** choix définitif du fournisseur KMS (décision d'infra ; l'interface le rend interchangeable) ; l'archivage froid réel vers stockage objet est opérationnel (l'implémentation du tiering est fournie, le bucket est infra).

**Critères d'acceptation**

- Corps chiffré lisible uniquement via `content:read` (accès audité) ; **invariant (a)** re-vérifié sous **chaque** valeur de `content_storage`.
- Crypto-shred (clé `destroyed`) rend le contenu illisible sans réécrire le CDR.
- Effacement MSISDN retire contenu + métadonnées across clients, **garde** l'opt-out ; attestation émise.
- Purge par **drop de partition** à l'échéance (pas de `DELETE WHERE`).

---

## 15. M11 — Observabilité complète & temps réel

**Objectif :** exploitabilité — traçage, métriques, streams, export.
**Dépend de :** transverse ; finalisé ici.

**Livrables**

- Spans par étape complets (nommage stable `pipeline.*`, `connector.*`), 100 % sur erreur/rejet/timeout ; jamais le corps.
- Catalogue de métriques à labels **bornés** (compte/connecteur/route/statut) : latences ingestion & bout-en-bout, profondeur de file, `breaker_state`, timeouts de script, fraîcheur du cache de solde.
- Gateway WS/SSE (`coder/websocket`) alimentée par `metrics.stream` : `stream-metrics`, `stream-sessions`, `stream-billing-alerts`.
- `get-message-trace`, `search-messages`, `create/get-message-export` (async, row-cap, masque MSISDN par rôle).
- Alerting Alertmanager **indépendant** du tableau de bord (règles infra).

**Nouvelles dépendances :** coder/websocket.

**Hors périmètre :** les dashboards Grafana et les règles Alertmanager sont des artefacts infra (fournis en exemple, ajustés par l'exploitation).

**Critères d'acceptation**

- Trace complète d'un message via `get-message-trace`, **sans aucun corps**.
- Métriques fraîches < 5 s ; **aucun label à cardinalité non bornée** (test de garde qui échoue sur un label MSISDN/message_id).
- Les WS poussent les mises à jour ; l'export produit un fichier masqué ; une alerte se déclenche (solde bas, disjoncteur ouvert).

---

## 16. M12 — Durcissement, charge & mise en production

**Objectif :** atteindre les NFR et passer la checklist de prod.
**Dépend de :** tous.

**Livrables**

- Campagne de **charge** (`k6`/`vegeta` + générateur de binds SMPP) : soutenu 8 000 SMS/s, pic 15 000 ; ingestion p99 < 250 ms ; bout-en-bout p99 < 2 s (disjoncteur fermé). Tuning : partitions Kafka, batch ClickHouse, pool `pgx`.
- **Chaos** : perte Redis (vérifier **chaque** politique de panne), flapping connecteur, redémarrage de pods (drain gracieux, `PodDisruptionBudget`, binds préservés), failover Postgres (réhydratation solde).
- **Sécurité** : TLS/SMPP-TLS/mTLS, hachage des identifiants, scan injection (`gosec`), gestion des secrets, piste d'audit, `govulncheck` ; **auth opérateur réelle** (OIDC/mTLS) remplaçant le stub de `M1`.
- Manifests `deploy/` (Kubernetes) : Deployments, Services, HPA (CPU/lag Kafka), PDB, probes `/healthz`/`/readyz`.
- Dérouler la **checklist de mise en production** (guide d'ingénierie §15).

**Nouvelles dépendances :** k6/vegeta (binaires, hors `go.mod`) ; gosec (outil).

**Hors périmètre :** reprise après sinistre inter-région (RPO/RTO) — non-objectif de la spec (§1.2bis).

**Critères d'acceptation**

- Débit soutenu tenu (disjoncteur fermé) avec les budgets de latence respectés.
- Rolling deploy sans coupure des binds (drain + PDB).
- L'injection de pannes dégrade **conformément aux politiques documentées** sans perte de message.
- Auth opérateur réelle active ; checklist de prod cochée.

---

## 17. Graphe de dépendances

```
M0 ─► M1 ─► M2 ─► M3 ─► M4 ─► M5 ─► M6 ─┬─► M7 ─► M8 ─► M11 ─► M12
                                         └─► M9 ─► M10 ────────────┘
```

`M2` est le point de bascule : avant, on outille ; après, chaque jalon épaissit un flux vivant. Une fois `M6` acquis, `M9`/`M10` (facturation, contenu — service `billing-svc`) peuvent avancer **en parallèle** de `M7`/`M8` (routage, résilience — `connector-pool-svc`), car ils touchent des services distincts. Le **codec SMPP** (`internal/smpp`) est livré à `M2` (voie sortante) puis étendu à `M3` (serveur entrant).

---

## 18. Le test harness (transversal)

Le pair SMSC arrive en deux temps. De `M2` à `M7` on utilise le **faux SMSC minimal in-repo** (`internal/testutil/fakesmsc`, §1.8) : il joue le SMSC en sortie et l'ESME en entrée, réponses scriptables et émission de MO/DLR. Le **vrai simulateur est livré depuis `M8`** (`internal/testutil/smscsim`, `make smsc-sim`) et porte les scénarios de résilience exigeant une injection de pannes réaliste (disjoncteur, reroute, reconnexion) ; la coupure de lien elle-même passe par `internal/testutil/tcpproxy`, à une adresse stable que le redémarrage d'un conteneur ne préserverait pas. Les **tests de contrat** vérifient que chi+huma reste fidèle à `api/openapi-*.yaml` ; les **tests d'intégration** (`testcontainers-go`) montent Postgres/Redis/Kafka/ClickHouse ; les **quatre invariants** (§0.5) restent verts en permanence. Détail complet : `strategie-de-test-passerelle.md`.
