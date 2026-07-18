# Step 000 — M0 : Fondations & outillage

> **Statut :** FAIT (jalon livré, cf. commit `ecf2012`)
> **Jalon (plan d'exécution) :** §4 — `docs/plan-execution-passerelle.md`
> **Dépend de :** — · **Débloque :** tout (M1 → M12)
> **Pair de test SMSC :** faux SMSC in-repo (`internal/testutil/fakesmsc`) — non requis à M0 (arrive à M2)

## 1. Objectif
Poser un dépôt qui build/teste/lint/scanne, démarre ses quatre dépendances (Postgres 18, Redis, Redpanda/Kafka, ClickHouse), applique ses migrations, et fournit les primitives transverses (config validée au boot, observabilité + serveur ops, type `Body` masquant, modèle d'erreur, squelette canonique de `main`). Aucun traitement métier : uniquement le socle sur lequel tous les jalons se greffent.

## 2. Contexte & références (source de vérité)
- Contrats touchés : `db/schema_passerelle_sms.sql` (dérivation de `migrations/0001_init.*.sql`) ; pas encore d'OpenAPI implémenté.
- Sections plan : §0 (boucle par tâche, convention STUB, DoD), §1 conventions transverses (§1.1 module `github.com/martialanouman/go-gateway` + Go, §1.2 bibliothèques imposées, §1.3 outillage, §1.4 ports, §1.5 health liveness/readiness, §1.6 topics), §4 (M0).
- Guides : `docs/guide-codage-go.md` §5 (squelette de `main`), §11 (invariant corps masqué), `docs/convention-style-go.md` §9 (`.golangci.yml`, `local-prefixes`), `docs/guide-ingenierie-passerelle-sms.md` §11.3 (mapping codes d'erreur).
- État existant sur lequel on greffe : dépôt vierge (`init go mod`, commit initial). M0 crée tout le socle.

## 3. Décomposition en tâches (livré — décrit au passé)

**000.1 — Module, Makefile, outillage, CI.**
- Livré : `go.mod` (module `github.com/martialanouman/go-gateway`, `go 1.26`), `Makefile` (cibles `tools`, `up`/`down`/`down-clean`, `migrate`/`migrate-clickhouse`, `build`, `run SVC=`, `generate`, `tidy`, `test`, `lint`, `fmt`, `vuln`, `check` ; versions d'outils épinglées : golangci-lint, sqlc, govulncheck), `.golangci.yml`, `sqlc.yaml`, workflows `.github/workflows/{ci,pr-title,release}.yml`.
- Points clés : les cibles Make sont exactement ce que la CI exécute (`make check` = lint + test + vuln). Versions d'outils épinglées pour éviter la dérive locale/CI.

**000.2 — Dépendances docker + migrations.**
- Livré : `docker-compose.yml` (`postgres:18-alpine`, `redis:7-alpine`, `redpandadata/redpanda:v24.2.18`, `clickhouse/clickhouse-server:24.8-alpine`, ports + volumes nommés `*-data`, `--wait` sur healthchecks) ; `migrations/0001_init.up.sql` + `.down.sql` (dérivées de `db/schema_passerelle_sms.sql`) ; `migrations/clickhouse/0001_cdr.{up,down}.sql` (une instruction par fichier, cf. contrainte splitter ClickHouse) ; runner `internal/storage/postgres/migrate.go` (golang-migrate, driver pgx) + `internal/storage/clickhouse/migrate.go` ; `cmd/migrate/main.go` (`-store postgres|clickhouse -dir …`).
- Tests : `internal/storage/postgres/migrate_internal_test.go` (source des migrations, up/down).

**000.3 — Config validée au boot.**
- Livré : `internal/config/config.go` (structs `Config` + `Load()` via `caarlos0/env/v11`, validation au boot).
- Tests : `internal/config/config_test.go` (valeurs par défaut, variables requises, échec de validation).
- Invariant : la validation au boot est le seul endroit toléré pour un `log.Fatal`.

**000.4 — Observabilité + serveur ops.**
- Livré : `internal/observability/logging.go` (slog JSON), `tracer.go`/`tracing.go` (init OTel, exporteur OTLP gRPC), `ops.go` (`NewOpsServer(readiness ...Check)` servant `/metrics`, `/healthz` liveness, `/readyz` readiness — port ops 9090, §1.4/§1.5).
- Tests : `logging_test.go`, `tracing_test.go`, `ops_test.go` (`/healthz` → 200 toujours ; `/readyz` → 503 si un check échoue).

**000.5 — Primitives `platform`.**
- Livré : `internal/platform/uuidx` (UUIDv7 via `google/uuid`), `internal/platform/e164` (`Normalize`, via `nyaruka/phonenumbers`), `internal/platform/msg/body.go` (**type `Body` masquant** : `String`/`MarshalJSON`/`LogValue` → `[REDACTED]`, `Reveal()` pour le clair), `internal/platform/errors` (sentinelles + type `Code` + table de mapping `code → (httpStatus, smppStatus)`, §11.3), plus `buildinfo`, `supervisor`, `encoding`.
- Tests : `uuidx_test.go`, `e164_test.go`, `body_test.go` (**invariant a** : slog JSON ET attribut de span ne révèlent jamais le clair), `errors_test.go`, `supervisor_test.go`.

**000.6 — Squelette canonique de `main`.**
- Livré : `cmd/router-svc/main.go` (`signal.NotifyContext`, `Load()` config, init observabilité, démarrage serveur ops, blocage jusqu'à SIGTERM) — gabarit de tous les `main`.
- Tests : `cmd/router-svc/main_test.go`.

## 4. Livrables détaillés (récap)
- Fichiers : `go.mod`/`go.sum`, `Makefile`, `.golangci.yml`, `sqlc.yaml`, `docker-compose.yml`, `.github/workflows/{ci,pr-title,release}.yml`.
- Migrations : `migrations/0001_init.{up,down}.sql`, `migrations/clickhouse/0001_cdr.{up,down}.sql` ; `cmd/migrate/main.go`.
- Packages : `internal/config`, `internal/observability`, `internal/platform/{uuidx,e164,msg,errors,buildinfo,supervisor,encoding}`, `internal/storage/{postgres,clickhouse}/migrate.go`.
- Service : `cmd/router-svc` (squelette). Aucun endpoint métier, aucun topic produit/consommé.

## 5. Nouvelles dépendances
Rappel : **avant tout `go get`, passer par `ctx7`** (version + API à jour). Introduites à M0 : `jackc/pgx/v5`, `golang-migrate/migrate/v4`, `google/uuid`, `nyaruka/phonenumbers`, `caarlos0/env/v11`, `go.opentelemetry.io/otel` (+ OTLP + sdk), `prometheus/client_golang`. Pas encore chi/huma/kafka/redis/clickhouse (arrivent M1/M2).

## 6. Hors périmètre (explicitement PAS fait ici)
Aucun traitement de message, aucun endpoint métier, aucune Admin API (M1), aucun accès Kafka/Redis/ClickHouse en écriture métier — seuls les checks readiness pingent les dépendances. La surcharge `huma.NewError` arrive à M1 (premier service HTTP). Le faux SMSC et le codec SMPP arrivent à M2.

## 7. Invariants & règles d'or applicables
- **Invariant (a)** posé ici : le type `Body` (`internal/platform/msg`) masque en `String`/`MarshalJSON`/`LogValue` ; test bloquant sur slog JSON + attribut de span.
- Règle d'or « JAMAIS le corps dans un log/span/label » : matérialisée par `Body`.
- Règle d'or « SQL toujours paramétré » : posée par le choix pgx/sqlc (schéma d'outillage).
- `context.Context` en 1er paramètre, aucune goroutine sans condition d'arrêt : appliqués dans le serveur ops et le squelette `main`.

## 8. Critères d'acceptation (tests)
- `docker compose up` démarre les 4 dépendances ; `make migrate` applique le schéma sans erreur.
- `make run SVC=router-svc` démarre ; `GET :9090/healthz` → 200 ; `GET :9090/readyz` → 200 quand les deps vitales répondent, 503 sinon.
- **Invariant (a)** vert : un test échoue si sérialiser une struct contenant un `Body` révèle le clair (log JSON slog ET attribut de span via exporteur de test).
- `make lint`, `go test -race ./...`, `make vuln` verts.

## 9. Definition of Done
gofmt/goimports • golangci-lint • `go test -race ./...` • govulncheck • critères couverts par tests • invariant (a) vert • godoc sur l'exporté • PR focalisée. Spécifique M0 : Makefile/CI cohérents (`make check` = ce que la CI exécute), versions d'outils épinglées.

## 10. Risques / points d'attention
- Migration ClickHouse = **une instruction par fichier** (le splitter multi-statement casse sur un `;` en commentaire) — mémoire projet `clickhouse-migration-one-statement`.
- sqlc doit schema-qualifier les tables (`control_plane.customers`) contre la migration PG18 — mémoire `sqlc-schema-qualification` (impacte M1, préparé ici).
- Tests d'intégration locaux : `DOCKER_HOST` doit pointer sur le socket OrbStack, sinon ils skippent — mémoire `testcontainers-orbstack-socket`.
- Le serveur ops (9090) ne doit JAMAIS être exposé publiquement ni apparaître dans un contrat OpenAPI (§1.4).
