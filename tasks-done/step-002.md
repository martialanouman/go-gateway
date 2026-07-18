# Step 002 — M2 : Squelette vertical MT (REST → codec SMPP → faux SMSC → CDR)

> **Statut :** FAIT (jalon livré, cf. commit `8b2e6e1`)
> **Jalon (plan d'exécution) :** §6 — `docs/plan-execution-passerelle.md`. **Jalon le plus important** (point de bascule §17).
> **Dépend de :** M0, M1 · **Débloque :** l'architecture prouvée de bout en bout ; base de M3 → M12
> **Pair de test SMSC :** faux SMSC in-repo (`internal/testutil/fakesmsc`, `make fake-smsc`)

## 1. Objectif
Le walking skeleton : `POST /messages` → `mt.inbound` (Kafka) → `router-svc` (E.164 + route déclarative statique, reste STUB marqués) → `mt.routed` → `connector-pool-svc` → `submit_sm` vers le faux SMSC → CDR ClickHouse versionné → `GET /messages/{id}`. Dès que le test bout-en-bout passe, l'architecture double-protocole/un-pipeline est prouvée.

## 2. Contexte & références (source de vérité)
- Contrats touchés : `api/openapi-public.yaml` (`submit-messages`, `get-message`, `health`), `db/schema_passerelle_sms.sql` (appendice ClickHouse → `migrations/clickhouse/0001_cdr.*.sql`).
- Sections plan : §2 (schéma du squelette), §6 (M2), §1.6 (topics `mt.inbound`/`mt.routed`, clés de partition, en-têtes), §1.9 (auth clé API SHA-256), §1.10 (CDR `ReplacingMergeTree` versionné, une ligne par changement de statut), §0.3 (convention STUB : span émis, jamais silencieux).
- Ordre du pipeline figé (spec §6.1 / CLAUDE.md) : auth → ACK durable Kafka → E.164 → sender ID → opt-out → anti-spam → résolution route → encodage/segmentation → débit → réserve crédit → envoi SMSC → capture → CDR.
- État existant sur lequel on greffe : primitives M0 (`platform`, observabilité, config), repos + Admin M1 (`controlplane`, `credential`, `storage/postgres`, `auth`).

## 3. Décomposition en tâches (livré — décrit au passé)

**002.1 — Codec PDU SMPP v3.4.**
- Livré : `internal/smpp/` — `pdu.go`, `codec.go`, `smpp.go` (command_id/status), `tlv.go`, `udh.go`. Encode/décode `bind_*`, `submit_sm`/`_resp`, `deliver_sm`, `enquire_link`, `unbind` ; support TLV/UDH ; payload > 254 o. **Interne, sans lib externe.**
- Tests : `codec_test.go` (round-trip), `tlv_test.go`, `udh_test.go`, `fuzz_test.go`.

**002.2 — Faux SMSC in-repo (§1.8).**
- Livré : `internal/testutil/fakesmsc/` — `fakesmsc.go` (serveur SMPP via `internal/smpp`), `resp.go` (réponses scriptables `OK`/`Throttled`/`SysErr`/`Delay`), `dlr.go` (émission MO/DLR à la demande) ; `cmd/fake-smsc/main.go` + cible `make fake-smsc`.
- Tests : `fakesmsc_test.go`. Débloque les tests M2 → M7 (le vrai simulateur n'est requis qu'à M8).

**002.3 — Couche Kafka (data plane).**
- Livré : `internal/storage/kafka/` — `producer.go` (`acks=all`, idempotent), `consumer.go` (commit après traitement, at-least-once), `topics.go` (constantes §1.6 + en-têtes `trace_id`/`message_id`/`account_id`/`customer_id`/`fallback_chain`), `kafka.go`. Clé de partition : `mt.inbound` = hash compte, `mt.routed` = id de message logique.
- Tests : `kafka_integration_test.go` (testcontainers Redpanda), helper `internal/testutil/kafkatest`.

**002.4 — Sink CDR ClickHouse versionné (§1.10).**
- Livré : `internal/storage/clickhouse/` — `cdr.go` (`ReplacingMergeTree`, `Status` : `accepted`/`enroute`/`rejected`/`failed` avec rangs espacés, une ligne par changement de statut, lecture `argMax`/dernière version), `clickhouse.go`. Écriture `accepted` par `rest-api-svc` (asynchrone après ACK), `enroute` par `connector-pool-svc`.
- Tests : `cdr_integration_test.go` (testcontainers ClickHouse), helper `internal/testutil/chtest`.

**002.5 — Pipeline MT (router).**
- Livré : `internal/pipeline/` — `pipeline.go` (ordre figé §6.1 : E.164 + résolution déclarative statique implémentés ; étapes 2–4 sender_id/opt_out/anti_spam et 7–8 rate_limit/credit = **STUB marqués émettant leur span**), `envelope.go` (`InboundMT`/`RoutedMT`), `wire.go`. Chaque étape = un span (`pipeline.*`) ; le corps est un `msg.Body`, jamais dans un span (invariant a). `internal/routing/snapshot.go` (résolution sur instantané immuable). `internal/router/router.go` (consumer `mt.inbound` → pipeline → `mt.routed`).
- Tests : `pipeline_test.go`, `wire_test.go`, `internal/routing/snapshot_test.go`, `internal/router/router_test.go`.

**002.6 — REST API publique.**
- Livré : `cmd/rest-api-svc/main.go` + `internal/restapi/` — `api.go` (huma : `submit-messages` `POST /messages`, `get-message` `GET /messages/{id}`, `health` `GET /health` public §1.5), `auth.go` (clé API SHA-256 §1.9 → compte → client, contrôle `rest_enabled`), `messages.go`, `accepted.go` (écriture CDR `accepted` asynchrone), `health.go`, `deps.go`. Génération `message_id`/`trace_id` UUIDv7 à l'ingestion ; publication `mt.inbound` ; `202` **après** confirmation durable ; span racine ouvert à l'ingestion.
- Tests : `restapi_test.go`, `conformance_test.go` (contrat public).

**002.7 — Connector pool (voie sortante).**
- Livré : `cmd/connector-pool-svc/main.go` + `internal/connectorpool/` — `connectorpool.go` (consomme `mt.routed`), `bind.go` (`bind_pool_size=1`, `submit_sm` → faux SMSC, suit `submit_sm_resp`, écrit CDR `status=enroute`).
- Tests : `connectorpool_test.go`.

**002.8 — Bout-en-bout.**
- Livré : `internal/e2e/e2e_test.go` (testcontainers Postgres/Kafka/ClickHouse + `fakesmsc` : `POST /messages` → `202` → `GET /messages/{id}` = `accepted` immédiat puis `enroute`).

## 4. Livrables détaillés (récap)
- Endpoints publics (operationId) : `submit-messages` (`POST /messages`), `get-message` (`GET /messages/{id}`), `health` (`GET /health`).
- Topics Kafka : `mt.inbound` (clé = hash compte), `mt.routed` (clé = id message logique).
- Packages : `internal/smpp`, `internal/testutil/{fakesmsc,kafkatest,chtest,otelrec}`, `internal/storage/{kafka,clickhouse}`, `internal/pipeline`, `internal/routing`, `internal/router`, `internal/restapi`, `internal/connectorpool`.
- Services : `cmd/rest-api-svc` (8080 + 9090), `cmd/router-svc` (consumer + 9090), `cmd/connector-pool-svc` (client SMPP + 9090), `cmd/fake-smsc`.
- CDR : `migrations/clickhouse/0001_cdr.*.sql` (ReplacingMergeTree versionné).

## 5. Nouvelles dépendances
Rappel : **avant tout `go get`, passer par `ctx7`**. Introduites à M2 : `twmb/franz-go` (+ `pkg/kadm`, `pkg/kmsg`), `ClickHouse/clickhouse-go/v2`, modules testcontainers `redpanda`/`clickhouse`. Le **codec SMPP est interne** (aucune lib externe).

## 6. Hors périmètre (explicitement PAS fait ici)
Aucune étape de conformité active (sender ID/opt-out/anti-spam restent STUB — M5/M6) ; pas de segmentation (1 segment supposé — M6) ; pas de débit (M6) ni de crédit/facturation (M9) ; pas de SMPP **serveur** entrant (M3) ; pas de MO/DLR (M4) ; un seul bind sortant, pas de disjoncteur (M8). Endpoints publics restants (`list-messages`, `cancel-message`, `get-account`, `Idempotency-Key`) → M3 (nécessitent Redis + parité `cancel_sm`).

## 7. Invariants & règles d'or applicables
- **Invariant (a)** : le corps voyage en `msg.Body` (record value Kafka, jamais en en-tête ni span) ; test « ne logge pas le corps » couvre aussi chaque STUB.
- Règle d'or « ordre du pipeline figé » : §6.1 respecté ; le court-circuit route saute seulement la *résolution de route*, jamais la conformité (les STUB restent sur le chemin).
- Règle d'or « STUB jamais silencieux » : chaque étape non implémentée émet son span (`pipeline.sender_id`, `pipeline.opt_out`, `pipeline.anti_spam`, `pipeline.rate_limit`, `pipeline.credit`) avec commentaire `// STUB Mx …`.
- Règle d'or « `202` après écriture durable Kafka » : le CDR `accepted` est asynchrone, jamais bloquant pour le `202` ; producteur `acks=all` idempotent.
- Règle d'or « clé API SHA-256 déterministe indexée, temps constant » : `internal/restapi/auth.go` (§1.9).

## 8. Critères d'acceptation (tests)
- Bout-en-bout (`testcontainers` + `fakesmsc`) : `POST /messages` → `202` → `GET /messages/{id}` renvoie **immédiatement** `accepted` (pas de fenêtre de 404), puis `enroute` après envoi au faux SMSC.
- `trace_id`/`message_id` (UUIDv7) générés à l'ingestion, présents dans le CDR et les en-têtes Kafka.
- Le `202` n'a lieu qu'après écriture durable dans `mt.inbound` (test : faux SMSC coupé → le `202` sort quand même, le message reste en file).
- Un span par étape (STUB compris) émis, vérifiable via l'exporteur de test OTel (`internal/testutil/otelrec`).
- Round-trip du codec SMPP testé unitairement (+ fuzz).

## 9. Definition of Done
gofmt/goimports • golangci-lint • `go test -race ./...` • govulncheck • critères couverts par tests • invariant (a) vert (y compris STUB) • godoc sur l'exporté • PR focalisée. Spécifique M2 : e2e vert, spans par étape vérifiés, `acks=all` idempotent, CDR versionné (§1.10).

## 10. Risques / points d'attention
- Coordination multi-service : `rest-api-svc`, `router-svc`, `connector-pool-svc` partagent les constantes de topics/en-têtes (`internal/storage/kafka/topics.go`) — toute évolution y est centralisée, jamais en littéral.
- CDR versionné : la lecture doit prendre la dernière version (`argMax`) — un `GET /messages/{id}` naïf lisant plusieurs lignes renverrait un statut périmé.
- STUB : la tentation de « sauter » un STUB en optimisant casserait l'ordre figé et l'invariant (b) (testé à M7) — chaque STUB reste sur le chemin et émet son span.
- Faux SMSC : réponses scriptables, mais l'injection de pannes réaliste (disjoncteur/reroute) est reportée à M8 (vrai simulateur) — les tests de résilience restent `t.Skip("needs SMSC simulator — M8")`.
- Migration ClickHouse une-instruction-par-fichier (mémoire `clickhouse-migration-one-statement`) ; testcontainers via socket OrbStack (mémoire `testcontainers-orbstack-socket`).
