# CLAUDE.md — Passerelle SMS (Go)

Manuel de travail pour Claude Code sur ce dépôt. Lis-le en entier avant d'écrire du code. Il est court volontairement : les détails vivent dans les guides et contrats référencés en bas.

## Ce qu'on construit

Une **passerelle SMS** en Go : elle reçoit des SMS (SMPP entrant + REST), les route vers des SMSC opérateurs (SMPP sortant), et gère la voie retour (MO/DLR). Double protocole sur **un pipeline unique**. Cible : agrégateur national, 8 000 SMS/s soutenu, 15 000 en pic. Ce n'est **pas** une plateforme de campagnes (pas de listes, modèles ni envoi programmé côté client).

## Commandes

```bash
make up          # docker-compose : Postgres 18, Redis/Dragonfly, Kafka, ClickHouse
make migrate     # applique migrations/ (dérivées de schema_passerelle_sms.sql)
make build       # compile tous les cmd/
make test        # go test -race ./...   (OBLIGATOIRE avant toute PR)
make lint        # golangci-lint (config .golangci.yml)
make run SVC=router-svc   # lance un service
make fake-smsc   # démarre le faux SMSC in-repo (pair de test — voir Tests)
```

## Architecture (carte mentale)

Trois plans : **contrôle** (config dans PostgreSQL, exposé par `admin-api-svc`, poussé au plan de données par `config-sync`), **données** (le traitement), **observabilité** (Prometheus/OTel/ClickHouse).

Services (`cmd/`) : `smpp-server-svc` (binds utilisateurs), `rest-api-svc` (HTTP), `router-svc` (pipeline MT), `connector-pool-svc` (envoi SMSC), `mo-dlr-router-svc` (retour), `session-manager-svc` (registre Redis), `billing-svc` (opt-in), `admin-api-svc`, `config-sync`.

Magasins : **PostgreSQL 18** = plan de contrôle + autorité des soldes ; **Redis/Dragonfly** = état opérationnel (sessions, débit, Bloom, cache de solde) ; **Kafka** = plan de données durable (`mt.inbound` → `mt.routed` → …) ; **ClickHouse** = CDR/analytique.

**Ordre du pipeline MT (NON réordonnable)** : auth → ACK durable Kafka → E.164 → autorisation sender ID → opt-out → anti-spam → résolution de route (numéro exact → script → déclaratif) → encodage/segmentation → débit → réservation crédit MT → envoi SMSC → capture/libère → CDR.

## Layout du dépôt

```
cmd/<service>/main.go   internal/smpp  internal/pipeline  internal/routing
internal/billing  internal/connector  internal/storage/{postgres,redis,kafka,clickhouse}
internal/config  internal/observability  internal/platform  api/  migrations/  deploy/
```
Tout le code métier vit sous `internal/`. Interfaces définies côté consommateur. Détail : `convention-style-go.md` §2.

## Règles d'or (toujours / jamais)

- **JAMAIS le corps d'un message dans un log, un span ou un label.** Invariant testable, pas un réglage. Utilise le type `Body` masquant (`Reveal()` pour le clair). Réf : guide de codage §11.
- **TOUJOURS `go test -race ./...` vert** avant une PR. Aucune goroutine sans condition d'arrêt. `context.Context` en 1er paramètre partout.
- **SQL toujours paramétré** (pgx/sqlc). Jamais de concaténation → faille d'injection.
- **Opérations Redis atomiques en Lua** (token-bucket, réserve/capture de crédit). Jamais un read-modify-write côté Go.
- **Ordre du pipeline figé.** Le court-circuit « numéro exact » saute la *résolution de route*, **jamais** la conformité (sender ID, opt-out, anti-spam).
- **Facturation idempotente par `message_id`** ; désactivée = zéro appel réseau (contrôle booléen en cache).
- **Secrets** (mots de passe bind, clés API) stockés en hash, révélés une seule fois à la création/rotation. Comparaison en temps constant.
- **Modèle d'erreur plat** `{ code, message, errors[] }` en `application/json` (surcharge `huma.NewError`). `code` = contrat partagé avec les `command_status` SMPP et `cdr.error_code`. Réf : guide d'ingénierie §11.
- **Les contrats sont la source de vérité** : implémente pour conformer `openapi-*.yaml` et `schema_passerelle_sms.sql`, jamais l'inverse.

## Les 4 invariants (tests bloquants, verts à vie)

a) le corps ne fuit dans aucune sérialisation ; b) un message routé par numéro exact traverse toutes les étapes de conformité ; c) la facturation est idempotente sous double livraison d'un même `message_id` ; d) `max_sessions` refuse le bind au-delà du quota.

## Tests

Pyramide : beaucoup d'unitaires (logique de domaine), des intégrations (`testcontainers-go` : Postgres/Redis/Kafka/ClickHouse), peu de bout-en-bout. Détail complet : `strategie-de-test-passerelle.md`.

> **Le simulateur SMSC (projet séparé) n'est pas encore prêt.** En attendant, utilise le **faux SMSC minimal in-repo** (`internal/testutil/fakesmsc`, `make fake-smsc`) comme pair SMPP pour les jalons M2→M7. Le vrai simulateur ne sera requis qu'à M8 (injection de pannes). N'écris aucun test qui dépende du simulateur avant M8.

## Definition of Done (chaque PR)

`gofmt`/`goimports` verts • `golangci-lint` sans alerte • `go test -race` vert • critères d'acceptation de la tâche couverts par des tests • aucun invariant violé • godoc sur l'exporté • PR petite et focalisée (une tâche du plan d'exécution).

## Recettes fréquentes

- **Ajouter un code d'erreur** : 3 endroits en même temps — `internal/platform/errors` (sentinelle + mapping HTTP/SMPP), le champ `code` des deux `api/openapi-*.yaml`, et la §11.3 du guide d'ingénierie.
- **Ajouter un endpoint Admin** : le déclarer d'abord dans `api/openapi-admin.yaml`, puis l'implémenter chi+huma pour conformer, puis test de contrat.
- **Changer le schéma** : éditer `db/schema_passerelle_sms.sql` **et** ajouter une migration `golang-migrate` correspondante dans `migrations/`.
- **Ajouter une étape de pipeline** : respecter l'ordre §6.1 ; émettre un span ; ne jamais y logger le corps.

## Index documentaire (source de vérité)

Contrats (référencés par le code, source de vérité) : `db/schema_passerelle_sms.sql`, `api/openapi-public.yaml`, `api/openapi-admin.yaml`. Le reste vit sous `docs/`.

- Quoi/pourquoi : `docs/specification-technique-passerelle-sms.md`
- Plan de construction : `docs/plan-execution-passerelle.md`
- Patterns Go : `docs/guide-codage-go.md` — Style Go : `docs/convention-style-go.md`
- Archi & exploitation : `docs/guide-ingenierie-passerelle-sms.md`
- Décisions : `docs/adr/` (ADR-0001…)
- Vocabulaire : `docs/glossaire-domaine-sms.md`
- Tests : `docs/strategie-de-test-passerelle.md`
- Pair de test SMSC : `docs/specification-technique-simulateur-smsc.md` (à venir)
- Consommateur de l'Admin API : `docs/specification-technique-tableau-de-bord.md`
