# Step 001 — M1 : Plan de contrôle minimal + Admin API (noyau)

> **Statut :** FAIT (jalon livré, cf. commit `22ce77b`)
> **Jalon (plan d'exécution) :** §5 — `docs/plan-execution-passerelle.md`
> **Dépend de :** M0 · **Débloque :** de quoi provisionner un envoi (client → compte → identifiants → connecteur → route), prérequis de M2
> **Pair de test SMSC :** faux SMSC in-repo (`internal/testutil/fakesmsc`) — non requis à M1

## 1. Objectif
Provisionner via l'API le minimum pour envoyer : `customer` → `smpp_account` → `credential` → `smsc_connector` → `route` statique. Livrer le noyau de l'Admin API (chi + huma), les repositories Postgres (sqlc), le hachage des secrets selon §1.9, le modèle d'erreur plat, et un test de contrat qui verrouille l'OpenAPI implémenté sur `api/openapi-admin.yaml`.

## 2. Contexte & références (source de vérité)
- Contrats touchés : `api/openapi-admin.yaml` (sous-ensemble d'operationId, cf. §3), `db/schema_passerelle_sms.sql` (tables `control_plane.*`), `api/collections/admin-api.yaml` (collection maintenue en phase).
- Sections plan : §5 (M1), §1.9 (schéma de hachage : argon2id/bcrypt pour le bind, SHA-256 déterministe indexé pour la clé API), §1.4 (port métier admin 8081), §0.4 (DoD).
- Guides : `docs/guide-ingenierie-passerelle-sms.md` §11 (modèle d'erreur `{code, message, errors[]}`), `docs/convention-style-go.md` §2 (interfaces côté consommateur).
- État existant sur lequel on greffe : primitives M0 (`internal/config`, `internal/observability`, `internal/platform/errors`), squelette `main`, migration `0001_init`.

## 3. Décomposition en tâches (livré — décrit au passé)

**001.1 — Modèle de domaine du plan de contrôle.**
- Livré : `internal/controlplane/` — `customer.go`, `account.go`, `credential.go`, `connector.go`, `route.go`, `senderid.go`, `principal.go`, `status.go` (statut effectif = min, cascade de suspension), `enums.go`, `page.go` + `cursor.go` (pagination par curseur), `doc.go` (dette connue documentée). Interfaces définies côté consommateur.
- Tests : `enums_test.go`, `status_test.go`, `cursor_test.go`.

**001.2 — Repositories Postgres (sqlc).**
- Livré : `internal/storage/postgres/queries/{customers,accounts,credentials,connectors,routes,sender_ids}.sql` + code généré `internal/storage/postgres/sqlcgen/` (`gen.go` + `//go:generate`), repos `customers.go`, `accounts.go`, `credentials.go`, `connectors.go`, `routes.go` (+ `route_targets`), `sender_ids.go`, `authn.go`, plus `pool.go`, `convert.go`, `pagination.go`, `pgerr.go` (mapping erreurs Postgres → conflits).
- Points clés : SQL toujours paramétré (sqlc) ; tables schema-qualifiées (`control_plane.*`, cf. mémoire `sqlc-schema-qualification`) ; UPDATE partiels en `COALESCE(narg, col)`.
- Tests : `*_integration_test.go` par repo (testcontainers Postgres), dont `accounts_provisioning_integration_test.go` (cardinalité), `authn_integration_test.go`, `pgerr_internal_test.go`.

**001.3 — Hachage des secrets (§1.9).**
- Livré : `internal/credential/credential.go` (génération clé API `sgw_<random>`, hachage SHA-256 déterministe indexé pour la clé API ; argon2id/bcrypt pour le mot de passe de bind ; comparaison temps constant `subtle.ConstantTimeCompare`).
- Tests : `credential_test.go` (secret révélé une fois, vérification, temps constant).

**001.4 — Modèle d'erreur plat + surcharge huma.**
- Livré : `internal/platform/errors/humaerr/humaerr.go` (surcharge `huma.NewError` → `{code, message, errors[]}` en `application/json`, `code` partagé avec les `command_status` SMPP et `cdr.error_code`).
- Tests : `humaerr_test.go`.

**001.5 — Auth opérateur (stub testé).**
- Livré : `internal/auth/` — `auth.go` (interface `TokenVerifier`, type `Principal`/`Scope` + `Has`), `static.go` (`StaticVerifier`), `middleware.go` (bearer, scopes câblés permissifs). OIDC/mTLS reporté à M12 derrière la même interface.
- Tests : `middleware_test.go`, `static_test.go`.

**001.6 — Service Admin API (chi + huma).**
- Livré : `cmd/admin-api-svc/main.go` + `internal/adminapi/` — `api.go` (montage huma), `customers.go`, `accounts.go`, `credentials.go`, `connectors.go`, `routes.go`, `sender_ids.go`, `deps.go`, `dto.go`, `doc.go`. Port métier 8081 (§1.4).
- operationId implémentés : `list/create/get/update/delete-customer` + `suspend-customer` ; `list-smpp-accounts`/`create/get/update/delete-smpp-account` + `set-account-channels` + `set-account-session-limits` ; `list/create-credential` + `update-credential-status`/`revoke-credential`/`rotate-credential` (secret renvoyé une fois) ; `list/create/get/update/delete-connector` ; `list/create/get/update/delete-route` ; `list/create/update/delete-sender-id`.
- Tests : `customers_test.go`, `accounts_test.go`, `credentials_test.go`, `connectors_test.go`, `routes_test.go`, `sender_ids_test.go` (fakes `fakes_test.go`), `cmd/admin-api-svc/main_test.go`.

**001.7 — Test de contrat + collection.**
- Livré : `internal/adminapi/contract_test.go` (l'OpenAPI généré par huma pour les opérations implémentées est comparé à `api/openapi-admin.yaml` — opérations, schémas, `Error` — échoue à la moindre dérive), `collection_test.go` (garde `api/collections/admin-api.yaml` en phase, cf. mémoire `admin-collection-sync`). Helper : `internal/platform/humaspec`.
- Note huma : v2.38+ auto-ajoute 500/default, injecte `$schema`, gotchas nullable/enum pour le test strict — mémoire `huma-contract-quirks`.

## 4. Livrables détaillés (récap)
- Endpoints (operationId) : cf. 001.6 — noyau customers/accounts/credentials/connectors/routes/sender-ids.
- Packages : `internal/controlplane`, `internal/storage/postgres` (+ `queries/`, `sqlcgen/`), `internal/credential`, `internal/auth`, `internal/adminapi`, `internal/platform/errors/humaerr`, `internal/platform/humaspec`.
- Service : `cmd/admin-api-svc` (port 8081 métier + 9090 ops).
- Contrats maintenus : `api/openapi-admin.yaml`, `api/collections/admin-api.yaml`.
- Aucun topic Kafka, aucun envoi de message.

## 5. Nouvelles dépendances
Rappel : **avant tout `go get`, passer par `ctx7`**. Introduites à M1 : `go-chi/chi/v5`, `danielgtaylor/huma/v2`, `sqlc` (outil de génération, épinglé dans le Makefile), `golang.org/x/crypto` (argon2/bcrypt). pgx déjà là (M0).

## 6. Hors périmètre (explicitement PAS fait ici)
Aucun envoi de message ; pas de routage dynamique/script, opt-out, anti-spam, facturation, contenu, métriques temps réel. Les endpoints Admin non listés (inbound-numbers → M4, suppressions/opt-out → M5, exact-routes/scripts → M7, connector-status/reconnect → M8, billing → M9, webhooks → M4, customer-groups) arrivent à leur jalon. Auth opérateur réelle (OIDC/mTLS) reportée à M12.

## 7. Invariants & règles d'or applicables
- **Invariant (a)** : aucun corps de message ne circule ici (plan de contrôle uniquement) ; conservé.
- Règle d'or « SQL toujours paramétré » : sqlc/pgx exclusivement, jamais de concaténation.
- Règle d'or « secrets hachés, révélés une fois, comparaison temps constant » : §1.9 respecté (`internal/credential`).
- Règle d'or « modèle d'erreur plat `{code, message, errors[]}` » : `humaerr` (surcharge `huma.NewError`).
- Règle d'or « les contrats sont la source de vérité » : test de contrat bloquant conforme l'implémentation à `api/openapi-admin.yaml`.

## 8. Critères d'acceptation (tests)
- Parcours complet : créer `customer` → `smpp_account` → `credential(api_key)` (secret renvoyé une seule fois, masqué ensuite) → `smsc_connector` → `route` statique.
- Cardinalité imposée par le schéma : 2ᵉ `smpp_bind` ou `api_key` sur un compte → `409` (`code=conflict`).
- Réponses d'erreur au format plat `{code, message, errors[]}`.
- Suspendre un client cascade sur ses comptes (statut effectif = min).
- Test de contrat vert (+ garde de collection).

## 9. Definition of Done
gofmt/goimports • golangci-lint • `go test -race ./...` • govulncheck • critères couverts par tests • aucun invariant violé • godoc sur l'exporté • PR focalisée. Spécifique M1 : test de contrat vert, collection `admin-api.yaml` en phase, secrets révélés une seule fois, commit un-par-PR (mémoire `commit-per-pr`).

## 10. Risques / points d'attention
- **Dette connue (documentée `internal/controlplane/doc.go`)** : null-clearing des champs nullable — `UPDATE` partiels en `COALESCE(narg, col)`, donc un `PATCH {champ: null}` sur un champ nullable est un no-op silencieux (tri-state absent/null/valeur reporté ; à traiter si aucun jalon ultérieur ne le résout).
- **Dette connue (`internal/storage/postgres/routes.go`)** : N+1 sur `list-routes`/`get-route` (route_targets lus par route hors transaction). Acceptable au débit du plan de contrôle ; à batcher (`WHERE route_id = ANY($1)`) au besoin.
- Gotchas huma (contract test strict) : 500/default auto-ajoutés, `$schema` injecté, nullable/enum — mémoire `huma-contract-quirks`.
- sqlc : schema-qualifier les tables contre PG18 (`control_plane.*`) — mémoire `sqlc-schema-qualification`.
