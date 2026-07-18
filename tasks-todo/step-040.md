# step-040 — Numéros entrants : repo + Admin (CRUD + assign)

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-045

## But
Gérer les numéros entrants (dédiés/partagés) via l'Admin API : socle de la résolution MO.

## Périmètre (ce que fait CETTE PR)
- Repo `internal/controlplane` sur `control_plane.inbound_numbers` (déjà au schéma) : list/create/update/delete + `assign` (pose `account_id` = dédié ; `NULL` = partagé).
- Handlers `adminapi` : `list-inbound-numbers`, `create-inbound-number`, `update-inbound-number`, `delete-inbound-number`, `assign-inbound-number` (opérations déjà déclarées `api/openapi-admin.yaml`).
- Requêtes sqlc `internal/storage/postgres/queries/inbound_numbers.sql`.
- Synchroniser `api/collections/admin-api.yaml` (test bloquant de collection).

## Points d'implémentation clés
- **Contrats source de vérité** : conformer aux schémas de `api/openapi-admin.yaml`, ne pas les modifier.
- SQL paramétré (sqlc), jamais de concaténation (règle d'or).
- Contrainte `inbound_numbers_uq (address, country_code)` : renvoyer un conflit propre (`409`) sur doublon.
- `number_type` ∈ `shortcode|longcode|alphanumeric` ; `connector_id` = lien qui délivre le MO.

## Tests (écrits dans la même PR)
- Intégration PG (testcontainers) : CRUD + `assign` (dédié↔partagé).
- Test de contrat admin (`internal/adminapi/contract_test.go`) couvre les nouvelles ops.
- Test de synchro collection.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `api/collections/admin-api.yaml` synchronisé

## Hors périmètre
Mots-clés entrants (step-041) ; résolution MO en runtime (step-045).
