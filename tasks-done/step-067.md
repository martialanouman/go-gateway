# step-067 — Admin anti-spam : *-antispam-rule

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-065 · **Bloque :** —

## But
Exposer la gestion des règles anti-spam via l'Admin API (CRUD), pour piloter le moteur des step-065/066.

## Périmètre (ce que fait CETTE PR)
- Handlers `adminapi` (opérations déjà déclarées `api/openapi-admin.yaml`) : `list-antispam-rules`, `create-antispam-rule`, `update-antispam-rule`, `delete-antispam-rule`.
- Repo `internal/controlplane` sur `control_plane.antispam_rules` (sqlc).
- Validation de `config_json` par `rule_type` (`velocity`/`content_blacklist`/`duplicate`/`reputation`) : regex compilables, seuils cohérents.
- Synchroniser `api/collections/admin-api.yaml`.

## Points d'implémentation clés
- `scope`/`scope_id` : contrainte `antispam_scope_ck` (global → `scope_id NULL`), à respecter à la création.
- Validation stricte à la création/màj : une regex non compilable est refusée (`400`), pas acceptée puis crashant le moteur.
- Après écriture, recharge à froid du moteur (hot reload M7 — documenter la latence).
- SQL paramétré (sqlc).

## Tests (écrits dans la même PR)
- Intégration PG : CRUD par portée ; règle globale vs compte/customer.
- Regex invalide refusée à la création.
- Contrat admin + synchro collection.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] collection admin synchronisée

## Hors périmètre
Moteur d'évaluation (step-065, step-066) ; hot reload (M7).
