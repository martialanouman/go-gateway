# step-041 — Mots-clés entrants : repo + Admin (CRUD)

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-040 · **Bloque :** step-045

## But
Gérer les mots-clés d'un numéro entrant **partagé** (routage MO vers le bon compte par mot-clé), via l'Admin API.

## Périmètre (ce que fait CETTE PR)
- Repo `internal/controlplane` sur `control_plane.inbound_keywords` : list/create/update/delete, scoping par `inbound_number_id`.
- Handlers `adminapi` : `list-inbound-keywords`, `create-inbound-keyword`, `update-inbound-keyword`, `delete-inbound-keyword` (déjà déclarés `api/openapi-admin.yaml`).
- Requêtes sqlc `internal/storage/postgres/queries/inbound_keywords.sql`.
- Synchroniser `api/collections/admin-api.yaml`.

## Points d'implémentation clés
- `match_type` ∈ `exact|prefix|regex`, `priority` ordonne l'évaluation (`inbound_keywords_lookup_idx`).
- Un mot-clé n'a de sens que sur un numéro **partagé** (`inbound_numbers.account_id IS NULL`) : valider ou documenter le cas.
- SQL paramétré (sqlc), FK `ON DELETE CASCADE` déjà au schéma.

## Tests (écrits dans la même PR)
- Intégration PG : CRUD + tri par `priority`.
- Contrat admin + synchro collection.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] collection admin synchronisée

## Hors périmètre
Application des mots-clés au runtime MO (step-045) ; détection STOP (M5, step-063).
