# step-107 — Repo `routing_scripts` + cycle de statut (draft/active/disabled)

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-108, step-109, step-110, step-111

## But
Persister les scripts de routage et leur cycle de vie, socle des runtimes goja/gopher-lua et de l'Admin.

## Périmètre (ce que fait CETTE PR)
- `internal/storage/postgres/queries/routing_scripts.sql` (sqlc) + `internal/storage/postgres/routing_scripts.go` : CRUD, list-versions par `(scope, scope_id)`, transition de statut respectant l'unicité « un seul actif ».
- `internal/routing/script/` (naissance du paquet) : types `Script{Scope, Language(js|lua), Source, Checksum, TimeoutMs, MaxInstructions, MaxMemoryKB, Status}` mappant `db/schema_passerelle_sms.sql` §12.

## Points d'implémentation clés
- Contrat : `routing_scripts_one_active_idx` (au plus un `active` par `(scope, scope_id)`, `NULLS NOT DISTINCT` pour `platform`) — la publication doit respecter cet index (transaction).
- `timeout_ms` borné à ≤ 20 (CHECK schéma) ; `checksum` calculé à l'écriture.
- SQL paramétré, schéma-qualifier `control_plane.routing_scripts` (mémoire projet sqlc).

## Tests (écrits dans la même PR)
- Intégration Postgres : create draft → publish (devient active) → publier un 2e viole/relègue l'ancien selon la règle d'unicité.
- list-versions renvoie l'historique par scope.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] unicité « un seul actif par scope » respectée

## Hors périmètre
Les runtimes d'exécution → step-108/109. Le contrat `resolveRoute` + intégration → step-110. L'Admin → step-111/112.
