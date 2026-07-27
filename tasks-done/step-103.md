# step-103 — Admin import-exact-routes (asynchrone)

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-102 · **Bloque :** —

## But
Importer en masse des numéros portés (feed MNP/opérateur) sans bloquer l'appelant : l'endpoint accepte le lot et renvoie un identifiant de job.

## Périmètre (ce que fait CETTE PR)
- `internal/adminapi/exact_routes.go` : handler `import-exact-routes` (`api/openapi-admin.yaml` L686) → accepte le lot, renvoie `202` + `job_id`, traite en arrière-plan via `BulkUpsert` (step-100, `CopyFrom`).
- Suivi minimal du job (statut en cours/terminé/erreurs) ; source = `mnp_import|carrier_feed`.
- Étendre surface contrat + collection admin.

## Points d'implémentation clés
- Traitement borné (worker avec condition d'arrêt via `context`), pas de goroutine fuyante.
- Import idempotent (upsert par `msisdn`) → un rejeu ne duplique pas.
- Réponse asynchrone : ne jamais tenir la connexion HTTP pendant l'import complet.

## Tests (écrits dans la même PR)
- Import d'un lot → `202` + job ; les lignes apparaissent via `list-exact-routes`.
- Rejeu du même lot → pas de doublon.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] import non bloquant + idempotent

## Hors périmètre
Le rechargement à chaud du Bloom après import massif → step-106.
