# step-102 — Admin exact-routes : list / create / update / delete / lookup

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-100 · **Bloque :** step-103

## But
Exposer la gestion des numéros exacts dans l'Admin API, conformément aux opérations déjà déclarées dans le contrat.

## Périmètre (ce que fait CETTE PR)
- `internal/adminapi/exact_routes.go` + DTO : handlers chi+huma pour `list-exact-routes`, `create-exact-route`, `update-exact-route`, `delete-exact-route`, `lookup-exact-route` (déjà dans `api/openapi-admin.yaml` L670-712).
- Étendre la surface d'opérations dans `internal/adminapi/contract_test.go` (liste `m1Operations` / surface autorisée) et `api/collections/admin-api.yaml` (test bloquant, mémoire projet).

## Points d'implémentation clés
- **Implémente pour conformer le contrat** `api/openapi-admin.yaml`, jamais l'inverse. Modèle d'erreur plat `{code,message,errors[]}`.
- `lookup-exact-route` = résolution ponctuelle par msisdn (debug opérateur).
- Validation E.164 du `msisdn` (réutiliser `internal/platform/e164`).
- **huma v2.38 quirks** (500/default auto, `$schema`) — voir mémoire projet.

## Tests (écrits dans la même PR)
- Test de contrat : les 5 opérations conforment le schéma.
- Handlers : create→get→delete round-trip ; lookup d'un numéro connu/inconnu.
- Collection admin re-synchronisée (test bloquant vert).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] surface contrat + collection admin à jour

## Hors périmètre
`import-exact-routes` (async) → step-103.
