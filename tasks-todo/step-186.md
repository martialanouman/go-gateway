# step-186 — search-messages avec masquage MSISDN par rôle

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-185 · **Bloque :** step-187

## But
Offrir aux opérateurs la recherche de messages (par plage de dates, client, connecteur, statut…) avec
**masquage du MSISDN selon le rôle** et sans jamais exposer de corps.

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` + `internal/adminapi` : `search-messages` (filtres bornés, pagination).
- Lecture CDR (ClickHouse) par dernière version ; masquage MSISDN conditionné au rôle/scope.
- Collection Admin synchronisée.

## Points d'implémentation clés
- **Masquage MSISDN par rôle** (§15) : un rôle sans droit de dévoilement voit le MSISDN masqué ; la règle
  de masquage est partagée avec `get-message-trace` (step-185) et l'export (step-187).
- Aucun corps dans les résultats (invariant a).
- Requêtes bornées (row-cap, plage de dates obligatoire) pour ne pas balayer tout l'historique.
- **`ctx7`** avant toute API `clickhouse-go/v2` de requête paginée.

## Tests (écrits dans la même PR)
- Recherche filtrée renvoie les bons messages, paginés.
- MSISDN masqué pour un rôle restreint, dévoilé pour un rôle habilité.
- Aucun corps dans les résultats.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** respecté
- [ ] masquage MSISDN par rôle testé ; collection synchronisée

## Hors périmètre
Export asynchrone → step-187.
