# step-185 — get-message-trace : trace complète d'un message, sans aucun corps

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-181 · **Bloque :** —

## But
Exposer côté Admin la trace complète du cheminement d'un message (étapes du pipeline, envoi,
DLR/statuts) via `get-message-trace`, **sans jamais aucun corps** (invariant a).

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` + `internal/adminapi` : `get-message-trace` (par `message_id`/`trace_id`).
- Assemblage : statuts CDR versionnés (ClickHouse) + jalons de span (`trace_id`) en une vue ordonnée.
- Collection Admin synchronisée.

## Points d'implémentation clés
- Corrélation par `trace_id` (UUIDv7 à l'ingress, §1.10/§6.11) : le CDR le porte, la trace OTel aussi.
- **Aucun corps** dans la réponse (critère §15, invariant a) : seulement étapes, codes, horodatages,
  identifiants, statut.
- Lecture CDR par dernière version (`argMax`/`FINAL`, §1.10).
- Scope opérateur requis ; MSISDN soumis au masquage par rôle (cohérent avec step-186).

## Tests (écrits dans la même PR)
- Trace complète d'un message : toutes les étapes présentes, ordonnées.
- **Invariant (a)** : aucun corps dans la réponse (test de non-fuite).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** re-testé sur la réponse
- [ ] collection synchronisée

## Hors périmètre
Recherche → step-186. Export → step-187.
