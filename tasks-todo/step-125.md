# step-125 — `fallback_chain` en en-tête + reroute unilatéral

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-123, step-114 · **Bloque :** step-126

## But
Permettre à `connector-pool-svc` de rerouter un message vers le connecteur suivant de sa `fallback_chain` quand la cible se dégrade, sans repasser par le routeur (reroute unilatéral).

## Périmètre (ce que fait CETTE PR)
- `internal/pipeline` : ajouter `fallback_chain` (liste ordonnée de connecteurs) dans l'en-tête de l'enveloppe `mt.routed` — le routeur la calcule à partir de la route/stratégie (step-114).
- `internal/connectorpool` : sur breaker `open` / rejet terminal, republier le message vers le connecteur suivant de la chaîne (nouveau `shard_index` pour la nouvelle cible).
- Chaîne épuisée → `mt.dead-letter` (step-129).

## Points d'implémentation clés
- **Reroute unilatéral** : le connector-pool décide seul via la chaîne portée dans l'en-tête → pas d'aller-retour au routeur (latence + découplage).
- `fallback_chain` en **en-tête** (métadonnée de routage), jamais le corps (invariant a).
- Idempotence : un reroute ne duplique pas le CDR (nouvelle ligne `enroute` sur la nouvelle cible, versionnée §1.10).
- Distinct du `fallback_route` niveau route (M7) : ici c'est la chaîne connecteur, pilotée par le breaker.

## Tests (écrits dans la même PR)
- Cible primaire `open` → message rerouté vers le connecteur suivant de la chaîne (simulateur dégradé, step-120).
- Chaîne épuisée → dead-letter (préparé pour step-129).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] reroute via `fallback_chain` sans repasser par le routeur

## Hors périmètre
Le draineur borné + `mt.reroute-park` (rafales) → step-126.
