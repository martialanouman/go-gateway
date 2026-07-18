# step-113 — Stratégies de distribution déterministes : round_robin, weighted, hash_based

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-104 · **Bloque :** step-114

## But
Ajouter la distribution multi-cibles pour une route non statique : répartir vers `route_targets` selon la stratégie choisie, de façon déterministe pour `weighted` et `hash_based`.

## Périmètre (ce que fait CETTE PR)
- `internal/routing/strategy/` (naissance du paquet) : `round_robin`, `weighted` (par `weight`), `hash_based` (par `hash(message_key) % Σ`), lisant `route_targets` (`db/schema_passerelle_sms.sql` §11).
- Intégration dans la résolution déclarative (`internal/routing`) : une route `distribution_strategy != static` choisit un connecteur cible via la stratégie.
- Les cibles compilées vivent dans l'instantané immuable (step-104).

## Points d'implémentation clés
- `hash_based` **déterministe** : même `message_key` → même cible (tous les segments d'un message vont au même connecteur, cohérent avec §1.6/§7.3).
- `weighted` déterministe et reproductible (distribution testable statistiquement).
- `round_robin` : compteur atomique dans la **surcouche mutable** (pas dans l'instantané immuable).

## Tests (écrits dans la même PR)
- `weighted` (70/30) sur N tirages → distribution attendue (tolérance) ; `hash_based` → mapping stable par clé.
- `round_robin` → rotation équilibrée.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `weighted`/`hash_based` déterministes (critère d'acceptation M7)

## Hors périmètre
`failover_priority`, `least_loaded`, `fallback_route` → step-114.
