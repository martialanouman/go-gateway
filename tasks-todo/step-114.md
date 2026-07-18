# step-114 — Stratégies failover_priority, least_loaded + fallback_route

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-113 · **Bloque :** step-125

## But
Compléter les 6 stratégies (`failover_priority`, `least_loaded`) et le repli `fallback_route` au niveau route, pour un routage de production.

## Périmètre (ce que fait CETTE PR)
- `internal/routing/strategy/` : `failover_priority` (par `route_targets.priority`), `least_loaded` (lit `connectorload:{connector_id}` Redis, §Appendix B).
- `internal/routing` : chaînage `fallback_route_id` (`db/schema_passerelle_sms.sql` §10, self-FK) quand la route primaire n'a aucune cible retenue.
- `least_loaded` et l'état de charge vivent dans la **surcouche mutable**, jamais dans l'instantané immuable (step-104).

## Points d'implémentation clés
- **Hors périmètre M7** : l'indisponibilité par disjoncteur n'existe pas encore — toutes les cibles sont considérées disponibles (§11). `failover_priority` bascule ici sur absence de cible, pas sur un breaker (branché en M8, step-123/125).
- `least_loaded` lit une jauge Redis (pas de RMW Go) ; tolère l'absence de la clé.
- `fallback_route` = repli au niveau **route** (distinct du `fallback_chain` connecteur de M8).

## Tests (écrits dans la même PR)
- `failover_priority` : priorité 1 choisie ; sans cible → priorité 2.
- `least_loaded` : cible la moins chargée selon la jauge injectée.
- `fallback_route` : route primaire sans cible → route de repli.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] les 6 stratégies présentes ; `fallback_route` fonctionnel

## Hors périmètre
La bascule pilotée par disjoncteur + `fallback_chain` connecteur → M8 (step-123/125).
