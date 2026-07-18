# step-105 — `cmd/config-sync` + pub/sub d'invalidation d'instantané

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-104, step-080 · **Bloque :** step-106

## But
Propager les changements du plan de contrôle au plan de données : `config-sync` publie une notification sur changement, les services de routage reconstruisent l'instantané et l'échangent atomiquement.

## Périmètre (ce que fait CETTE PR)
- `cmd/config-sync/main.go` (service inexistant) : observe les changements de config (déclencheur admin → canal pub/sub Redis) et publie l'invalidation.
- `internal/config` (ou `internal/routing`) : abonné pub/sub Redis (socle step-080) qui appelle `BuildSnapshot` + `Swap` (step-104) sur notification.
- Port ops 9090 + `/healthz`/`/readyz` comme les autres services (§1.4).

## Points d'implémentation clés
- Un seul canal d'invalidation de config ; le canal `breaker:events` (§Appendix B) sera réutilisé pour le disjoncteur en M8 (step-123).
- Reconstruction **débattue/coalescée** : plusieurs notifications rapprochées → un seul rebuild (évite le thundering herd).
- Abonné = goroutine avec condition d'arrêt (`context`), jamais fuyante.
- Reconstruction idempotente ; un échec de rebuild garde l'ancien instantané servi (pas de downtime).

## Tests (écrits dans la même PR)
- Intégration Redis : une publication déclenche un rebuild + `Swap` ; la nouvelle route est visible.
- Deux notifications rapprochées → coalescées (un rebuild).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] rebuild sur notification ; ancien instantané conservé si le rebuild échoue

## Hors périmètre
Le hot reload spécifique des Bloom exact/suppressions → step-106.
