# step-122 — Agrégation multi-pod du disjoncteur par majorité (Redis)

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-121, step-080 · **Bloque :** step-123

## But
Agréger l'état du disjoncteur à travers tous les pods : chaque sous-bind publie son état, l'agrégat connecteur est calculé par **majorité**, et un changement est diffusé par pub/sub.

## Périmètre (ce que fait CETTE PR)
- `internal/connector/breaker/aggregate.go` : écrit l'état par `(pod_id, bind_index)` dans le HASH `breaker:binds:{connector_id}` (§Appendix B), calcule l'agrégat `breaker:state:{connector_id}` (closed|open|half_open) **par majorité**, et publie sur `breaker:events`.
- Agrégation atomique en **Lua** (socle step-080) : lecture du HASH + calcul + écriture de l'état dérivé en un script.
- TTL/expiration des sous-binds silencieux (un pod mort ne fige pas l'agrégat).

## Points d'implémentation clés
- **Majorité** : l'agrégat n'ouvre que si la majorité des sous-binds vivants est ouverte → un pod isolé ne coupe pas tout le connecteur.
- **Atomicité Lua**, pas de read-modify-write Go (règle d'or).
- Sous-binds expirés exclus du quorum (heartbeat + TTL).
- `breaker:events` = canal d'invalidation consommé par le routeur (step-123).

## Tests (écrits dans la même PR)
- Intégration Redis multi-« pods » simulés : 3 sous-binds, 2 ouverts → agrégat `open` ; 1 ouvert → reste `closed` (majorité).
- Sous-bind expiré exclu du calcul ; publication `breaker:events` émise au changement.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] agrégat correct par majorité, calculé atomiquement en Lua

## Hors périmètre
Lecture de l'agrégat par le routeur à la (re)construction de l'instantané → step-123.
