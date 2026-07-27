# step-123 — Le routeur lit `breaker:state` à la (re)construction de l'instantané

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-122, step-105 · **Bloque :** step-125

## But
Rendre le routage conscient du disjoncteur : le routeur exclut les connecteurs `open` de la sélection, en lisant `breaker:state` **uniquement** à la (re)construction de l'instantané, déclenchée par `breaker:events`.

## Périmètre (ce que fait CETTE PR)
- `internal/routing` : la **surcouche mutable** (step-104) porte l'état de disponibilité par connecteur, lu depuis `breaker:state:{connector_id}`.
- `cmd/config-sync` (step-105) : s'abonner à `breaker:events` en plus du canal de config → reconstruction/rafraîchissement de la surcouche.
- Les stratégies `failover_priority`/`least_loaded` (step-114) écartent désormais les cibles `open`.

## Points d'implémentation clés
- **Le routeur lit le breaker à la (re)construction seulement** (§12), pas par message → chemin chaud non ralenti par un accès Redis.
- État de disponibilité dans la **surcouche mutable**, jamais dans l'instantané immuable (celui-ci reste figé).
- `half_open` = cible sondable avec trafic limité (laisser passer un filet).
- Un connecteur redevenu `closed` réintègre la sélection au rafraîchissement suivant.

## Tests (écrits dans la même PR)
- Connecteur passé `open` (via breaker) → écarté de la sélection après event ; redevenu `closed` → réintégré.
- Le chemin de résolution par message ne touche pas Redis (assertion sur les accès).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] lecture du breaker uniquement à la (re)construction ; surcouche mutable

## Hors périmètre
`fallback_chain` + reroute unilatéral → step-125. Le parking → step-126.
