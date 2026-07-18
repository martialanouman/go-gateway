# step-110 — Contrat `resolveRoute`, résolution de scope, intégration pipeline (repli déclaratif)

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-108, step-109 · **Bloque :** step-112, step-115

## But
Unifier les deux runtimes derrière le contrat `resolveRoute(message) → routeId | null`, résoudre le script applicable par scope (`account → customer → platform`), et brancher l'étape « script » dans la résolution de route avec repli déclaratif.

## Périmètre (ce que fait CETTE PR)
- `internal/routing/script/resolve.go` : interface commune `Resolver.Resolve(ctx, msg) (routeID *uuid.UUID, err error)` implémentée par goja (step-108) et gopher-lua (step-109) ; sélection du script actif par scope.
- Intégration dans `internal/routing` : étape script placée **entre** exact (L0, step-101) et déclaratif (`snapshot.go`) — ordre §6.1.
- `null` → repli déclaratif ; **dépassement du plafond d'instructions / erreur** → repli déclaratif **+ log + métrique** (jamais de message perdu).

## Points d'implémentation clés
- Contexte passé au script = message **sans corps loggable** (invariant a) ; le script voit les métadonnées de routage, pas de fuite en log.
- Résolution de scope : le premier script `active` trouvé en remontant `account → customer → platform` gagne (comme anti-spam/opt-out).
- Le script s'exécute sur l'instantané immuable (compilé dans le snapshot, step-104) → pas de recompilation par message.

## Tests (écrits dans la même PR)
- Script renvoyant un `routeId` valide → routé par script ; renvoyant `null` → repli déclaratif.
- Script dépassant le plafond d'instructions → repli déclaratif + log + métrique incrémentée (critère d'acceptation M7).
- Résolution de scope : script compte prime sur script customer/platform.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] repli déclaratif sur `null` et sur dépassement du plafond

## Hors périmètre
Le cycle Admin validate/test/publish → step-112. Les stratégies déclaratives → step-113/114.
