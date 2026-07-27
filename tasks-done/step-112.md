# step-112 — Admin assign / validate / test / publish routing-script

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-110, step-111 · **Bloque :** —

## But
Compléter le cycle de vie d'un script : l'affecter à un scope, le valider (compilation + gardes), le tester sur un message d'exemple, puis le publier (bascule `draft → active`).

## Périmètre (ce que fait CETTE PR)
- `internal/adminapi/routing_scripts.go` : handlers `assign-routing-script`, `validate-routing-script`, `test-routing-script`, `publish-routing-script` (`api/openapi-admin.yaml` L763-798).
- `validate` = compile via les runtimes (step-108/109) et vérifie les bornes ; `test` = exécute `resolveRoute` (step-110) sur un message fourni et renvoie le `routeId|null` + coût.
- `publish` = transition atomique respectant l'unicité « un seul actif par scope » (step-107) puis déclenche `config-sync` (step-105).

## Points d'implémentation clés
- `test-routing-script` exécute dans le **même bac à sable borné** que la prod (plafond d'instructions, timeout) — un script trop coûteux échoue au test, pas en prod.
- Le message de test ne contient pas de corps loggable (invariant a).
- Publish → invalidation d'instantané (le nouveau script actif entre en vigueur par hot reload).

## Tests (écrits dans la même PR)
- validate d'un script erroné → `422`/erreur plate ; valide → OK.
- test → renvoie `routeId` attendu ; script coûteux → dépassement signalé.
- publish → l'ancien actif est relégué, le nouveau devient actif (unicité).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] publish atomique + déclenche config-sync ; test exécute le même sandbox

## Hors périmètre
Les stratégies de distribution → step-113/114.
