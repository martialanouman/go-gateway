# step-109 — Runtime de script Lua (gopher-lua) poolé, mêmes gardes

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-107 · **Bloque :** step-110

## But
Exécuter un script de routage Lua avec les mêmes garanties que le runtime JS : états gopher-lua poolés, plafond d'instructions primaire, timeout mur, plafond mémoire.

## Périmètre (ce que fait CETTE PR)
- `internal/routing/script/lua.go` : pool d'états `*lua.LState`, chargement du chunk une fois, exécution bornée.
- **Plafond d'instructions** via hook de comptage (`SetHook`/count), **timeout mur**, plafond mémoire.

## Points d'implémentation clés
- **`ctx7` gopher-lua obligatoire** : `NewState`, hooks de comptage d'instructions, `LState.Close`, sandboxing (retirer `os`/`io`). Ne pas deviner l'API.
- **Plafond d'instructions = garde primaire** (déterministe), même contrat d'erreur `ErrInstructionLimit` que goja (step-108).
- État `*lua.LState` jamais partagé entre exécutions concurrentes → pool ; fermeture propre au retour au pool.
- Aucun accès système exposé au script.

## Tests (écrits dans la même PR)
- Script en boucle → interrompu par le plafond d'instructions, erreur typée, aucune fuite d'état/goroutine.
- Script valide → `routeId` ; réutilisation sous concurrence (`-race`).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] plafond d'instructions primaire ; gopher-lua figé via `ctx7`

## Hors périmètre
Contrat `resolveRoute` unifié JS/Lua + résolution de scope + intégration pipeline → step-110.
