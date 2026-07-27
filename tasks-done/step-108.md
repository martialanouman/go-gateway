# step-108 — Runtime de script JS (goja) poolé, plafond d'instructions = garde primaire

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-107 · **Bloque :** step-110

## But
Exécuter un script de routage JavaScript de façon sûre et bornée : runtimes goja **poolés**, avec un **plafond d'instructions comme garde primaire** (le timeout mur et le plafond mémoire ne sont que des filets).

## Périmètre (ce que fait CETTE PR)
- `internal/routing/script/goja.go` : pool de VM goja réutilisables (`sync.Pool`), compilation du programme une fois (checksum), exécution avec :
  - **plafond d'instructions** (interruption déterministe quand le compteur dépasse `max_instructions`),
  - **timeout mur** (`Interrupt` via horloge),
  - **plafond mémoire** best-effort.

## Points d'implémentation clés
- **`ctx7` goja obligatoire** : mécanisme d'interruption (`vm.Interrupt`), compilation `goja.Compile`, isolation par VM. Ne pas deviner l'API.
- **Le plafond d'instructions est la garde primaire** (déterministe, reproductible) ; le timeout mur ne suffit pas seul (dépend de la charge). Un dépassement = erreur typée `ErrInstructionLimit`.
- Pas d'accès I/O/réseau/horloge réelle exposé au script (déterminisme + sécurité).
- VM jamais partagée entre deux exécutions concurrentes (goja n'est pas thread-safe) → pool.

## Tests (écrits dans la même PR)
- Script coûteux (boucle infinie) → interrompu par le plafond d'instructions, erreur typée, aucune fuite de goroutine.
- Script valide → `routeId` renvoyé ; pool réutilise les VM sous concurrence (`-race`).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] plafond d'instructions coupe avant le timeout mur ; goja figé via `ctx7`

## Hors périmètre
Le runtime Lua → step-109. Le contrat `resolveRoute` + résolution de scope + intégration pipeline → step-110.
