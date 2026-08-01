# step-193b — Appliquer le patron de câblage aux mains restantes

> **Jalon :** Audit pré-production (structure) · **Statut :** À FAIRE
> **Dépend de :** step-193 · **Bloque :** step-206

## But
Étendre aux trois mains restantes le patron de constructeurs testables validé en step-193 :
`cmd/mo-dlr-router-svc` (**235 lignes**), `cmd/admin-api-svc` (**234**), `cmd/smpp-server-svc` (**196**).
`admin-api-svc` est le plus urgent des trois : step-206 y remplacera le stub d'auth M1 par de l'OIDC/mTLS,
et cette bascule doit atterrir dans un câblage testable.

## Périmètre (ce que fait CETTE PR)
- Même découpage qu'en step-193, appliqué aux trois services.
- Tests de câblage pour chacun.

## Points d'implémentation clés
- **Suivre le patron de step-193, ne pas en inventer un second.** Si un service résiste au moule, c'est un
  signal à remonter — pas une invitation à diverger. Trois câblages qui se ressemblent valent mieux que
  trois variantes « optimisées » localement.
- Refactoring à comportement constant, comme en step-193 : même ordre d'initialisation, mêmes logs.
- `smpp-server-svc` touche le listener modifié par step-191 : rebaser plutôt que résoudre un conflit à la main.
- Ne pas anticiper le câblage OIDC de step-206 — se contenter de rendre `admin-api-svc` prêt à le recevoir.

## Tests (écrits dans la même PR)
- Pour chacun des trois services : le graphe se construit avec des dépendances de test sans écoute réseau.
- Pour chacun : une dépendance indisponible produit une erreur enveloppée, pas une sortie de processus.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] **aucun changement de comportement** sur les trois services
- [ ] les 5 services du plan de données/contrôle suivent le même patron de câblage

## Hors périmètre
`billing-svc`, `content-key-svc`, `session-manager-svc` et `config-sync` : mains déjà courtes, à aligner au
fil de l'eau si elles grossissent. Auth OIDC → step-206.
