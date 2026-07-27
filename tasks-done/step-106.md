# step-106 — Hot reload des filtres de Bloom (exact + suppressions) sans downtime

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-101, step-105 · **Bloque :** —

## But
Recharger à chaud les filtres de Bloom (numéros exacts et suppressions/opt-out) sur notification `config-sync`, en échangeant le filtre par pointeur atomique — trafic continu, zéro downtime.

## Périmètre (ce que fait CETTE PR)
- `internal/routing/exact/bloom.go` (+ équivalent suppressions, chemin opt-out M5) : Bloom porté par un `atomic.Pointer`, reconstruit et `Swap` sur invalidation (step-105).
- Rebuild depuis `exact_routes` / `suppressions` (repos existants).
- Métrique : âge du dernier reload, taille du filtre.

## Points d'implémentation clés
- Le rebuild construit un **nouveau** filtre puis l'échange atomiquement — jamais de mutation en place pendant les lectures (chemin chaud).
- Cohérence Bloom↔Redis : le Bloom peut avoir un léger retard, mais un possible-hit reste confirmé par Redis (step-101) → pas de mauvais routage transitoire.
- Rebuild borné et coalescé (réutilise le déclencheur step-105).

## Tests (écrits dans la même PR)
- **Trafic continu pendant un reload** : goroutines de résolution en boucle pendant un `Swap` de Bloom → aucune erreur, aucune course (`-race`), pas de trou de routage (critère d'acceptation M7).
- Un numéro ajouté puis reload → devient un possible-hit ; supprimé puis reload → repli.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] échange de Bloom sans downtime sous trafic

## Hors périmètre
Autres surfaces de config (routes déclaratives, scripts) déjà couvertes par step-104/105.
