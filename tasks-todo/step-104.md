# step-104 — Instantané de routage immuable + pointeur atomique

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-105, step-113, step-123

## But
Transformer le `SnapshotResolver` de M2 (`internal/routing/snapshot.go`, chargé une fois) en **instantané immuable échangé par un pointeur atomique**, pour préparer le hot reload sans verrou ni downtime (guide de codage §5.1).

## Périmètre (ce que fait CETTE PR)
- `internal/routing/snapshot.go` : envelopper l'instantané dans un `atomic.Pointer[Snapshot]` ; les lecteurs (router, par message) lisent `Load()` lock-free ; un `Swap(newSnapshot)` remplace l'ensemble d'un coup.
- Séparer l'**instantané immuable** (routes déclaratives, exact, scripts compilés) d'une **surcouche mutable** distincte pour l'état volatil (charge, futur breaker) — pas de mutation en place de l'instantané.
- Constructeur `BuildSnapshot(ctx)` réutilisable par `config-sync` (step-105).

## Points d'implémentation clés
- **Immuabilité stricte** : aucune écriture dans un instantané publié ; toute évolution = nouvel instantané + `Swap`.
- Lecture per-message sans allocation ni verrou (chemin chaud 8 000/s).
- État volatil (least_loaded, breaker M8) **hors** de l'instantané → surcouche séparée lue à côté.

## Tests (écrits dans la même PR)
- `-race` : lectures concurrentes pendant un `Swap` → jamais d'instantané partiel, aucune course.
- Un `Swap` change la résolution observée par les lecteurs suivants ; les lectures en cours gardent l'ancien instantané cohérent.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] instantané immuable + `atomic.Pointer` ; lecture lock-free

## Hors périmètre
Le déclencheur pub/sub `config-sync` → step-105. Le hot reload des Bloom → step-106.
