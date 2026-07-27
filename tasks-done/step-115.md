# step-115 — Invariant (b) : un message routé L0 traverse toute la conformité

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-101, step-110 · **Bloque :** —

## But
Verrouiller l'**invariant (b)** par un test bloquant : un message court-circuité en L0 (numéro exact) passe quand même E.164, sender ID, opt-out, anti-spam, segmentation et débit — seule la *résolution de route* est sautée.

## Périmètre (ce que fait CETTE PR)
- `internal/router` (ou `internal/e2e`) : test avec **spies par étape** (chaque étape de conformité incrémente un compteur observable).
- Cas : message avec `exactroute:{msisdn}` présent → chemin L0 → assertion que chaque étape de conformité a bien été exécutée, et que la résolution de route déclarative/script a été sautée.

## Points d'implémentation clés
- C'est l'un des **4 invariants verts à vie** (CLAUDE.md §Les 4 invariants). Test bloquant, pas un réglage.
- Réutilise les étapes existantes (E.164 M2, conformité M5, segmentation step-082, débit step-085) et le court-circuit L0 (step-101).
- Aucune fuite de corps dans les spies (invariant a).

## Tests (écrits dans la même PR)
- L0 hit → E.164 ✓, sender ID ✓, opt-out ✓, anti-spam ✓, segmentation ✓, débit ✓, résolution de route ✗ (sautée).
- Un opt-out actif sur un numéro L0 → toujours bloqué (la conformité prime sur le court-circuit).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] invariant (b) prouvé par spies ; opt-out non contournable par L0

## Hors périmètre
Aucun — clôt le cœur de M7.
