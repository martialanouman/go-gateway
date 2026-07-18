# step-101 — Bloom en mémoire + `exactroute:{msisdn}` Redis + court-circuit L0

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-100, step-080 · **Bloque :** step-106, step-115

## But
Résoudre un numéro porté en O(1) : filtre de Bloom en mémoire (garde-fou négatif) puis lecture `exactroute:{msisdn}` Redis, en **court-circuit L0** avant la résolution déclarative — sans jamais sauter la conformité.

## Périmètre (ce que fait CETTE PR)
- `internal/routing/exact/bloom.go` : filtre `bits-and-blooms/bloom/v3` chargé depuis `exact_routes` ; `MightContain(msisdn)`.
- `internal/routing/exact/resolver.go` : sur possible-hit Bloom → lecture Redis `exactroute:{msisdn}` (§Appendix B) → `Target` ; miss → nil (repli sur déclaratif).
- Intégration dans le pipeline de `internal/router` : **L0 avant** la résolution de route déclarative (`internal/routing/snapshot.go`).

## Points d'implémentation clés
- **Ordre du pipeline figé** : L0 saute uniquement la *résolution de route*, **jamais** E.164, sender ID, opt-out, anti-spam, segmentation, débit (CLAUDE.md + invariant b).
- Bloom = filtre probabiliste : un possible-hit **doit** être confirmé par Redis (pas de faux positif routé) ; un miss Bloom est définitif (pas de lecture Redis).
- **API `bits-and-blooms/bloom/v3` via `ctx7`** (dimensionnement m/k, `TestString`/`AddString`).
- Le Bloom sera rechargé à chaud en step-106 ; ici il est chargé une fois au démarrage.

## Tests (écrits dans la même PR)
- Numéro porté présent → routé par exact_routes ; absent → repli déclaratif (scénario de portabilité, critère M7).
- Possible-hit Bloom sans entrée Redis → repli (pas de mauvais routage).
- Faux positif Bloom géré (test avec collision forcée).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] L0 saute la résolution de route mais aucune étape de conformité

## Hors périmètre
Hot reload du Bloom → step-106. Test complet invariant (b) → step-115.
