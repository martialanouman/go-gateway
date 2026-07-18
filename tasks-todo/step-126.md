# step-126 — Draineur borné + `mt.reroute-park` (rafales de reroute)

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-125 · **Bloque :** —

## But
Absorber une rafale de reroutes (panne massive d'un connecteur) sans écrouler la cible de repli : parquer l'excédent durablement dans `mt.reroute-park` et le rejouer à débit contrôlé.

## Périmètre (ce que fait CETTE PR)
- `internal/connectorpool` : quand le reroute (step-125) dépasse un seuil, publier l'excédent sur `mt.reroute-park` (§Appendix B / §6.15) au lieu de saturer la cible.
- Draineur **borné** : consomme `mt.reroute-park` à débit limité (réutilise le token-bucket M6, step-084) et réinjecte dans le flux d'envoi.

## Points d'implémentation clés
- **Parking durable** (topic Kafka) → aucun message perdu lors d'une bascule massive.
- Draineur = worker à débit contrôlé, condition d'arrêt via `context`, jamais fuyant.
- Le rejeu respecte l'ordre des segments d'un message (même `message_key`).
- Ne jamais rejouer plus vite que le plafond de la cible de repli (protection connecteur, M6).

## Tests (écrits dans la même PR)
- Rafale au-delà du seuil → excédent parqué ; le draineur rejoue à débit borné ; tout finit envoyé.
- `-race` sur le draineur.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] excédent parqué durablement puis rejoué à débit contrôlé

## Hors périmètre
Dead-letter après épuisement (échec définitif) → step-129.
