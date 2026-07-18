# step-145 — Réserve MT dans le router (étape 8) ; désactivée = zéro appel réseau

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-144 · **Bloque :** step-146

## But
Remplacer le STUB `pipeline.credit` (étape 8, §6.1) par une vraie réservation de crédit MT via le
client billing, **sans aucune I/O réseau quand la facturation est désactivée** (contrôle booléen en
cache), en respectant l'ordre figé du pipeline.

## Périmètre (ce que fait CETTE PR)
- `internal/pipeline/pipeline.go` : l'étape 8 (`pipeline.credit`) appelle `Reserve` (client gRPC billing)
  au lieu du `stubStage`.
- Client billing côté router (interface consommateur, câblée dans `cmd/router-svc`).
- Court-circuit : si `billing_enabled=false` (booléen en cache), l'étape passe **sans** appel réseau.
- Solde insuffisant → rejet avec `insufficient_balance` (→ `402` REST) → CDR `rejected` (via `router.go`),
  **aucune** entrée de grand livre.

## Points d'implémentation clés
- **Ordre du pipeline figé** : la réserve reste l'étape 8, après débit (rate) et avant l'envoi ; ne
  jamais la déplacer ni la court-circuiter au titre de la conformité (§6.1, CLAUDE.md).
- **« Désactivée = zéro appel réseau »** : le flag `billing_enabled` est lu depuis un cache local
  (booléen), pas un aller-retour billing. C'est un critère d'acceptation testable.
- Le coût = nombre de segments (dépend de M6/segmentation) ; en absence de segmentation réelle, 1 segment.
- Le corps n'entre jamais dans l'appel billing (invariant a) : seulement `message_id`, owner, crédits.

## Tests (écrits dans la même PR)
- Prépayé : réserve OK → publication `mt.routed` ; solde insuffisant → `rejected` + zéro ledger.
- **Désactivée = zéro appel réseau** : test comptant les I/O du chemin chaud (mock billing, 0 appel).
- L'ordre du pipeline reste figé (les spans conformité précèdent toujours la réserve).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] test « zéro I/O réseau quand désactivée » bloquant

## Hors périmètre
Capture/libération (connector-pool) → step-146. Le calcul de segments réel est M6.
