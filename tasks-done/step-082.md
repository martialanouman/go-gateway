# step-082 — Découper les messages longs en segments UDH (étape pipeline)

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-081 · **Bloque :** step-085

## But
Ajouter au pipeline MT une étape de **segmentation** qui découpe un message long en N segments concaténés (UDH), avant le débit et le futur crédit — respect de l'ordre figé du pipeline.

## Périmètre (ce que fait CETTE PR)
- `internal/pipeline/encoding/segment.go` : `Split(body, encoding) [][]byte` produisant les charges utiles par segment, en s'appuyant sur `smpp.EncodeConcatUDH` (`internal/smpp/udh.go`, déjà livré).
- Insertion de l'étape dans `internal/pipeline/pipeline.go` / `internal/router` : renseigne `RoutedMT.SegmentCount` et prépare les segments ; tous les segments partagent le **`message_key` logique** (§1.6) → même partition `mt.routed`, même bind.
- Émettre un **span** `pipeline.segment` (STUB-compatible, jamais le corps).

## Points d'implémentation clés
- Position dans l'ordre §6.1 : **segmentation précède débit** (critère d'acceptation M6). Ne réordonne aucune étape de conformité.
- Référence de concaténation 16 bits par défaut (`smpp.Concat.Ref16`) pour limiter les collisions à haut volume.
- Un message court reste 1 segment (`SegmentCount=1`) — pas de régression M2.
- Le corps segmenté ne sort du `Reveal()` que le temps de l'encodage ; aucun segment n'est loggé (invariant a).

## Tests (écrits dans la même PR)
- Un message de 161 caractères GSM-7 → 2 segments de 153/8 ; UDH bien formé (round-trip via `smpp.ParseUDH`).
- `SegmentCount` cohérent avec `encoding.DetectAndCount` (step-081).
- Test « ne logge pas le corps » couvre la nouvelle étape.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] segmentation strictement avant l'étape débit dans le pipeline

## Hors périmètre
Le débit (token-bucket) → step-084/085. Le réassemblage MO → step-083.
