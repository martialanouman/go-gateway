# step-062 — Étape pipeline : opt-out MT bloquant (union des portées)

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-061 · **Bloque :** —

## But
Activer l'étape STUB `pipeline.opt_out` : bloquer un MT si le destinataire est désabonné dans **au moins une** portée applicable.

## Périmètre (ce que fait CETTE PR)
- Remplacer le `stubStage(ctx, "pipeline.opt_out")` de `internal/pipeline/pipeline.go` par l'étape réelle (span conservé, ordre §6.1 figé).
- Vérifier l'**union** des portées : `platform` ∪ `customer` ∪ `smpp_account` ∪ `inbound_number` (via `internal/pipeline/optout`, step-061).
- Match → `errs.ErrRecipientOptedOut` (`recipient_opted_out`, `403`) → CDR `rejected`.

## Points d'implémentation clés
- Chemin chaud : Bloom d'abord (`MightBeSuppressed`), confirmation exacte seulement sur positif → coût minimal sur le cas majoritaire non suppressé.
- **Bloquante** et **jamais court-circuitée** par une route exacte (invariant b, testable M7).
- **Invariant (a)** : span/log portent le `code`, jamais le corps ni le MSISDN complet si la politique de logs l'exige (au moins jamais le corps).

## Tests (écrits dans la même PR)
- Bloqué si **une** portée matche (chaque portée testée) ; passe si aucune.
- `code=recipient_opted_out`, CDR `rejected`.
- Faux positif Bloom → confirmation base négative → message passe (pas de rejet erroné).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] STUB `pipeline.opt_out` remplacé, span conservé

## Hors périmètre
STOP côté MO (step-063) ; Admin opt-out (step-064) ; anti-spam (step-065+).
