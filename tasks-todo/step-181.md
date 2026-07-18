# step-181 — Spans complets par étape (pipeline.* / connector.*), 100 % sur erreur

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-185

## But
Compléter le traçage par étape : nommage stable `pipeline.*` / `connector.*`, échantillonnage à
100 % sur erreur/rejet/timeout, et **jamais le corps** dans un span (invariant a).

## Périmètre (ce que fait CETTE PR)
- `internal/pipeline`, `internal/connectorpool`, `internal/mo-dlr` (router MO/DLR) : consolider les
  spans par étape avec un nommage stable et des attributs bornés.
- Politique d'échantillonnage : forcer 100 % quand l'étape se solde par erreur/rejet/timeout.
- Audit des attributs de span : uniquement identifiants et codes, jamais le corps.

## Points d'implémentation clés
- Nommage **stable** (`pipeline.e164`, `pipeline.route`, `connector.submit`…) déjà amorcé
  (`internal/pipeline/pipeline.go`, `internal/connectorpool/connectorpool.go`) — figer la convention.
- **Jamais le corps dans un span/attribut/label** (CLAUDE.md, invariant a) — re-vérifier chaque
  `span.SetAttributes`. Le type `Body` masquant reste la barrière.
- 100 % sur erreur : décision d'échantillonnage côté SDK OTel (parent-based + record-on-error).
- **`ctx7`** avant toute API `go.opentelemetry.io/otel` (sampler, span options).

## Tests (écrits dans la même PR)
- Un rejet/erreur produit un span complet (recorder OTel de test).
- **Invariant (a)** : aucun span ne contient le corps (test de non-fuite sur les attributs).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (a)** re-vérifié sur les spans
- [ ] nommage de span stable ; 100 % sur erreur

## Hors périmètre
Assemblage de trace côté Admin (`get-message-trace`) → step-185.
