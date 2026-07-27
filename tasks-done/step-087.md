# step-087 — Limite de débit dédiée pour `query_sm`

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-084 · **Bloque :** —

## But
Protéger le SMSC d'un abus de `query_sm` (interrogation d'état) avec un limiteur distinct du débit `submit_sm`, comme exigé au §6.22.

## Périmètre (ce que fait CETTE PR)
- `internal/connectorpool` : chemin `query_sm` gouverné par un token-bucket séparé (réutilise `internal/pipeline/ratelimit`, socle step-084) sur une clé `ratelimit:...:query_sm`.
- Dépassement → l'appel `query_sm` est différé/refusé, jamais bloquant pour le pipeline `submit_sm`.

## Points d'implémentation clés
- Compteur **distinct** de celui de `submit_sm` : un `query_sm` intensif ne doit pas consommer le budget d'envoi.
- Même atomicité Lua/fail-closed que step-084.
- Métrique dédiée.

## Tests (écrits dans la même PR)
- Rafale de `query_sm` au-delà de la limite → refus, sans impact sur le débit `submit_sm` (buckets indépendants).
- `-race`.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] budget `query_sm` isolé du budget `submit_sm`

## Hors périmètre
La corrélation DLR via `query_sm` polling (relève de §1.11/M4).
