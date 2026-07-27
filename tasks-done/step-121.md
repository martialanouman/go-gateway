# step-121 — Disjoncteur par connecteur : machine à états locale

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-122

## But
Implémenter la machine à états du disjoncteur (`closed → open → half_open → closed`) par connecteur, alimentée par les résultats de `submit_sm`, d'abord en local (un pod).

## Périmètre (ce que fait CETTE PR)
- `internal/connector/breaker/` (naissance du paquet) : `State` (closed|open|half_open), transitions, seuils (taux d'échec, fenêtre, cooldown avant half_open, quota de sondes half_open).
- Alimentée par les issues d'envoi de `internal/connectorpool` (succès / `ESME_R*` terminaux vs transitoires).

## Points d'implémentation clés
- **Distinct du throttling AIMD** (M6, step-086) et du **`link_status`** (M8, step-127) : le breaker traduit la *santé applicative* du connecteur, pas l'état du lien TCP.
- Transitions déterministes, horloge injectable ; pas de goroutine sans condition d'arrêt.
- Local ici ; l'agrégation multi-pod arrive en step-122.

## Tests (écrits dans la même PR)
- Rafale d'échecs → `open` ; après cooldown → `half_open` ; sonde OK → `closed` ; sonde KO → `open`.
- Table-driven des seuils, `-race`.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] machine à états correcte et déterministe (horloge injectée)

## Hors périmètre
Agrégation multi-pod par majorité → step-122. Consommation par le routeur → step-123.
