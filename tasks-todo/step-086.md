# step-086 — Throttling adaptatif AIMD piloté par `ESME_RTHROTTLED`

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-084 · **Bloque :** —

## But
Faire réagir le débit sortant aux signaux du SMSC : un `submit_sm_resp = ESME_RTHROTTLED` réduit le rythme (multiplicative decrease), l'absence d'erreur le fait remonter (additive increase).

## Périmètre (ce que fait CETTE PR)
- `internal/connectorpool` : boucle de contrôle AIMD par connecteur qui module le débit effectif d'envoi (en amont du token-bucket connecteur de step-084).
- Lecture du `command_status` de `submit_sm_resp` (déjà géré comme rejet transitoire dans `connectorpool.go` — réutiliser `errTransientReject`).
- Métriques : débit courant, événements throttled (Prometheus).

## Points d'implémentation clés
- AIMD : décroissance multiplicative (ex. ×0.5) sur `ESME_RTHROTTLED`, croissance additive bornée par `throughput_limit_per_sec`.
- Le débit AIMD est un **plafond dynamique local au pod**, sans coordination multi-pod (hors périmètre M6, §10).
- Aucune goroutine sans condition d'arrêt ; horloge injectable pour tester.
- Ne conflate pas throttling et disjoncteur (M8) — l'AIMD ne coupe jamais le bind.

## Tests (écrits dans la même PR)
- Faux SMSC (`internal/testutil/fakesmsc`, réponse scriptable `Throttled`) : rafale de `ESME_RTHROTTLED` → le débit baisse ; retour à `OK` → il remonte (critère d'acceptation M6).
- `-race` sur la boucle de contrôle.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `ESME_RTHROTTLED` fait baisser puis remonter le débit

## Hors périmètre
Le disjoncteur / la bascule de trafic → M8. La coordination multi-pod du débit → au-delà de M6.
