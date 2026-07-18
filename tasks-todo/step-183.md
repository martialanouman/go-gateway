# step-183 — Gateway WS/SSE (coder/websocket) + stream-metrics

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-182 · **Bloque :** step-184

## But
Poser la gateway temps réel WebSocket/SSE (`coder/websocket`) alimentée par `metrics.stream`, et
livrer le premier flux : `stream-metrics`.

## Périmètre (ce que fait CETTE PR)
- Nouvelle dépendance `github.com/coder/websocket` (§1.2) — **`ctx7` avant `go get`**, puis `make tidy`.
- Gateway : consommateur `metrics.stream` → hub de fan-out → connexions WS/SSE.
- Endpoint `stream-metrics` (déclaré au contrat Admin, gardé par scope opérateur).
- Gestion du cycle de vie : ping/pong, timeouts, fermeture propre, backpressure par client lent.

## Points d'implémentation clés
- **`ctx7` obligatoire** pour `coder/websocket` (API `Accept`, `Read`/`Write`, `CloseRead`, options) —
  ne pas deviner l'API.
- Aucune goroutine sans condition d'arrêt ; `context.Context` en 1er paramètre (CLAUDE.md). Un client lent
  est déconnecté, jamais une fuite de goroutine ni un blocage du hub.
- Fraîcheur des métriques **< 5 s** (critère §15).
- Événements bornés uniquement, jamais le corps (invariant a).

## Tests (écrits dans la même PR)
- Un client WS reçoit les mises à jour poussées depuis `metrics.stream` (< 5 s).
- Client lent déconnecté sans fuite de goroutine (`go test -race`).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] fraîcheur < 5 s ; pas de goroutine sans arrêt ; `coder/websocket` figé via `ctx7`

## Hors périmètre
`stream-sessions` et `stream-billing-alerts` → step-184.
