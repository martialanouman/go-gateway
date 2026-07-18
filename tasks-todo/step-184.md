# step-184 — Flux stream-sessions + stream-billing-alerts

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-183, step-143 · **Bloque :** —

## But
Ajouter à la gateway temps réel deux flux : `stream-sessions` (états de bind SMPP) et
`stream-billing-alerts` (alertes de solde bas / plancher MO / disjoncteur ouvert).

## Périmètre (ce que fait CETTE PR)
- `stream-sessions` : diffusion des changements d'état de session (registre Redis / session-manager).
- `stream-billing-alerts` : diffusion des événements d'alerte émis par le cœur billing (step-143 :
  plancher MO ; solde bas ; disjoncteur ouvert de M8).
- Endpoints déclarés au contrat Admin, gardés par scope, réutilisant le hub de step-183.

## Points d'implémentation clés
- Réutiliser l'infra WS de step-183 (hub, cycle de vie, backpressure) — pas de nouvelle gateway.
- Les alertes billing proviennent des événements **émis** en step-143 (couplage par `metrics.stream` ou
  un canal d'alerte dédié) — pas de nouvelle logique de décision billing ici.
- Une alerte doit **se déclencher** de bout en bout : solde bas → événement → WS (critère §15).
- Labels/évts bornés, jamais le corps (invariant a).

## Tests (écrits dans la même PR)
- `stream-sessions` pousse un changement d'état de bind.
- `stream-billing-alerts` : un solde bas / plancher MO déclenche une alerte reçue côté WS.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] une alerte se déclenche bout-en-bout (test)

## Hors périmètre
Trace/recherche/export → step-185/186/187. Règles Alertmanager (infra, §15 hors périmètre).
