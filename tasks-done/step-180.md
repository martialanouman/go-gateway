# step-180 — Catalogue de métriques à labels BORNÉS + test de garde de cardinalité

> **Jalon :** M11 (§15 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-182, step-184

## But
Définir le catalogue de métriques Prometheus de la passerelle, avec des labels **strictement bornés**
(compte/connecteur/route/statut), et un **test de garde** qui échoue si un label à cardinalité non
bornée (MSISDN, message_id) est introduit.

## Périmètre (ce que fait CETTE PR)
- `internal/observability/metrics/` : vecteurs de métriques — latence d'ingestion, latence bout-en-bout,
  profondeur de file, `breaker_state`, timeouts de script, fraîcheur du cache de solde.
- Enregistrement sur le registre Prometheus existant (`observability.OpsServer.Registry()`).
- Test de garde : liste blanche des labels autorisés ; échec si un label hors liste apparaît.

## Points d'implémentation clés
- **Labels bornés uniquement** (§15, critère) : `customer_id`, `connector_id`, `route_id`, `status`,
  `breaker_state`… Jamais `msisdn`/`message_id`/corps → cardinalité explosive + fuite (invariant a).
- Le test de garde est **bloquant à vie** : il échoue si quelqu'un ajoute un label MSISDN/message_id.
- Réutiliser le registre existant (`internal/observability/ops.go` expose `Registry()`), ne pas en créer un autre.
- **`ctx7`** avant toute API `prometheus/client_golang` (HistogramVec, buckets natifs, exemplars).

## Tests (écrits dans la même PR)
- Chaque métrique s'enregistre et s'expose sur `/metrics`.
- **Test de garde** : injecter un label non borné → le test échoue (prouve la garde).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] garde de cardinalité bloquante ; aucun label non borné

## Hors périmètre
Émission depuis le pipeline/connector → step-182. Spans → step-181. Dashboards Grafana (infra).
