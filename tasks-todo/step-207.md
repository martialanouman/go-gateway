# step-207 — Manifests deploy/ Kubernetes (Deployments, Services, HPA, PDB, probes)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-203 · **Bloque :** step-208

## But
Fournir les manifests Kubernetes sous `deploy/` pour tous les services : Deployments, Services, HPA
(CPU + lag Kafka), PodDisruptionBudget, et probes `/healthz`/`/readyz`.

## Périmètre (ce que fait CETTE PR)
- `deploy/k8s/` : un Deployment + Service par service `cmd/` (rest-api, admin-api, smpp-server,
  connector-pool, router, mo-dlr-router, session-manager, billing, config-sync).
- HPA : CPU **et** lag de consommation Kafka pour les consumers (router, connector-pool, mo-dlr).
- PDB (drain gracieux, binds préservés — cf. step-203) ; probes `/healthz` (liveness) + `/readyz`
  (readiness) sur le port ops 9090.

## Points d'implémentation clés
- **Ports** (§1.4) : port métier + port ops 9090 par service ; le port ops **jamais exposé publiquement**.
- `/healthz` = liveness (échec → restart), `/readyz` = readiness (échec → retrait LB, pas de restart) —
  respecter la sémantique (§1.5) dans les probes.
- PDB cohérent avec le drain SMPP (step-203) : ne pas évincer tous les binds d'un connecteur d'un coup.
- Secrets/certs (step-205) et config OIDC (step-206) injectés via Secrets/ConfigMaps, jamais en clair.
- Ce sont des artefacts de déploiement : pas de code Go, mais versionnés et revus.
- **Arbitrage reporté depuis step-181 — échantillonnage des traces.** L'échantillonnage 100 % sur erreur est
  aujourd'hui **in-process** (`observability.ErrorBiasedSampler` + `ErrorBiased`). Il tient la promesse de
  `TRACES_SAMPLER_ARG` sans aucun collecteur, mais il exporte un span en erreur **sans ses ancêtres** quand
  la trace n'était pas échantillonnée — un span orphelin, pas une trace partielle. Si un collecteur OTel est
  déployé ici, trancher : l'**échantillonnage de queue** côté collecteur donne des traces complètes et un
  coût nul dans la passerelle. Coût actuel mesuré : **nul au ratio 1.0** (528 o/op avec ou sans), +384 o et
  +120 ns par span seulement si l'exploitant baisse le ratio. Décision utilisateur du 2026-08-01 : garder
  l'in-process et revoir ici ; les deux peuvent coexister (à ratio 1.0 le code in-process est inerte).

## Tests (écrits dans la même PR)
- Validation statique des manifests (kubeconform/`kubectl --dry-run=client` ou lint YAML en CI).
- Cohérence des ports/probes avec §1.4/§1.5 (revue + check automatisable).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts (code inchangé)
- [ ] manifests validés statiquement · probes/ports conformes §1.4/§1.5 · PDB cohérent avec le drain
- [ ] port ops non exposé publiquement

## Hors périmètre
Checklist de mise en production → step-208. Dashboards Grafana/règles Alertmanager (infra).
