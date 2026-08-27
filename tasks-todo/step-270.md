# step-270 — Manifests deploy/ Kubernetes (Deployments, Services, HPA, PDB, probes)

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-260 · **Bloque :** step-410

## But
Fournir les manifests Kubernetes sous `deploy/` pour tous les services : Deployments, Services, HPA
(CPU + lag Kafka), PodDisruptionBudget, et probes `/healthz`/`/readyz`.

## Périmètre (ce que fait CETTE PR)
- `deploy/k8s/` : un Deployment + Service par service `cmd/` (rest-api, admin-api, smpp-server,
  connector-pool, router, mo-dlr-router, session-manager, billing, config-sync).
- HPA : CPU **et** lag de consommation Kafka pour les consumers (router, connector-pool, mo-dlr).
- PDB (drain gracieux, binds préservés — cf. step-260) ; probes `/healthz` (liveness) + `/readyz`
  (readiness) sur le port ops 9090.

## Points d'implémentation clés
- **Le nombre de partitions doit dominer le plafond de l'HPA, pas l'égaler** (mesuré en step-201e `D1`).
  Depuis step-201d le routeur ouvre **une goroutine par partition présente dans son lot de poll** : son
  parallélisme est donc plafonné par les partitions **assignées à son pod**, pas par celles du topic. Si
  l'HPA monte à autant de pods que `mt.inbound` a de partitions, chaque pod retombe à **une seule lane**
  — le fan-out disparaît à l'instant précis où la charge le réclame, et l'HPA continue de croître sans
  rien acheter.
  Le banc « routeur seul » mesure 5 842 msg/s à 1 lane et 27 856 à 16, **mais 1 741/s par lane** à 16 :
  le fan-out achète encore du débit à 16, et de moins en moins. Deux conséquences pour les manifests :
  ne pas sous-provisionner les partitions, et **ne pas extrapoler un dimensionnement de ces chiffres** —
  ils sont un majorant mesuré sur un portable, en un seul processus, pipeline partiellement bouché.
  Le dimensionnement chiffré appartient à step-280, sur environnement représentatif. Courbe et
  réserves : `test/load/README.md`, mesure du 08/08/2026 (step-201e).
- **Ports** (§1.4) : port métier + port ops 9090 par service ; le port ops **jamais exposé publiquement**.
- `/healthz` = liveness (échec → restart), `/readyz` = readiness (échec → retrait LB, pas de restart) —
  respecter la sémantique (§1.5) dans les probes.
- **La readiness de `billing-svc` est désormais testée (step-193c), et ce manifeste doit s'y conformer.**
  ClickHouse y est en lecture seule et **PAS** une dépendance de readiness : le reaper est un job
  périodique, donc une panne ClickHouse ne doit pas sortir billing-svc du load balancer.
  `TestClickHouseIsNotAReadinessDependency` sert un vrai `/readyz` et exige exactement
  `{redis, postgres}` — la probe `readinessProbe` de ce manifeste pointe sur cet endpoint et n'a donc
  rien à redéclarer, mais tout à ne pas contredire.
- PDB cohérent avec le drain SMPP (step-260) : ne pas évincer tous les binds d'un connecteur d'un coup.
- Secrets/certs (step-300) et config OIDC (step-310) injectés via Secrets/ConfigMaps, jamais en clair.
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
Checklist de mise en production → step-410. Dashboards Grafana/règles Alertmanager (infra).
