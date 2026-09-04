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

- **Dette reportée depuis step-260b — le drain n'a pas de plafond global.** `supervisor.Ordered.Run`
  draine ses composants en ordre inverse en attendant `<-dones[i]` **sans timeout d'ensemble** : un
  composant qui ignore son contexte bloque le drain jusqu'au `SIGKILL` du kubelet, et le pod meurt
  brutalement au lieu de partir proprement. Aucun composant actuel ne l'exhibe, c'est pourquoi rien ne
  l'a jamais montré. C'est ici que la dette mord, parce que c'est ici qu'on fixe le
  `terminationGracePeriodSeconds` : il doit rester **supérieur à `DRAIN_DELAY` + `SHUTDOWN_TIMEOUT`**,
  et cette arithmétique ne veut rien dire tant que le drain lui-même n'est pas borné. Trancher :
  plafonner le drain dans `supervisor`, ou l'assumer explicitement et dimensionner la grâce en
  conséquence.

## Tests (écrits dans la même PR)
- Validation statique des manifests (kubeconform/`kubectl --dry-run=client` ou lint YAML en CI).
- Cohérence des ports/probes avec §1.4/§1.5 (revue + check automatisable).

## Hérité de step-250e et step-260e — deux dimensionnements à porter, un levier à ne pas porter

Les revues de branche de step-250e et step-260e ont laissé trois grandeurs que les manifests devront
refléter, les deux premières une fois mesurées par step-280 :

- **`POSTGRES_MAX_CONNS` de `router-svc`.** Le défaut 10 vient d'une prémisse désormais fausse pour ce
  service (« le plan de contrôle n'est pas un chemin chaud ») : depuis step-250e, un possible-hit du
  Bloom que le cache n'a pas interroge Postgres sur le chemin MT. À arbitrer contre `max_connections`
  côté serveur, réplicas compris.
- **Mémoire du Redis partagé.** Le cache `exactroute:{msisdn}` y ajoute 1,3 à 10 Go de clés en vol
  selon le profil. La politique d'éviction compte : en `allkeys-*` Redis évincerait des clés de routage
  (dégradation silencieuse vers Postgres), en `volatile-*` la pression retomberait sur les clés à TTL,
  dont ce cache. À décider explicitement plutôt qu'à hériter du défaut.
- **`KAFKA_PRODUCE_TIMEOUT` (step-260e) ne gouverne que rest-api-svc et smpp-server-svc** (défaut 5 s,
  sous les 15 s du chemin SMPP). Les producteurs fail-closed (router, connector-pool, mo-dlr-router,
  mt-replay) sont construits avec `kafka.ForFailClosedConsumer()` : 30 s constants, ignorés de l'env —
  rien à poser dans leurs manifests, et rien à pouvoir y régler de travers.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts (code inchangé)
- [ ] manifests validés statiquement · probes/ports conformes §1.4/§1.5 · PDB cohérent avec le drain
- [ ] port ops non exposé publiquement

## Hors périmètre
Checklist de mise en production → step-410. Dashboards Grafana/règles Alertmanager (infra).
