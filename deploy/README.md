# `deploy/` — manifests Kubernetes

Un fichier par service sous `k8s/`, chacun portant son `Deployment`, son `Service` s'il en a un, son
HPA et son `PodDisruptionBudget`. Les Jobs (migrations, provisionnement Kafka) sont sous `k8s/jobs/`.
Ni kustomize ni Helm : ces fichiers se lisent et se valident tels quels.

Deux gardes les tiennent, et elles ne se recouvrent pas :

- `make manifests` — **kubeconform**, le schéma Kubernetes. Il attrape ce que la garde Go ne regarde
  pas : un `maxUnavailible: 1` dans un PDB laisse un PDB qui ne protège rien, et la garde Go — qui
  vérifie qu'un PDB *existe* et qu'il sélectionne bien le Deployment — passe au vert (vérifié).
- `go test ./internal/deploy/` — les invariants **de ce dépôt** : un Deployment par service `cmd/`,
  les probes sur le port ops, la période de grâce, le port ops absent des Services, un PDB par
  Deployment, les secrets par référence, les surcharges de port obligatoires, le plafond des HPA.

## Ce que ces manifests ne contiennent pas

- **Aucune image n'existe encore.** Elles sont nommées par convention
  (`ghcr.io/martialanouman/go-gateway/<svc>:<version>`) ; les Dockerfiles et la publication GHCR sont
  **step-270b**, qui bloque step-280 et step-410. Le tag `v0.0.0` est un gabarit : la chaîne de
  déploiement y substitue la version publiée.
- **Aucun `Secret`.** Les manifests ne font que référencer `gateway-secrets` ; step-300 (TLS, certs)
  et step-310 (auth opérateur) le provisionnent. Un secret dans ce dépôt serait un secret dans
  l'historique git.
- **Aucune règle Alertmanager, aucun collecteur OTel, aucun Ingress.** Ils vivent côté
  infrastructure (guide d'ingénierie §13) ; §15 vérifie au go-live qu'ils ont bien été posés.

## La période de grâce : 90 secondes, et pourquoi

L'arrêt d'un pod se déroule en trois temps, en séquence :

| Étape | Durée | Ce qui la borne |
|---|---|---|
| `/readyz` bascule à 503, on attend le load balancer | `DRAIN_DELAY` = **5 s** | Le hook de pré-drain (`ops.DrainHook`), attente non interruptible |
| Le superviseur arrête les composants | `SHUTDOWN_TIMEOUT` = **30 s** | Le budget de drain (step-270) : au-delà, les composants restants sont abandonnés et `ErrDrainBudgetExceeded` remonte |
| `DrainTracing` vide l'exporteur de spans | `SHUTDOWN_TIMEOUT` = **30 s** | Il court en `defer`, donc **après** le retour du superviseur |

Soit **65 s**, et `terminationGracePeriodSeconds: 90` laisse la marge. En dessous, le kubelet envoie
`SIGKILL` en plein drain — c'est-à-dire exactement ce que le drain existe pour éviter : des records
Kafka redélivrés (jusqu'à ~250 `submit_sm` dupliqués par partition, ADR-0012/0014) et un jeton de
session retenu pendant tout son TTL de 60 s, qui bloque le quota `max_sessions` du client.

Changer `DRAIN_DELAY` ou `SHUTDOWN_TIMEOUT` dans `configmap.yaml` **change ce calcul**, et la garde Go
le refait : elle lit les deux valeurs dans le manifeste, et retombe sur les défauts de `internal/config`
quand il ne les surcharge pas.

## Probes : `/healthz` n'est pas `/readyz`

Sur le port ops **9090**, jamais exposé publiquement — il n'apparaît dans aucun `Service`, et
Prometheus le scrape par les annotations du pod.

- `/healthz` = **liveness**. Il ne sonde rien et répond toujours 200. Il reste 200 pendant le drain :
  sans quoi le kubelet redémarrerait le pod qu'on est en train de retirer.
- `/readyz` = **readiness**. Il sonde les dépendances **vitales pour ce service-là**, décidées dans
  son `wiring.go` et prouvées par ses tests. Le manifeste n'a rien à y redéclarer, et tout à ne pas
  contredire — `TestClickHouseIsNotAReadinessDependency` (billing-svc) et
  `TestRouterReadinessFollowsTheFailurePolicy` (router-svc) épinglent deux de ces politiques.
- `timeoutSeconds: 4` partout, au-dessus des 3 s dont le serveur ops borne ses propres sondes : une
  probe qui abandonne avant lui rapporte un 503 que le serveur n'a jamais rendu.

`connector-pool-svc` est le seul à s'écarter (`initialDelaySeconds: 15`, `failureThreshold: 6`) : sa
readiness inclut `smsc-bind`, donc il reste *not ready* tant que le bind SMSC n'est pas établi — dial
5 s puis bind.

## Le plafond des HPA

`maxReplicas` doit rester **strictement sous** le nombre de partitions du topic consommé. Depuis
step-201d le routeur ouvre une goroutine par partition **assignée à son pod** : à un pod par
partition, chaque pod retombe à une seule lane, le fan-out disparaît à l'instant précis où la charge
le réclame, et l'HPA continue de croître sans rien acheter (ADR-0014).

Les valeurs posées ici préservent cette propriété ; **elles ne sont pas un dimensionnement**. Le
verdict chiffré appartient à **step-280**, sur environnement représentatif. Ne pas l'extrapoler des
mesures de step-201e : elles ont été prises sur un portable, en un seul processus, pipeline
partiellement bouché.

Restent trois grandeurs à trancher là-bas, et qui ne sont pas devinables ici :

- **`POSTGRES_MAX_CONNS` de `router-svc`.** Le défaut 10 vient d'une prémisse devenue fausse (« le
  plan de contrôle n'est pas un chemin chaud ») : depuis step-250e, un possible-hit du Bloom que le
  cache n'a pas interroge Postgres **sur le chemin MT**. À arbitrer contre le `max_connections` du
  serveur, réplicas compris.
- **Mémoire et politique d'éviction du Redis partagé.** Le cache `exactroute:{msisdn}` y ajoute 1,3 à
  10 Go de clés en vol selon le profil. En `allkeys-*`, Redis évincerait des clés de routage —
  dégradation silencieuse vers Postgres ; en `volatile-*`, la pression retombe sur les clés à TTL,
  dont ce cache. À décider, pas à hériter du défaut.
- **`maxReplicas` de chaque HPA**, avec le nombre de partitions correspondant.

Une métrique `External` suppose un `prometheus-adapter` ou KEDA dans le cluster : la jauge
`queue_depth_records{queue=…}` est publiée par les services (`router-svc` pour `mt.inbound` et
`mt.outcome`, `connector-pool-svc` pour `mt.routed`), l'adaptateur qui l'expose à l'API HPA est de
l'infrastructure.

## Les PDB, et pourquoi ils ne sont pas décoratifs

`maxUnavailable: 1` partout. Deux services en dépendent vraiment :

- **`smpp-server-svc`** : un pod tué sans drain laisse son jeton `pod_id:bind_id` dans le registre
  jusqu'à 60 s, et un pod redémarré porte un **nouveau** `pod_id` — la règle « un rebind ne
  double-compte pas » de `bind.lua` ne le couvre donc pas. Évincer tous les pods d'un coup laisse les
  ESME sans pod où se rebinder, refusés par leur propre quota.
- **`connector-pool-svc`** : un connecteur par pod. Les évincer ensemble, c'est zéro bind vers ce
  SMSC, `smsc-bind` not-ready partout, `mt.routed` qui s'accumule et le disjoncteur agrégé qui
  bascule.

## Deux singularités de topologie

- **`smpp-server-headless`** : le Service headless doit porter **ce nom** et exposer 7000. C'est le
  défaut de `SMPP_POD_ADDR_TEMPLATE` (`%s.smpp-server-headless:7000`), par lequel `mo-dlr-router-svc`
  remet un `deliver_sm` au pod qui détient le bind. Le renommer coupe la voie retour SMPP.
- **`connector-pool-svc` est un gabarit d'instance** : un connecteur par pod, un groupe de
  consommation `connector-pool-svc-<CONNECTOR_ID>` par connecteur. On copie le fichier par
  connecteur, avec `replicas` = `bind_pool_size` — `mt.routed` est shardé une partition par bind.
