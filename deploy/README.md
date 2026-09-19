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

- **Les images existent depuis step-270b**, publiées sur GHCR par le workflow `Release` — qui se
  déclenche **à la main** (`workflow_dispatch`), pas au merge. Elles sont `linux/amd64` et
  `linux/arm64`, distroless, sans shell, en `USER 65532`.
  **Les paquets GHCR naissent privés.** Tant qu'ils ne sont pas basculés en public, ou qu'un
  `imagePullSecret` n'est pas posé dans le namespace, tous les pods restent en `ImagePullBackOff` :
  c'est le premier mur d'un déploiement neuf.
- **Le tag `v0.0.0` est un gabarit, et doit le rester.** La version publiée y est substituée au
  déploiement :

  ```
  make deploy-render VERSION=v1.4.2 | kubectl apply -f -
  ```

  Le script relit sa propre sortie et échoue si un `v0.0.0` a survécu. Ne figez pas une version à la
  main dans un fichier : elle ne serait plus substituée, et ce service resterait en arrière pendant
  que les onze autres avancent. Une garde de `internal/deploy` le refuse.
- **Le `nofile` du nœud est un prérequis de `smpp-server-svc`, que ces manifests ne peuvent pas
  poser.** `SMPP_MAX_CONNS` vaut 16384 et borne les descripteurs qu'un flood peut épingler. Le
  runtime Go relève seul le *soft* limit jusqu'au *hard* (`syscall/rlimit.go`), mais le hard vient du
  nœud — `LimitNOFILE` de l'unit systemd de containerd — et ni l'image, ni un `securityContext`, ni
  un initContainer ne le changent. Le défaut de containerd (1048576) est très au-dessus de 16384 ;
  **step-280 le vérifie sur environnement représentatif** plutôt que de le supposer.
- **Aucun `Secret`.** Un secret dans ce dépôt serait un secret dans l'historique git, donc **rien ici
  n'en provisionne aucun**. Il y en a deux, de natures différentes : `gateway-secrets`, référencé en
  variables d'environnement par les dix services (mots de passe de base, URL), que step-310 complétera
  pour l'auth opérateur ; et **un `Secret` TLS par service**, monté en volume, dont step-300 livre le
  *contrat* — ses clés, son montage, ses pièges — dans `k8s/tls/README.md`. L'exploitant remplit ce
  dernier avec cert-manager, une PKI interne, ou le générateur `test/tlsgen`. **Les huit `Secret` TLS
  doivent exister avant l'`apply`** : huit des dix pods montent le leur en volume, et un volume qui ne se
  monte pas laisse le pod en `ContainerCreating` sans limite de temps — sans une ligne dans les journaux
  du service, et sans jamais devenir un `CrashLoopBackOff` qu'on remarquerait.
- **Aucune règle Alertmanager, aucun collecteur OTel, aucun Ingress.** Ils vivent côté
  infrastructure (guide d'ingénierie §13) ; §15 vérifie au go-live qu'ils ont bien été posés.

## La période de grâce : 90 secondes, et pourquoi

L'arrêt d'un pod se déroule en trois temps, en séquence :

| Étape | Durée | Ce qui la borne |
|---|---|---|
| `/readyz` bascule à 503, on attend le load balancer | `DRAIN_DELAY` = **5 s** | Le hook de pré-drain (`ops.DrainHook`), attente non interruptible |
| Le superviseur arrête les composants | `DRAIN_BUDGET` = **90 s** | Au-delà, les composants restants sont abandonnés et `ErrDrainBudgetExceeded` remonte |
| `DrainTracing` vide l'exporteur de spans | `SHUTDOWN_TIMEOUT` = **30 s** | Il court en `defer`, donc **après** le retour du superviseur |

Soit **125 s**, et `terminationGracePeriodSeconds: 150` laisse la marge. En dessous, le kubelet envoie
`SIGKILL` en plein drain — c'est-à-dire exactement ce que le drain existe pour éviter : des records
Kafka redélivrés (jusqu'à ~250 `submit_sm` dupliqués par partition, ADR-0012/0014) et un jeton de
session retenu pendant tout son TTL de 60 s, qui bloque le quota `max_sessions` du client.

**`DRAIN_BUDGET` et `SHUTDOWN_TIMEOUT` sont deux variables, et ce n'est pas un doublon.**
`SHUTDOWN_TIMEOUT` borne **un** composant — l'arrêt gracieux d'un serveur HTTP ou gRPC, celui du
serveur ops, le flush de l'exporteur. `DRAIN_BUDGET` borne **leur ensemble**, et sur les superviseurs
ordonnés cet ensemble est une **séquence**. Les confondre coupe un drain parfaitement légitime : un
composant qui dépense la fenêtre qu'on lui a accordée épuiserait à lui seul le budget global, et tout
ce qui est enregistré derrière lui serait abandonné au lieu d'être drainé. `smpp-server-svc` peut
légitimement demander 80 s — deliver gRPC, puis les `submit_sm` en vol et les unbind du listener, puis
le serveur ops. `internal/config` refuse d'ailleurs un budget inférieur ou égal au timeout par
composant.

Changer l'une des trois valeurs dans `configmap.yaml` **change ce calcul**, et la garde Go le refait :
elle les lit dans le manifeste, et retombe sur les défauts de `internal/config` quand il ne les
surcharge pas.

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
  Le TTL, lui, **est** réglable depuis step-270d : `EXACT_CACHE_TTL` (6h, le défaut inchangé) est dans
  le ConfigMap, et les clés en vol valent le taux de peuplement × ce TTL. C'est la valeur qui reste à
  step-280, plus le levier ; jusque-là le seul recours sur un Redis qui se remplissait était un
  redéploiement.
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

- **La voie retour SMPP ne passe par aucun `Service`** : `mo-dlr-router-svc` remet un `deliver_sm` au
  pod qui détient le bind en dialant l'adresse que ce pod a lui-même publiée dans le registre de
  sessions (`SMPP_POD_ADDR` ← `status.podIP`, port `GRPC_PORT`). Il n'y a rien à nommer, donc rien à
  renommer. Un `Service` headless a occupé cette place jusqu'à step-302 et ne pouvait pas marcher :
  un `Deployment` ne donne pas d'enregistrement DNS par pod, `spec.hostname` étant une chaîne unique
  pour toutes ses répliques. La garde `downward-api-fields` (`internal/deploy`) tient le lien.
- **`connector-pool-svc` est un gabarit d'instance** : un connecteur par pod, un groupe de
  consommation `connector-pool-svc-<CONNECTOR_ID>` par connecteur. On copie le fichier par
  connecteur, avec `replicas` = `bind_pool_size` — `mt.routed` est shardé une partition par bind.
