# step-292b — Un topic par catégorie, un consommateur par topic, priorité bornée par bind (ADR-0021)

> **Jalon :** ADR-0021 §1-§2, §4-§6 · **Statut :** EN COURS
> **Dépend de :** step-292, step-287 · **Bloque :** step-409 ; paie
> `debts/pas-de-file-prioritaire-sur-un-connecteur-partage.md`
> Née du découpage de step-292 (06/10/2026, ADR-0021 action item 10) ; unité faute de multiple de dix libre.

## Pourquoi
step-292 porte la catégorie et la priorité effective sur `mt.routed`, mais une file unique laisse l'OTP
derrière le backlog marketing, au routeur comme au pool. ADR-0021 sépare les files par catégorie et borne la
priorité par bind.

## Découpage arrêté (step-292, design du 06/10/2026)
- [x] **PR3a** : les topics `mt.routed.<catégorie>` (routeur → pool). Un consommateur par topic et par connecteur
  au pool. Le drainer de parking et `mt-replay` republient selon la catégorie du record. Dans la même PR :
  les groupes du pool dans `deploy/k8s` et le lag par catégorie côté pool.
- **PR3b** : les topics `mt.inbound.<catégorie>`. L'ingestion (REST, SMPP) lit la catégorie dans un
  instantané de `sender_ids` (une lecture, pas une autorisation : ADR-0021 §2). Un consommateur par topic au
  routeur. `mt.inbound` et `mt.routed` sont retirés sans drain, avec les anciens groupes `connector-pool-svc-<CONNECTOR_ID>`
  (ADR-0021, *Conséquences*). Dans la même PR : HPA et groupes du routeur, alerte de lag OTP, guide
  d'ingénierie §5.1 (MT, le chemin critique ; la spec §5.1 est « Interfaces SMPP »), ligne « ordre du
  pipeline » de `CLAUDE.md`.
- **PR4** : l'ordonnanceur par bind (OTP → transactionnel → marketing) et `CONNECTOR_MARKETING_MIN_SHARE`
  (défaut 0,4). Dans la même PR : spec §6.4 et guide §4.1. Paie la dette.

`routed` passe avant `inbound`, parce que la dette est au pool : c'est là que l'OTP attend au débit plafonné
du SMSC.

## Arbitrages tranchés
- **Largeurs de départ : 4 / 4 / 12**, valeur d'ADR-0021 §1, appliquée telle quelle. Le banc de step-287
  n'injecte qu'une catégorie : il ne mesure rien de ces largeurs. step-409 les mesure (l'ADR le prévoit).
- **Dépendance à step-287** : levée pour PR3a sur consigne humaine du 11/10/2026 ; elle tenait à la mesure
  des largeurs (ci-dessus) et au gel de `main` pendant les runs, suspendus.
- **ADR-0021 §3** (débit par sender ID) n'est pas de cette step : livré par step-289 (#269).

## Design arrêté — PR3a (11/10/2026)
- **Un seul point de choix du topic.** Tout record `mt.routed` sort de `pipeline.EncodeRouted`
  (`internal/pipeline/wire.go`) : routeur, drainer de parking, reroute, `mt-replay`, auto-réponse STOP. Il
  choisit `mt.routed.<catégorie>` sur `env.TrafficCategory` ; toutes les republications suivent sans autre
  changement (ADR-0021 §1). Vide → `marketing` (la règle de `routedCategory`) ; inconnue → erreur d'encodage.
  `TopicMTRouted` laisse place à trois constantes et `TopicMTRoutedFor(cat)`.
- **Pool : trois consommateurs par connecteur.** `Deps.Consumer` devient un consommateur par topic, groupe
  `connector-pool-svc-<id>-<catégorie>`, `FromLatest` comme aujourd'hui. `runOnce` lance les trois
  `RunBatch` sur le même `batchHandler(binds)` ; la première erreur, ou la chute d'un bind, les arrête tous.
  Jusqu'à trois goroutines par bind au lieu d'une : `bind.Submit` est borné par sa fenêtre, le disjoncteur
  par son mutex, `retryFirstFail` est une `sync.Map`. L'ordre UDH (§7.3) tient : même catégorie, même topic,
  même clé, même shard. Aucune priorité encore : la fenêtre du bind est partagée à égalité jusqu'à PR4.
- **Largeurs** : `KAFKA_TOPIC_PARTITIONS_OVERRIDES` dans `deploy/k8s/configmap.yaml` (OTP et transactionnel
  à 4). `maxReplicas` du pool (4) reste ≤ 4 : chaque pod garde une voie par catégorie.
- **Lag et HPA.** `pollQueueDepth` lit les trois consommateurs ; le label `queue` est le nom du topic (trois
  valeurs bornées). L'HPA du pool sélectionne `queue In [les trois topics]` : sur une métrique External,
  l'HPA additionne les séries que le sélecteur retient (doc Kubernetes, *autoscaling on metrics not related
  to Kubernetes objects*). La garde `internal/deploy/manifests_test.go` suit. Une sonde de readiness par
  consommateur.
- **Provisionnement.** `kafkaprovision.Topics()` et `kafkatest` déclarent les trois topics, sans
  `mt.routed`. Le provisionneur ne supprime aucun topic : l'ancien `mt.routed` et ses groupes restent sur le
  broker de test jusqu'au déploiement de PR3b, qui retire `mt.inbound` et `mt.routed` ensemble, sans drain
  (ADR-0021, *Conséquences*) — une étape manuelle de déploiement, dite dans la PR.
- **Documents, même PR** : spec §434 (liste des topics), `deploy/README.md`, commentaires de
  `connector-pool-svc.yaml` et `jobs/kafka-provision.yaml`, cases d'ADR-0021.
- **Hors PR3a** : `mt.inbound.*` et le routeur multi-consommateurs (PR3b) ; l'ordonnanceur et
  `CONNECTOR_MARKETING_MIN_SHARE` (PR4).

## Design arrêté — PR3b (11/10/2026)
- **Choix du topic à l'ingestion, une lecture (ADR-0021 §2).** `EncodeInbound` reste le seul producteur : il
  reçoit la catégorie et choisit `mt.inbound.<catégorie>`, clé `message_id` inchangée. `rest-api-svc` et
  `smpp-server-svc` chargent `senderid.Holder` (celui du routeur), rechargé par leur watcher d'invalidation.
  Expéditeur inconnu ou instantané en retard → `marketing` ; l'autorisation reste au routeur, qui recalcule la
  catégorie sur son propre instantané.
- **Routeur : trois consommateurs**, groupes `router-svc-<catégorie>`. `FanIn` passe de `internal/connectorpool`
  à `internal/storage/kafka`, partagé par le routeur et le pool. Voies, commit par préfixe et halte de voie
  inchangés (voies indexées par `PartitionKey`). Une sonde de readiness et un lag publié par topic. Aucune clé
  `(partition, offset)` sans topic (piège de PR3a).
- **Projecteur du CDR accepted (arbitrage Fable du 11/10/2026)** : un seul consommateur, groupe
  `router-svc-accepted-cdr` inchangé, abonné aux trois topics — hors chemin de remise, l'ADR §4 vise le lot qui
  retient un OTP. Il passe de `FromLatest` à `AtStart` : topics neufs, rien à rejouer, et un record publié avant
  le join du nouveau routeur ne perd plus sa ligne accepted (raisonnement du projecteur outcome).
- **Retrait** : `TopicMTInbound` quitte le code, le provisionnement et `kafkatest`. Sur le broker de test, la
  suppression de `mt.inbound`, `mt.routed` et des groupes `router-svc` et `connector-pool-svc-<CONNECTOR_ID>`
  est manuelle et sans drain (ADR-0021, *Conséquences*) : commande dans `deploy/README.md` et la PR.
- **HPA du routeur** : `queue In [mt.inbound.*]`. `inspectHPA` lit `matchExpressions` : `maxReplicas` strictement
  sous la largeur du topic le plus large ; `pool-covers-lanes` compte la somme des voies par pod sur les trois
  topics (⌈4/4⌉ + ⌈4/4⌉ + ⌈12/4⌉ = 5 au plancher de 4). L'exemption nominative du pool reste. Largeurs :
  `mt.inbound.otp=4,mt.inbound.transactional=4` s'ajoutent à l'override.
- **Alerte de lag OTP (arbitrage Fable)**, guide §13, sans code :
  `max by (queue) (queue_depth_records{queue=~"mt\\.(inbound|routed)\\.otp"}) > 5000` (`for: 2m`), plus
  `absent()` sur `mt.inbound.otp` et sur `mt.routed.otp`. En records et non en quotient : aucun compteur de
  débit par topic, et un dénominateur toutes catégories rend l'alerte aveugle à un consommateur OTP mort.
  5 000 = seuil de l'HPA du pool ; step-409 recalibre. Limite dite : au pool, `absent()` ne part que si tous
  les pods disparaissent.
- **Documents, même PR** : ligne « ordre du pipeline » de `CLAUDE.md` (débit sender ID puis compte, choix du
  topic par lecture de catégorie, puis ACK) ; guide §5.1 ; commentaire de `ROUTER_LANE_WINDOW` (réserves en vol
  = somme des voies des trois topics, 20 au lieu de 12).
- **Conséquence mesurée par step-409** : 12 → 20 voies au routeur, autant de réserves de facturation en vol.

## Hors périmètre
Tout ce que step-292 livre (priorité effective, `priority_flag`, CDR, `priority_tier`).
