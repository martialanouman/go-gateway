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
- **PR3a** : les topics `mt.routed.<catégorie>` (routeur → pool). Un consommateur par topic et par connecteur
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

## Hors périmètre
Tout ce que step-292 livre (priorité effective, `priority_flag`, CDR, `priority_tier`).
